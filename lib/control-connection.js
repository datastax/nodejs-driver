var async = require('async');
var events = require('events');
var util = require('util');
var dns = require('dns');

var errors = require('./errors.js');
var RequestHandler = require('./request-handler.js');
var Host = require('./host.js').Host;
var HostMap = require('./host.js').HostMap;
var Metadata = require('./metadata.js');
var requests = require('./requests.js');
var utils = require('./utils.js');

var selectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
var selectLocal = "SELECT * FROM system.local WHERE key='local'";
var selectKeyspaces = "SELECT * FROM system.schema_keyspaces";
/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster
 * @param {Object} options
 * @constructor
 */
function ControlConnection(options) {
  this.protocolVersion = null;
  this.hosts = new HostMap();
  this.options = options;
  /**
   * Cluster metadata that is going to be shared between the Client and ControlConnection
   */
  this.metadata = new Metadata();
}

util.inherits(ControlConnection, events.EventEmitter);

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.init = function (callback) {
  var self = this;
  async.series([
    function resolveNames(next) {
      async.each(self.options.contactPoints, function (name, eachNext) {
        dns.lookup(name, function (err, address) {
          if (err) {
            self.log('error', 'Host with name ' + name + ' could not be resolved');
            return eachNext();
          }
          var h = new Host(address, self.protocolVersion, self.options);
          self.hosts.push(address, h);
          self.log('info', 'Adding host ' + address);
          eachNext();
        });
      }, function (err) {
        if (!err && self.hosts.length === 0) {
          err = new errors.NoHostAvailableError(null, 'No host could be resolved')  ;
        }
        next(err);
      });
    },
    this.getConnection.bind(this),
    function tryInitOnConnection(next) {
      self.initOnConnection(true, next);
    }
  ], callback);
};

/**
 * Gets a connection to any Host in the pool.
 * If its the first time, it will try to create a connection to a host present in the contactPoints in order.
 * @param {Function} callback
 */
ControlConnection.prototype.getConnection = function (callback) {
  var self = this;
  var handler = new RequestHandler(null, this.options);
  if (!this.host) {
    //it is the first time
    handler.getFirstConnection(this.hosts, function (err, c, host) {
      if (!err && c) {
        self.protocolVersion = c.protocolVersion;
        self.log('info', 'Control connection using protocol version ' + self.protocolVersion);
      }
      next(err, c, host);
    });
  }
  else {
    handler.getNextConnection(null, function (err, c, host) {
      if (err) {
        //it had an active connection, but now it failed to acquire a new one.
        //lets retry in a couple seconds
        setTimeout(function () {
          self.getConnection.call(self, callback);
        }, 5000);
        return;
      }
      next(err, c, host);
    });
  }
  function next(err, c, host) {
    self.connection = c;
    self.host = host;
    callback(err);
  }
};

/**
 * Gets info and subscribe to events on an specific connection
 * @param {Boolean} firstTime Determines if the cc is being initialized and
 * it's the first time that trying to retrieve host information
 * @param {Function} callback
 */
ControlConnection.prototype.initOnConnection = function (firstTime, callback) {
  var c = this.connection;
  var self = this;
  self.log('info', 'Connection acquired, refreshing nodes list');
  async.series([
    function getLocalAndPeersInfo(next) {
      self.refreshHosts(firstTime, next);
    },
    function getKeyspaces(next) {
      self.getKeyspaces(next);
    },
    function buildTokenPerReplica(next) {
      self.metadata.buildTokens(self.hosts);
      next();
    },
    function subscribeHostEvents(next) {
      self.host.once('down', self.hostDownHandler.bind(self));
      next();
    },
    function subscribeConnectionEvents(next) {
      c.on('nodeTopologyChange', self.nodeTopologyChangeHandler.bind(self));
      c.on('nodeStatusChange', self.nodeStatusChangeHandler.bind(self));
      var request = new requests.RegisterWriter(['TOPOLOGY_CHANGE', 'STATUS_CHANGE']);
      c.sendStream(request, {}, next);
    }],
    function initDone(err) {
      if (err) {
        self.log('error', 'ControlConnection could not be initialized', err);
      }
      else {
        self.log('info', 'ControlConnection connected and up to date');
      }
      callback(err);
    });
};

/**
 * Gets the info from local and peer metadata column families
 * @param {Boolean} newNodesUp
 * @param {Function} [callback]
 */
ControlConnection.prototype.refreshHosts = function (newNodesUp, callback) {
  if (!callback) {
    callback = function () {};
  }
  var self = this;
  if (!this.host.protocolVersion) {
    this.hosts.forEach(function (h) {
      h.setProtocolVersion(self.protocolVersion);
    });
  }
  this.log('info', 'Refreshing local and peers info');
  var c = this.connection;
  async.series([
    function getLocalInfo(next) {
      var request = new requests.QueryWriter(selectLocal, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setLocalInfo(c.address, result);
        next(err);
      });
    },
    function getPeersInfo(next) {
      var request = new requests.QueryWriter(selectPeers, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setPeersInfo(newNodesUp, result);
        next(err);
      });
    }], callback);
};

ControlConnection.prototype.hostDownHandler = function () {
  this.log('warning', 'Host ' + this.host.address + ' used by the ControlConnection DOWN');
  var self = this;
  async.series([
      this.getConnection.bind(this),
      function (next) {
        self.initOnConnection(false, next);
      }
    ],
    function (err) {
      if (err) {
        self.log('error', 'Could not reconnect');
      }
    }
  );
};

ControlConnection.prototype.getKeyspaces = function (callback) {
  this.log('info', 'Retrieving keyspaces metadata');
  var self = this;
  var request = new requests.QueryWriter(selectKeyspaces, null, null);
  this.connection.sendStream(request, {}, function (err, result) {
    if (err) return callback(err);
    self.metadata.setKeyspaces(result);
    callback();
  });
};

/**
 * @param {String} type
 * @param {String} info
 * @param [furtherInfo]
 */
ControlConnection.prototype.log = utils.log;

/**
 * Handles a TOPOLOGY_CHANGE event
 */
ControlConnection.prototype.nodeTopologyChangeHandler = function (event) {
  this.log('info', 'Received topology change', event);
  //TODO: Handle removed node
  this.refreshHosts(false);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  var hostIp = utils.toIpString(event.inet.address);
  var host = this.hosts.get(hostIp);
  if (!host) return;
  if (event.up) {
    //wait a couple of seconds before marking it as UP
    setTimeout(function () {
      host.setUp();
    }, 10000);
  }
  else {
    host.setDown();
  }
};

ControlConnection.prototype.setLocalInfo = function (address, result) {
  if (!result || !result.rows || !result.rows.length) {
    return;
  }
  var row = result.rows[0];
  var localHost = this.hosts.get(address);
  localHost.datacenter = row['data_center'];
  localHost.rack = row.rack;
  localHost.tokens = row.tokens;
  this.metadata.setPartitioner(row['partitioner']);
};

/**
 * @param {Boolean} newNodesUp
 * @param {Object} result
 */
ControlConnection.prototype.setPeersInfo = function (newNodesUp, result) {
  if (!result || !result.rows || !result.rows.length) {
    return;
  }
  var self = this;
  result.rows.forEach(function (row) {
    var address = row['rpc_address'] || row['peer'];
    address = utils.toIpString(address);
    var host = self.hosts.get(address);
    if (!host) {
      host = new Host(address, self.protocolVersion, self.options);
      if (!newNodesUp) {
        host.setDown();
      }
      self.log('info', 'Adding host ' + address);
      self.hosts.push(address, host);
    }
    host.datacenter = row['data_center'];
    host.rack = row.rack;
    host.tokens = row.tokens;
  });
};

module.exports = ControlConnection;