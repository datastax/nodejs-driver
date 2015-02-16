var async = require('async');
var events = require('events');
var util = require('util');
var dns = require('dns');
var net = require('net');

var errors = require('./errors.js');
var RequestHandler = require('./request-handler.js');
var Host = require('./host.js').Host;
var HostMap = require('./host.js').HostMap;
var Metadata = require('./metadata.js');
var requests = require('./requests.js');
var utils = require('./utils.js');

var selectPeers = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers";
var selectLocal = "SELECT * FROM system.local WHERE key='local'";
var selectSchemaVersionPeers = "SELECT schema_version FROM system.peers";
var selectSchemaVersionLocal = "SELECT schema_version FROM system.local";
var selectAllKeyspaces = "SELECT * FROM system.schema_keyspaces";
var selectSingleKeyspace = "SELECT * FROM system.schema_keyspaces where keyspace_name = '%s'";
/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster
 * @param {Object} options
 * @constructor
 */
function ControlConnection(options) {
  this.protocolVersion = null;
  this.hosts = new HostMap();
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  /**
   * Cluster metadata that is going to be shared between the Client and ControlConnection
   */
  this.metadata = new Metadata(this.options);
}

util.inherits(ControlConnection, events.EventEmitter);

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.init = function (callback) {
  var self = this;
  function addHost(address, cb) {
    var h = new Host(address, self.protocolVersion, self.options);
    self.hosts.push(address, h);
    self.log('info', 'Adding host ' + address);
    cb();
  }
  async.series([
    function resolveNames(next) {
      async.each(self.options.contactPoints, function (name, eachNext) {
        if (net.isIP(name)) {
          return addHost(name, eachNext);
        }
        dns.lookup(name, function (err, address) {
          if (err) {
            self.log('error', 'Host with name ' + name + ' could not be resolved');
            return eachNext();
          }
          if (!address) {
            //Resolving a name can return undefined
            address = name;
          }
          addHost(address, eachNext);
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
    this.log('info', 'Getting first connection');
    handler.getFirstConnection(this.hosts, function (err, c, host) {
      if (!err && c) {
        self.protocolVersion = c.protocolVersion;
        self.log('info', 'Control connection using protocol version ' + self.protocolVersion);
      }
      next(err, c, host);
    });
  }
  else {
    this.log('info', 'Getting a connection');
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
    if (!err) {
      self.connection = c;
      self.host = host;
    }
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
      if (self.metadata.tokenizer) {
        self.metadata.buildTokens(self.hosts);
      }
      else {
        self.log('warning', 'Tokenizer could not be determined');
      }
      next();
    },
    function subscribeHostEvents(next) {
      self.host.once('down', self.hostDownHandler.bind(self));
      next();
    },
    function subscribeConnectionEvents(next) {
      c.on('nodeTopologyChange', self.nodeTopologyChangeHandler.bind(self));
      c.on('nodeStatusChange', self.nodeStatusChangeHandler.bind(self));
      c.on('nodeSchemaChange', self.nodeSchemaChangeHandler.bind(self));
      var request = new requests.RegisterRequest(['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE']);
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
      var request = new requests.QueryRequest(selectLocal, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setLocalInfo(c.address, result);
        next(err);
      });
    },
    function getPeersInfo(next) {
      var request = new requests.QueryRequest(selectPeers, null, null);
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
  var request = new requests.QueryRequest(selectAllKeyspaces, null, null);
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
  //All hosts information needs to be refreshed as tokens might have changed
  this.refreshHosts(false);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  var hostIp = event.inet.address;
  var host = this.hosts.get(hostIp.toString());
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

/**
 * Handles a SCHEMA_CHANGE event
 */
ControlConnection.prototype.nodeSchemaChangeHandler = function (event) {
  this.log('info', 'Schema change', event);
  if (event.table) {
    //Is a table change, dismiss
    return;
  }
  if (event.schemaChangeType === 'DROPPED') {
    delete this.metadata.keyspaces[event.keyspace];
    return;
  }
  //The keyspace was either created or altered
  var query = util.format(selectSingleKeyspace, event.keyspace);
  var request = new requests.QueryRequest(query, null, null);
  var self = this;
  this.connection.sendStream(request, {}, function (err, result) {
    if (err) {
      self.log('error', 'There was an error while trying to retrieve keyspace information', err);
      return;
    }
    if (result.rows.length === 0) {
      self.log('warning', 'There was not possible to retrieve keyspace info', event.keyspace);
      return;
    }
    self.metadata.setKeyspaceInfo(result.rows[0]);
  });
};

ControlConnection.prototype.setLocalInfo = function (address, result) {
  if (!result || !result.rows || !result.rows.length) {
    this.log('warning', 'No local info provided');
    return;
  }
  var row = result.rows[0];
  var localHost = this.hosts.get(address);
  if (!localHost) {
    this.log('error', 'Localhost could not be found');
    return;
  }
  localHost.datacenter = row['data_center'];
  localHost.rack = row.rack;
  localHost.tokens = row.tokens;
  this.metadata.setPartitioner(row['partitioner']);
  this.log('info', 'Local info retrieved');
};

/**
 * @param {Boolean} newNodesUp
 * @param {Object} result
 */
ControlConnection.prototype.setPeersInfo = function (newNodesUp, result) {
  if (!result || !result.rows) {
    return;
  }
  var self = this;
  //A map of peers, could useful for in case there are discrepancies
  var peers = {};
  result.rows.forEach(function (row) {
    var address = self.getAddressForPeerHost(row);
    if (!address) {
      return;
    }
    peers[address] = true;
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
  //Is there a difference in number between peers + local != hosts
  if (this.hosts.length > result.rows.length + 1) {
    //There are hosts in the current state that don't belong (nodes removed or wrong contactPoints)
    this.log('info', 'Removing nodes from the pool');
    var toRemove = [];
    this.hosts.forEach(function (h) {
      //It is not a peer and it is not local host
      if (!peers[h.address] && h !== self.host) {
        toRemove.push(h.address);
      }
    });
    toRemove.forEach(function (key) {
      self.hosts.get(key).setDown();
      self.hosts.remove(key);
      self.log('info', 'Host ' + key + ' removed');
    });
  }
  this.log('info', 'Peers info retrieved');
};

/**
 * @param {Object|Row} row
 * @returns {String} The string representation of the host address
 */
ControlConnection.prototype.getAddressForPeerHost = function (row) {
  var address = row['rpc_address'];
  var peer = row['peer'];
  var bindAllAddress = '0.0.0.0';
  if (!address) {
    this.log('error', util.format('No rpc_address found for host %s in %s\'s peers system table. %s will be ignored.', peer, this.host.address, peer));
    return null;
  }
  if (address.toString() === bindAllAddress) {
    this.log('warning', util.format('Found host with 0.0.0.0 as rpc_address, using listen_address (%s) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.', peer));
    address = peer;
  }
  return address.toString();
};

ControlConnection.prototype.getLocalSchemaVersion = function (callback) {
  var request = new requests.QueryRequest(selectSchemaVersionLocal, null, null);
  this.connection.sendStream(request, {}, function (err, result) {
    var version;
    if (!err && result && result.rows && result.rows.length === 1) {
      version = result.rows[0]['schema_version'];
    }
    callback(err, version);
  });
};

ControlConnection.prototype.getPeersSchemaVersions = function (callback) {
  var request = new requests.QueryRequest(selectSchemaVersionLocal, null, null);
  this.connection.sendStream(request, {}, function (err, result) {
    var versions = [];
    if (!err && result && result.rows) {
      for (var i = 0; i < result.rows.length; i++) {
        versions.push(result.rows[i]['schema_version']);
      }
    }
    callback(err, versions);
  });
};

/** @returns {Encoder} The encoder used by the current connection */
ControlConnection.prototype.getEncoder = function () {
  return this.connection.encoder;
};

module.exports = ControlConnection;