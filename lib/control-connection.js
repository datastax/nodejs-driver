"use strict";
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

var selectPeers = "SELECT peer,data_center,rack,tokens,rpc_address,release_version FROM system.peers";
var selectLocal = "SELECT * FROM system.local WHERE key='local'";
var selectAllKeyspaces = "SELECT * FROM system.schema_keyspaces";
var selectSingleKeyspace = "SELECT * FROM system.schema_keyspaces where keyspace_name = '%s'";
var newNodeDelay = 1000;
var retryNewConnectionDelay = 5000;
var schemaChangeTypes = {
  created: 'CREATED',
  updated: 'UPDATED',
  dropped: 'DROPPED'
};
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
  this.metadata = new Metadata(this.options, this);
  this.addressTranslator = this.options.policies.addressResolution;
  this.initialized = false;
}

util.inherits(ControlConnection, events.EventEmitter);

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.init = function (callback) {
  if (this.initialized) {
    //prevent multiple serial initializations
    return callback();
  }
  var self = this;
  function addHost(address, port, cb) {
    var endPoint = address + ':' + (port || self.options.protocolOptions.port);
    var h = new Host(endPoint, self.protocolVersion, self.options);
    self.hosts.set(endPoint, h);
    self.log('info', 'Adding host ' + endPoint);
    cb();
  }
  async.series([
    function resolveNames(next) {
      async.each(self.options.contactPoints, function (name, eachNext) {
        if (name.indexOf(':') > 0) {
          var parts = name.split(':');
          return addHost(parts[0], parts[1], eachNext);
        }
        if (net.isIP(name)) {
          return addHost(name, null, eachNext);
        }
        dns.lookup(name, function (err, address) {
          if (err || !address) {
            self.log('error', 'Host with name ' + name + ' could not be resolved');
            return eachNext();
          }
          addHost(address, null, eachNext);
        });
      }, function (err) {
        if (!err && self.hosts.length === 0) {
          err = new errors.NoHostAvailableError(null, 'No host could be resolved');
        }
        next(err);
      });
    },
    this.getConnection.bind(this),
    function tryInitOnConnection(next) {
      self.initOnConnection(true, next);
    }
  ], function (err) {
    self.initialized = !err;
    callback(err);
  });
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
        }, retryNewConnectionDelay);
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
    function subscribeHostEvents(next) {
      self.host.once('down', self.hostDownHandler.bind(self));
      next();
    },
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
        self.setLocalInfo(c.endPoint, result);
        next(err);
      });
    },
    function getPeersInfo(next) {
      var request = new requests.QueryRequest(selectPeers, null, null);
      c.sendStream(request, {}, function (err, result) {
        self.setPeersInfo(newNodesUp, result, next);
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
  this.query(selectAllKeyspaces, function (err, result) {
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
  var self = this;
  setTimeout(function () {
    self.refreshHosts(false);
  }, newNodeDelay);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  var self = this;
  this.addressTranslator.translate(event.inet.address.toString(), this.options.protocolOptions.port, function (endPoint) {
    var host = self.hosts.get(endPoint);
    if (!host) {
      self.log('warning', 'Received status change event but host was not found: ' + event.inet.address);
      return;
    }
    if (event.up) {
      //wait a couple of seconds before marking it as UP
      setTimeout(function () {
        host.setUp();
      }, newNodeDelay);
    }
    else {
      host.setDown();
    }
  });
};

/**
 * Handles a SCHEMA_CHANGE event
 */
ControlConnection.prototype.nodeSchemaChangeHandler = function (event) {
  this.log('info', 'Schema change', event);
  if (event.schemaChangeType !== schemaChangeTypes.created) {
    var ksInfo = this.metadata.keyspaces[event.keyspace];
    if (!ksInfo) {
      //It hasn't been loaded and it is not part of the metadata, don't mind
      return;
    }
    if (event.table) {
      //Is a table change, clean the internal cache
      delete ksInfo.tables[event.table];
      return;
    }
    if (event.udt) {
      //Is a user defined type change, clean the internal cache
      delete ksInfo.udts[event.udt];
      return;
    }
    if (event.functionName) {
      //Is a function change, clean the internal cache
      delete ksInfo.functions[event.functionName];
      return;
    }
    if (event.aggregate) {
      //Is a aggregate change, clean the internal cache
      delete ksInfo.aggregates[event.aggregate];
      return;
    }
    if (event.schemaChangeType === schemaChangeTypes.dropped) {
      delete this.metadata.keyspaces[event.keyspace];
      return;
    }
  }
  //The keyspace was either created or altered
  var query = util.format(selectSingleKeyspace, event.keyspace);
  var self = this;
  this.query(query, function (err, result) {
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

ControlConnection.prototype.setLocalInfo = function (endPoint, result) {
  if (!result || !result.rows || !result.rows.length) {
    this.log('warning', 'No local info provided');
    return;
  }
  var row = result.rows[0];
  var localHost = this.hosts.get(endPoint);
  if (!localHost) {
    this.log('error', 'Localhost could not be found');
    return;
  }
  localHost.datacenter = row['data_center'];
  localHost.rack = row['rack'];
  localHost.tokens = row['tokens'];
  localHost.cassandraVersion = row['release_version'];
  this.metadata.setPartitioner(row['partitioner']);
  this.log('info', 'Local info retrieved');
};

/**
 * @param {Boolean} newNodesUp
 * @param {ResultSet} result
 * @param {Function} callback
 */
ControlConnection.prototype.setPeersInfo = function (newNodesUp, result, callback) {
  if (!result || !result.rows) {
    return callback();
  }
  var self = this;
  //A map of peers, could useful for in case there are discrepancies
  var peers = {};
  var port = this.options.protocolOptions.port;
  async.eachSeries(result.rows, function (row, next) {
    self.getAddressForPeerHost(row, port, function (endPoint) {
      if (!endPoint) {
        return next();
      }
      peers[endPoint] = true;
      var host = self.hosts.get(endPoint);
      if (!host) {
        host = new Host(endPoint, self.protocolVersion, self.options);
        if (!newNodesUp) {
          host.setDown();
        }
        self.log('info', 'Adding host ' + endPoint);
        self.hosts.set(endPoint, host);
      }
      host.datacenter = row['data_center'];
      host.rack = row['rack'];
      host.tokens = row['tokens'];
      host.cassandraVersion = row['release_version'];
      next();
    });
  }, function (err) {
    if (err) {
      return callback(err);
    }
    //Is there a difference in number between peers + local != hosts
    if (self.hosts.length > result.rows.length + 1) {
      //There are hosts in the current state that don't belong (nodes removed or wrong contactPoints)
      self.log('info', 'Removing nodes from the pool');
      var toRemove = [];
      self.hosts.forEach(function (h) {
        //It is not a peer and it is not local host
        if (!peers[h.address] && h !== self.host) {
          self.log('info', 'Removing host ' + h.address);
          toRemove.push(h.address);
          h.setDown();
        }
      });
      self.hosts.removeMultiple(toRemove);
    }
    self.log('info', 'Peers info retrieved');
    callback();
  });
};

/**
 * @param {Object|Row} row
 * @param {Number} defaultPort
 * @param {Function} callback The callback to invoke with the string representation of the host endpoint,
 *  containing the ip address and port.
 */
ControlConnection.prototype.getAddressForPeerHost = function (row, defaultPort, callback) {
  var address = row['rpc_address'];
  var peer = row['peer'];
  var bindAllAddress = '0.0.0.0';
  if (!address) {
    this.log('error', util.format('No rpc_address found for host %s in %s\'s peers system table. %s will be ignored.', peer, this.host.address, peer));
    return callback(null);
  }
  if (address.toString() === bindAllAddress) {
    this.log('warning', util.format('Found host with 0.0.0.0 as rpc_address, using listen_address (%s) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.', peer));
    address = peer;
  }
  this.addressTranslator.translate(address.toString(), defaultPort, callback);
};

/**
 * Executes a query using the active connection
 * @param {string} cqlQuery
 * @param {function} callback
 */
ControlConnection.prototype.query = function (cqlQuery, callback) {
  var request = new requests.QueryRequest(cqlQuery, null, null);
  this.connection.sendStream(request, utils.emptyObject, callback);
};

/** @returns {Encoder} The encoder used by the current connection */
ControlConnection.prototype.getEncoder = function () {
  return this.connection.encoder;
};

module.exports = ControlConnection;