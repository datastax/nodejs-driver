"use strict";
var async = require('async');
var events = require('events');
var util = require('util');
var dns = require('dns');
var net = require('net');

var errors = require('./errors');
var RequestHandler = require('./request-handler');
var Host = require('./host').Host;
var HostMap = require('./host').HostMap;
var Metadata = require('./metadata');
var requests = require('./requests');
var utils = require('./utils');
var types = require('./types');

var selectPeers = "SELECT peer,data_center,rack,tokens,rpc_address,release_version FROM system.peers";
var selectLocal = "SELECT * FROM system.local WHERE key='local'";
var newNodeDelay = 1000;
var metadataQueryAbortTimeout = 2000;
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
  //noinspection JSUnresolvedFunction
  this.setMaxListeners(0);
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  /**
   * Cluster metadata that is going to be shared between the Client and ControlConnection
   */
  this.metadata = new Metadata(this.options, this);
  this.addressTranslator = this.options.policies.addressResolution;
  this.initialized = false;
  /**
   * Host used by the control connection
   * @type {Host|null}
   */
  this.host = null;
  /**
   * Connection used to retrieve metadata and subscribed to events
   * @type {Connection|null}
   */
  this.connection = null;
  /**
   * Reference to the encoder of the last valid connection
   * @type {Encoder|null}
   */
  this.encoder = null;
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
      self.refreshOnConnection(true, next);
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
 * @emits ControlConnection#newConnection When a new connection is acquired
 */
ControlConnection.prototype.getConnection = function (callback) {
  var self = this;
  function done(err, c, host) {
    if (c) {
      self.connection = c;
      self.encoder = c.encoder;
      self.host = host;
    }
    callback(err);
    self.emit('newConnection', err, c, host);
  }
  if (!this.initialized) {
    //it is the first time
    this.log('info', 'Getting first connection');
    return this.getFirstConnection(function (err, c, host) {
      if (!err && c) {
        self.protocolVersion = c.protocolVersion;
        self.log('info', 'Control connection using protocol version ' + self.protocolVersion);
      }
      done(err, c, host);
    });
  }
  this.log('info', 'Trying to acquire a connection to a new host');
  this.getConnectionToNewHost(done);
};

/**
 * Gets an open connection to using the provided hosts Array, without using the load balancing policy.
 * Invoked before the Client can access topology of the cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.getFirstConnection = function (callback) {
  var connection = null;
  var index = 0;
  var openingErrors = {};
  var hosts = this.hosts.values();
  var host = null;
  async.doWhilst(function iterator(next) {
    host = hosts[index];
    host.borrowConnection(function (err, c) {
      if (err) {
        openingErrors[host.address] = err;
      }
      else {
        connection = c;
      }
      next();
    });
  }, function condition () {
    return !connection && (++index < hosts.length);
  }, function done(err) {
    if (!connection) {
      err = new errors.NoHostAvailableError(openingErrors);
    }
    callback(err, connection, host);
  });
};

/**
 * Acquires a connection to a host according to the load balancing policy.
 * If its not possible to connect, it subscribes to the hosts UP event.
 * @param {Function} callback
 */
ControlConnection.prototype.getConnectionToNewHost = function (callback) {
  var self = this;
  var host;
  var connection = null;
  var loadBalancingPolicy = this.options.policies.loadBalancing;
  loadBalancingPolicy.newQueryPlan(null, null, function (err, iterator) {
    if (err) {
      var message = 'Control connection could not retrieve a query plan to determine which hosts to use, ' +
        'using current hosts map';
      self.log('error', message, err);
      iterator = utils.arrayIterator(self.hosts.values());
    }
    //use iterator
    async.whilst(
      function condition() {
        //while there isn't a valid connection
        if (connection) {
          return false;
        }
        var item = iterator.next();
        host = item.value;
        return (!item.done);
      },
      function whileIterator(next) {
        if (!host.isUp()) {
          return next();
        }
        var distance = loadBalancingPolicy.getDistance(host);
        host.setDistance(distance);
        if (distance === types.distance.ignored) {
          return next();
        }
        host.borrowConnection(function (err, c) {
          //move next if there was an error
          connection = c;
          next();
        });
      },
      function whilstEnded() {
        if (!connection) {
          self.listenHostsForUp();
          return callback();
        }
        callback(null, connection, host);
      });
  });
};

/**
 * Subscribe to the UP event of all current hosts to reconnect when one
 * of them are back up.
 */
ControlConnection.prototype.listenHostsForUp = function () {
  var self = this;
  var hostArray = this.hosts.values();
  function onUp() {
    //unsubscribe from all host
    hostArray.forEach(function (host) {
      host.removeListener('up', onUp);
    });
    self.refresh();
  }
  //All hosts are DOWN, we should subscribe to the UP event
  //of each host as the HostConnectionPool is attempting to reconnect
  hostArray.forEach(function (host) {
    host.on('up', onUp);
  });
};

/**
 * Gets info and subscribe to events on an specific connection
 * @param {Boolean} firstTime Determines if the cc is being initialized and
 * it's the first time that trying to retrieve host information
 * @param {Function} callback
 */
ControlConnection.prototype.refreshOnConnection = function (firstTime, callback) {
  var c = this.connection;
  var self = this;
  self.log('info', 'Connection acquired to ' + self.host.address + ', refreshing nodes list');
  async.series([
    function subscribeHostEvents(next) {
      self.host.once('down', self.hostDownHandler.bind(self));
      next();
    },
    function getLocalAndPeersInfo(next) {
      self.refreshHosts(firstTime, next);
    },
    function getKeyspaces(next) {
      //to acquire metadata we need to specify the cassandra version
      self.metadata.setCassandraVersion(self.host.getCassandraVersion());
      self.metadata.refreshKeyspaces(next);
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
        self.log('info', 'ControlConnection connected to ' + self.host.address + ' and is up to date');
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

  // it's possible that this was called as a result of a topology change, but the connection was lost
  // between now and newNodeDelay.  In this case, simply return as this will be called again when there
  // is a new connection.
  if (!this.connection) {
    callback();
    return;
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
        self.setLocalInfo(c.endpoint, result);
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
  this.host = null;
  this.connection = null;
  this.refresh();
};

/**
 * Acquires a connection and refreshes topology metadata.
 * @param {Function} [callback]
 */
ControlConnection.prototype.refresh = function (callback) {
  var self = this;
  async.series([
      this.getConnection.bind(this),
      function (next) {
        if (!self.connection) {
          return next();
        }
        self.refreshOnConnection(false, next);
      }
    ],
    function doneRefreshing(err) {
      if (err || !self.connection) {
        self.log('error', 'ControlConnection was not able to reconnect');
      }
      if (callback) {
        callback(err);
      }
    }
  );
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
      //Is a table or view change, clean the internal cache
      delete ksInfo.tables[event.table];
      delete ksInfo.views[event.table];
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
  this.metadata.refreshKeyspace(event.keyspace);
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
 * Waits for a connection to be available. If timeout expires before getting a connection it callbacks in error.
 * @param {Function} callback
 */
ControlConnection.prototype.waitForReconnection = function (callback) {
  var timeout;
  function newConnectionListener(err) {
    clearTimeout(timeout);
    callback(err);
  }
  this.once('newConnection', newConnectionListener);
  timeout = setTimeout(function waitTimeout() {
    this.removeListener('newConnection', newConnectionListener);
    callback(errors.OperationTimedOutError('A connection could not be acquired before timeout.'));
  }, metadataQueryAbortTimeout);
};

/**
 * Executes a query using the active connection
 * @param {string} cqlQuery
 * @param {function} callback
 */
ControlConnection.prototype.query = function (cqlQuery, callback) {
  var self = this;
  function queryOnConnection() {
    var request = new requests.QueryRequest(cqlQuery, null, null);
    self.connection.sendStream(request, utils.emptyObject, callback);
  }
  if (!this.connection) {
    //It's reconnecting
    return this.waitForReconnection(function waitCallback(err) {
      if (err) {
        //it was not able to reconnect in time
        return callback(err);
      }
      queryOnConnection();
    });
  }
  queryOnConnection();
};

/** @returns {Encoder} The encoder used by the current connection */
ControlConnection.prototype.getEncoder = function () {
  if (!this.encoder) {
    throw new errors.DriverInternalError('Encoder is not defined');
  }
  return this.encoder;
};

module.exports = ControlConnection;