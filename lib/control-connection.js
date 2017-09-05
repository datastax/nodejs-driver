"use strict";
var events = require('events');
var util = require('util');
var dns = require('dns');
var net = require('net');

var errors = require('./errors');
var Host = require('./host').Host;
var HostMap = require('./host').HostMap;
var Metadata = require('./metadata');
var EventDebouncer = require('./metadata/event-debouncer');
var requests = require('./requests');
var utils = require('./utils');
var types = require('./types');
var f = util.format;

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
 * Creates a new instance of <code>ControlConnection</code>.
 * @classdesc
 * Represents a connection used by the driver to receive events and to check the status of the cluster.
 * <p>It uses an existing connection from the hosts' connection pool to maintain the driver metadata up-to-date.</p>
 * @param {Object} options
 * @param {ProfileManager} profileManager
 * @param {{borrowHostConnection: function}} [context] An object containing methods to allow dependency injection.
 * @extends EventEmitter
 * @constructor
 */
function ControlConnection(options, profileManager, context) {
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
  this.reconnectionPolicy = this.options.policies.reconnection;
  this.reconnectionSchedule = this.reconnectionPolicy.newSchedule();
  this.initialized = false;
  this.isShuttingDown = false;
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
  this.debouncer = new EventDebouncer(options.refreshSchemaDelay, this.log.bind(this));
  this.profileManager = profileManager;
  /** Timeout used for delayed handling of topology changes */
  this.topologyChangeTimeout = null;
  /** Timeout used for delayed handling of node status changes */
  this.nodeStatusChangeTimeout = null;
  this.reconnectionTimeout = null;
  this.hostIterator = null;
  this.triedHosts = null;
  if (context && context.borrowHostConnection) {
    this.borrowHostConnection = context.borrowHostConnection;
  }
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
    var h = new Host(endPoint, self.protocolVersion, self.options, self.metadata);
    self.hosts.set(endPoint, h);
    self.log('info', 'Adding host ' + endPoint);
    if (cb) {
      cb();
    }
  }
  utils.series([
    function resolveNames(next) {
      utils.each(self.options.contactPoints, function eachResolve(name, eachNext) {
        if (name.indexOf('[') === 0 && name.indexOf(']:') > 1) {
          // IPv6 host notation [ip]:port (RFC 3986 section 3.2.2)
          var portSeparatorIndex = name.lastIndexOf(']:');
          return addHost(name.substr(1, portSeparatorIndex - 1), name.substr(portSeparatorIndex + 2), eachNext);
        }
        var host = name;
        var port = null;
        if (name.indexOf(':') > 0) {
          // IPv4 or host name with port notation
          var parts = name.split(':');
          if (parts.length === 2) {
            host = parts[0];
            port = parts[1];
          }
        }
        if (net.isIP(host)) {
          return addHost(host, port, eachNext);
        }
        resolveAll(host, function (err, addresses) {
          if (err) {
            self.log('error', 'Host with name ' + host + ' could not be resolved', err);
            return eachNext();
          }
          addresses.forEach(function (address) {
            addHost(address, port);
          });
          eachNext();
        });
      }, function (err) {
        if (!err && self.hosts.length === 0) {
          err = new errors.NoHostAvailableError(null, 'No host could be resolved');
        }
        next(err);
      });
    },
    function startRefresh(next) {
      self.refresh(false, next);
    }
  ], function seriesFinished(err) {
    self.initialized = !err;
    callback(err);
  });
};

ControlConnection.prototype.setHealthListeners = function () {
  var host = this.host;
  var connection = this.connection;
  var self = this;
  var wasRefreshCalled = 0;

  function removeListeners() {
    host.removeListener('down', downOrIgnoredHandler);
    host.removeListener('ignore', downOrIgnoredHandler);
    connection.removeListener('socketClose', socketClosedHandler);
  }

  function startReconnecting(hostDown) {
    if (wasRefreshCalled++ !== 0) {
      // Prevent multiple calls to reconnect
      return;
    }
    removeListeners();
    if (self.isShuttingDown) {
      // Don't attempt to reconnect when the ControlConnection is being shutdown
      return;
    }
    if (hostDown) {
      self.log('warning', f('Host %s used by the ControlConnection DOWN', host.address));
    }
    else {
      self.log('warning', f('Connection to %s used by the ControlConnection was closed', host.address));
    }
    self.host = null;
    self.connection = null;
    self.refresh();
  }

  function downOrIgnoredHandler() {
    startReconnecting(true);
  }

  function socketClosedHandler() {
    startReconnecting(false);
  }

  host.once('down', downOrIgnoredHandler);
  host.once('ignore', downOrIgnoredHandler);
  connection.once('socketClose', socketClosedHandler);
};

/**
 * Iterates through the hostIterator and gets the following open connection.
 * @param callback
 */
ControlConnection.prototype.borrowAConnection = function (callback) {
  var self = this;
  var host;
  var connection = null;
  utils.whilst(
    function condition() {
      // while there isn't a valid connection
      if (connection) {
        return false;
      }
      var item = self.hostIterator.next();
      host = item.value;
      return (!item.done);
    },
    function whileIterator(next) {
      if (self.initialized) {
        // Only check distance once the load-balancing policies have been initialized
        var distance = self.profileManager.getDistance(host);
        if (!host.isUp() || distance === types.distance.ignored) {
          return next();
        }
      }
      self.borrowHostConnection(host, function borrowConnectionCallback(err, c) {
        self.triedHosts[host.address] = err;
        connection = c;
        next();
      });
    },
    function whilstEnded() {
      if (!connection) {
        return callback(new errors.NoHostAvailableError(self.triedHosts));
      }
      if (!self.initialized) {
        self.protocolVersion = connection.protocolVersion;
        self.encoder = connection.encoder;
      }
      self.host = host;
      self.connection = connection;
      callback();
    });
};

/** Default implementation for borrowing connections, that can be injected at constructor level */
ControlConnection.prototype.borrowHostConnection = function (host, callback) {
  host.borrowConnection(callback);
};

/**
 * Gets the info from local and peer metadata, reloads the keyspaces metadata and rebuilds tokens.
 * @param {Boolean} newNodesUp
 * @param {Function} [callback]
 */
ControlConnection.prototype.refreshHosts = function (newNodesUp, callback) {
  callback = callback || utils.noop;
  // it's possible that this was called as a result of a topology change, but the connection was lost
  // between scheduling time and now.
  if (!this.connection) {
    callback();
    // this will be called again when there is a new connection.
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
  var host = this.host;
  utils.series([
    function getLocalInfo(next) {
      var request = new requests.QueryRequest(selectLocal, null, null);
      c.sendStream(request, null, function (err, result) {
        self.setLocalInfo(c.endpoint, result);
        next(err);
      });
    },
    function getPeersInfo(next) {
      var request = new requests.QueryRequest(selectPeers, null, null);
      c.sendStream(request, null, function (err, result) {
        self.setPeersInfo(newNodesUp, err, result, next);
      });
    },
    function getKeyspaces(next) {
      // to acquire metadata we need to specify the cassandra version
      self.metadata.setCassandraVersion(host.getCassandraVersion());
      self.metadata.buildTokens(self.hosts);
      if (!self.options.isMetadataSyncEnabled) {
        return next();
      }
      self.metadata.refreshKeyspaces(false, next);
    }
  ], callback);
};

/**
 * Acquires a connection and refreshes topology and keyspace metadata.
 * <p>If it fails obtaining a connection:</p>
 * <ul>
 *   <li>
 *     When its initializing, it should:
 *     <ul>
 *       <li>Continue iterating through the hosts</li>
 *       <li>When there aren't any more hosts, it should invoke callback with the inner errors</li>
 *     </ul>
 *   </li>
 *   <li>
 *     When its running in the background, it should:
 *     <ul>
 *       <li>Continue iterating through the hosts</li>
 *       <li>
 *         When there aren't any more hosts, it should:
 *         <ul>
 *           <li>Schedule reconnection</li>
 *           <li>Invoke callback with the inner errors</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 * <p>If it fails obtaining the metadata, it should:</p>
 * <ul>
 *   <li>It should mark connection and/or host unusable</li>
 *   <li>Retry using the same iterator from query plan / host list</li>
 * </ul>
 * @param {Boolean} [reuseQueryPlan]
 * @param {Function} [callback]
 */
ControlConnection.prototype.refresh = function (reuseQueryPlan, callback) {
  var initializing = !this.initialized;
  callback = callback || utils.noop;
  // Reset the state of the host field, that way we can identify when the query plan was exhausted
  this.host = null;
  var self = this;
  utils.series([
    function getHostIterator(next) {
      if (reuseQueryPlan) {
        return next();
      }
      self.triedHosts = {};
      if (initializing) {
        self.log('info', 'Getting first connection');
        self.hostIterator = utils.arrayIterator(self.hosts.values());
        return next();
      }
      self.log('info', 'Trying to acquire a connection to a new host');
      self.profileManager.getDefaultLoadBalancing().newQueryPlan(null, null, function onNewPlan(err, iterator) {
        if (err) {
          self.log('error', 'ControlConnection could not retrieve a query plan to determine which hosts to use', err);
          return next(err);
        }
        self.hostIterator = iterator;
        next();
      });
    },
    function getConnectionTask(next) {
      self.borrowAConnection(next);
    },
    function getLocalAndPeersInfo(next) {
      if (initializing) {
        self.log('info', f('ControlConnection using protocol version %d, connected to %s',
          self.protocolVersion, self.host.address));
      }
      else {
        self.log('info', f('ControlConnection connected to %s', self.host.address));
      }
      self.refreshHosts(initializing, next);
    },
    function subscribeConnectionEvents(next) {
      self.connection.on('nodeTopologyChange', self.nodeTopologyChangeHandler.bind(self));
      self.connection.on('nodeStatusChange', self.nodeStatusChangeHandler.bind(self));
      self.connection.on('nodeSchemaChange', self.nodeSchemaChangeHandler.bind(self));
      var request = new requests.RegisterRequest(['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE']);
      self.connection.sendStream(request, null, next);
    }
  ], function refreshSeriesEnd(err) {
    if (!err) {
      if (!self.connection.connected) {
        // Before refreshSeriesEnd() was invoked, the connection changed to a "not connected" state.
        // We have to avoid subscribing to 'down' or 'socketClosed' events after it was down / connection closed.
        // The connection is no longer valid and we should retry the whole thing
        self.log('info', f('Connection to %s was closed before finishing refresh', self.host.address));
        return self.refresh(false, callback);
      }
      self.setHealthListeners();
      self.reconnectionSchedule = self.reconnectionPolicy.newSchedule();
      self.emit('newConnection', null, self.connection, self.host);
      self.log('info', f('ControlConnection connected to %s and up to date', self.host.address));
      return callback();
    }
    if (!self.host) {
      self.log('error', 'ControlConnection failed to acquire a connection', err);
      if (!initializing) {
        self.noOpenConnectionHandler();
      }
      self.emit('newConnection', err);
      return callback(err);
    }
    self.log('error', 'ControlConnection failed to retrieve topology and keyspaces information', err);
    self.triedHosts[self.host.address] = err;
    if (err && err.isSocketError) {
      self.host.removeFromPool(self.connection);
    }
    self.connection = null;
    // Retry the whole thing with the same query plan, in the background or foreground
    self.refresh(true, callback);
  });
};

/**
 * There isn't an open connection at the moment, try again later.
 */
ControlConnection.prototype.noOpenConnectionHandler = function () {
  var delay = this.reconnectionSchedule.next().value;
  this.log('warning', f('ControlConnection could not reconnect, scheduling reconnection in %dms', delay));
  var self = this;
  setTimeout(function controlConnectionReconnection() {
    self.refresh();
  }, delay);
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
  // all hosts information needs to be refreshed as tokens might have changed
  var self = this;
  clearTimeout(this.topologyChangeTimeout);
  // Use an additional timer to make sure that the refresh hosts is executed only AFTER the delay
  // In this case, the event debouncer doesn't help because it could not honor the sliding delay (ie: processNow)
  this.topologyChangeTimeout = setTimeout(function nodeTopologyDelayedHandler() {
    self.scheduleRefreshHosts();
  }, newNodeDelay);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  var self = this;
  var addressToTranslate = event.inet.address.toString();
  var port = this.options.protocolOptions.port;
  this.addressTranslator.translate(addressToTranslate, port, function translateCallback(endPoint) {
    var host = self.hosts.get(endPoint);
    if (!host) {
      self.log('warning', 'Received status change event but host was not found: ' + addressToTranslate);
      return;
    }
    var distance = self.profileManager.getDistance(host);
    if (event.up) {
      if (distance === types.distance.ignored) {
        return host.setUp(true);
      }
      clearTimeout(self.nodeStatusChangeTimeout);
      // Waits a couple of seconds before marking it as UP
      self.nodeStatusChangeTimeout = setTimeout(function delayCheckIsUp() {
        host.checkIsUp();
      }, newNodeDelay);
      return;
    }
    // marked as down
    if (distance === types.distance.ignored) {
      return host.setDown();
    }
    self.log('warning', 'Received status change to DOWN for host ' + host.address);
  });
};

/**
 * Handles a SCHEMA_CHANGE event
 */
ControlConnection.prototype.nodeSchemaChangeHandler = function (event) {
  this.log('info', 'Schema change', event);
  if (!this.options.isMetadataSyncEnabled) {
    return;
  }
  this.handleSchemaChange(event, false);
};

/**
 * @param {{keyspace: string, isKeyspace: boolean, schemaChangeType, table, udt, functionName, aggregate}} event
 * @param {Boolean} processNow
 * @param {Function} [callback]
 */
ControlConnection.prototype.handleSchemaChange = function (event, processNow, callback) {
  var self = this;
  var handler, cqlObject;
  if (event.isKeyspace) {
    if (event.schemaChangeType === schemaChangeTypes.dropped) {
      handler = function removeKeyspace() {
        // if on the same event queue there is a creation, this handler is not going to be executed
        // it is safe to remove the keyspace metadata
        delete self.metadata.keyspaces[event.keyspace];
      };
      return this.scheduleObjectRefresh(handler, event.keyspace, null, processNow, callback);
    }
    return this.scheduleKeyspaceRefresh(event.keyspace, processNow, callback);
  }
  var ksInfo = this.metadata.keyspaces[event.keyspace];
  if (!ksInfo) {
    // it hasn't been loaded and it is not part of the metadata, don't mind
    return;
  }
  if (event.table) {
    cqlObject = event.table;
    handler = function clearTableState() {
      delete ksInfo.tables[event.table];
      delete ksInfo.views[event.table];
    };
  }
  else if (event.udt) {
    cqlObject = event.udt;
    handler = function clearUdtState() {
      delete ksInfo.udts[event.udt];
    };
  }
  else if (event.functionName) {
    cqlObject = event.functionName;
    handler = function clearFunctionState() {
      delete ksInfo.functions[event.functionName];
    };
  }
  else if (event.aggregate) {
    cqlObject = event.aggregate;
    handler = function clearKeyspaceState() {
      delete ksInfo.aggregates[event.aggregate];
    };
  }
  if (handler) {
    // is a cql object change clean the internal cache
    this.scheduleObjectRefresh(handler, event.keyspace, cqlObject, processNow, callback);
  }
};

/**
 * @param {Function} handler
 * @param {String} keyspaceName
 * @param {String} cqlObject
 * @param {Boolean} processNow
 * @param {Function} [callback]
 */
ControlConnection.prototype.scheduleObjectRefresh = function (handler, keyspaceName, cqlObject, processNow, callback) {
  this.debouncer.eventReceived(
    { handler: handler, keyspace: keyspaceName, cqlObject: cqlObject, callback: callback },
    processNow
  );
};

/**
 * @param {String} keyspaceName
 * @param {Boolean} processNow
 * @param {Function} [callback]
 */
ControlConnection.prototype.scheduleKeyspaceRefresh = function (keyspaceName, processNow, callback) {
  var self = this;
  var handler = function keyspaceRefreshHandler(cb) {
    self.metadata.refreshKeyspace(keyspaceName, cb);
  };
  this.debouncer.eventReceived({ handler: handler, keyspace: keyspaceName, callback: callback}, processNow);
};

/**
 * @param {Function} [callback]
 */
ControlConnection.prototype.scheduleRefreshHosts = function (callback) {
  var self = this;
  var handler = function hostsRefreshHandler(cb) {
    self.refreshHosts(false, cb);
  };
  this.debouncer.eventReceived({ handler: handler, all: true, callback: callback }, false);
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
 * @param {Error} err
 * @param {ResultSet} result
 * @param {Function} callback
 */
ControlConnection.prototype.setPeersInfo = function (newNodesUp, err, result, callback) {
  if (!result || !result.rows || err) {
    return callback(err);
  }
  var self = this;
  //A map of peers, could useful for in case there are discrepancies
  var peers = {};
  var port = this.options.protocolOptions.port;
  utils.eachSeries(result.rows, function eachPeer(row, next) {
    self.getAddressForPeerHost(row, port, function getAddressForPeerCallback(endPoint) {
      if (!endPoint) {
        return next();
      }
      peers[endPoint] = true;
      var host = self.hosts.get(endPoint);
      if (!host) {
        host = new Host(endPoint, self.protocolVersion, self.options, self.metadata);
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
          h.shutdown(true);
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
    this.log('error', f('No rpc_address found for host %s in %s\'s peers system table. %s will be ignored.',
      peer, this.host.address, peer));
    return callback(null);
  }
  if (address.toString() === bindAllAddress) {
    this.log('warning', f('Found host with 0.0.0.0 as rpc_address, using listen_address (%s) to contact it instead.' +
      ' If this is incorrect you should avoid the use of 0.0.0.0 server side.', peer));
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
  var self = this;
  function newConnectionListener(err) {
    clearTimeout(timeout);
    callback(err);
  }
  this.once('newConnection', newConnectionListener);
  timeout = setTimeout(function waitTimeout() {
    self.removeListener('newConnection', newConnectionListener);
    callback(new errors.OperationTimedOutError('A connection could not be acquired before timeout.'));
  }, metadataQueryAbortTimeout);
};

/**
 * Executes a query using the active connection
 * @param {String|Request} cqlQuery
 * @param {Boolean} [waitReconnect] Determines if it should wait for reconnection in case the control connection is not
 * connected at the moment. Default: true.
 * @param {Function} callback
 */
ControlConnection.prototype.query = function (cqlQuery, waitReconnect, callback) {
  var self = this;
  if (typeof waitReconnect === 'function') {
    callback = waitReconnect;
    waitReconnect = true;
  }
  function queryOnConnection() {
    var request = typeof cqlQuery === 'string' ? new requests.QueryRequest(cqlQuery, null, null) : cqlQuery;
    self.connection.sendStream(request, null, callback);
  }
  if (!this.connection) {
    if (!waitReconnect) {
      return callback(new errors.NoHostAvailableError(null));
    }
    // Wait until its reconnected (or timer elapses)
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

ControlConnection.prototype.shutdown = function () {
  // no need for callback as it all sync
  this.isShuttingDown = true;
  this.debouncer.shutdown();
  // Emit a "newConnection" event with Error, as it may clear timeouts that were waiting new connections
  this.emit('newConnection', new errors.DriverError('ControlConnection is being shutdown'));
  // Cancel timers
  clearTimeout(this.topologyChangeTimeout);
  clearTimeout(this.nodeStatusChangeTimeout);
  clearTimeout(this.reconnectionTimeout);
};

/**
 * Resets the Connection to its initial state.
 */
ControlConnection.prototype.reset = function (callback) {
  // Reset the internal state of the ControlConnection for future initialization attempts
  var currentHosts = this.hosts.clear();
  // Set the shutting down flag temporarily to avoid reconnects.
  this.isShuttingDown = true;
  var self = this;
  utils.each(currentHosts, function forEachHost(h, next) {
    h.shutdown(false, function () {
      // Ignore any shutdown error
      next();
    });
  }, function shuttingDownFinished() {
    self.initialized = false;
    self.isShuttingDown = false;
    callback();
  });
};

/**
 * Uses the DNS protocol to resolve a IPv4 and IPv6 addresses (A and AAAA records) for the hostname
 * @private
 * @param name
 * @param callback
 */
function resolveAll(name, callback) {
  var addresses = [];
  utils.parallel([
    function resolve4(next) {
      dns.resolve4(name, function resolve4Callback(err, arr) {
        if (arr) {
          addresses.push.apply(addresses, arr);
        }
        // Ignore error
        next();
      });
    },
    function resolve6(next) {
      dns.resolve6(name, function resolve6Callback(err, arr) {
        if (arr) {
          addresses.push.apply(addresses, arr);
        }
        // Ignore error
        next();
      });
    }
  ], function resolveAllCallback() {
    if (addresses.length === 0) {
      // In case dns.resolve*() methods don't yield a valid address for the host name
      // Use system call getaddrinfo() that might resolve according to host system definitions
      return dns.lookup(name, function (err, addr) {
        if (err) {
          return callback(err);
        }
        addresses.push(addr);
        callback(null, addresses);
      });
    }
    callback(null, addresses);
  });
}

module.exports = ControlConnection;
