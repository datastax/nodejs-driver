"use strict";
const events = require('events');
const util = require('util');
const net = require('net');

const errors = require('./errors');
const Host = require('./host').Host;
const HostMap = require('./host').HostMap;
const Metadata = require('./metadata');
const EventDebouncer = require('./metadata/event-debouncer');
const Connection = require('./connection');
const requests = require('./requests');
const utils = require('./utils');
const types = require('./types');
const f = util.format;
// eslint-disable-next-line prefer-const
let dns = require('dns');

const selectPeers = "SELECT * FROM system.peers";
const selectLocal = "SELECT * FROM system.local WHERE key='local'";
const newNodeDelay = 1000;
const metadataQueryAbortTimeout = 2000;
const schemaChangeTypes = {
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
 * @param {{borrowHostConnection: function, createConnection: function}} [context] An object containing methods to
 * allow dependency injection.
 * @extends EventEmitter
 * @constructor
 */
function ControlConnection(options, profileManager, context) {
  this.protocolVersion = null;
  this.hosts = new HostMap();
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
  this._resolvedContactPoints = new Map();
  this._contactPoints = new Set();

  if (context && context.borrowHostConnection) {
    this.borrowHostConnection = context.borrowHostConnection;
  }

  if (context && context.createConnection) {
    this.createConnection = context.createConnection;
  }
}

util.inherits(ControlConnection, events.EventEmitter);

/**
 * Stores the contact point information and what it resolved to.
 * @param {String|null} address
 * @param {String} port
 * @param {String} name
 * @param {Boolean} isIPv6
 */
ControlConnection.prototype.addContactPoint = function(address, port, name, isIPv6) {
  if (address === null) {
    // Contact point could not be resolved, store that the resolution came back empty
    this._resolvedContactPoints.set(name, utils.emptyArray);
    return;
  }

  const portNumber = parseInt(port, 10) || this.options.protocolOptions.port;
  const endpoint = `${address}:${portNumber}`;
  this._contactPoints.add(endpoint);

  // Use RFC 3986 for IPv4 and IPv6
  const standardEndpoint = !isIPv6 ? endpoint : `[${address}]:${portNumber}`;

  let resolvedAddressedByName = this._resolvedContactPoints.get(name);
  if (resolvedAddressedByName === undefined) {
    resolvedAddressedByName = [];
    this._resolvedContactPoints.set(name, resolvedAddressedByName);
  }

  resolvedAddressedByName.push(standardEndpoint);
};

ControlConnection.prototype.parseEachContactPoint = function(name, next) {
  let addressOrName = name;
  let port = null;

  if (name.indexOf('[') === 0 && name.indexOf(']:') > 1) {
    // IPv6 host notation [ip]:port (RFC 3986 section 3.2.2)
    const index = name.lastIndexOf(']:');
    addressOrName = name.substr(1, index - 1);
    port = name.substr(index + 2);
  } else if (name.indexOf(':') > 0) {
    // IPv4 or host name with port notation
    const parts = name.split(':');
    if (parts.length === 2) {
      addressOrName = parts[0];
      port = parts[1];
    }
  }

  if (net.isIP(addressOrName)) {
    this.addContactPoint(addressOrName, port, name, net.isIPv6(addressOrName));
    return next();
  }

  resolveAll(addressOrName, (err, addresses) => {
    if (err) {
      this.log('error', `Host with name ${addressOrName} could not be resolved`, err);
      this.addContactPoint(null, null, name, false);
      return next();
    }

    addresses.forEach(addressInfo => this.addContactPoint(addressInfo.address, port, name, addressInfo.isIPv6));

    next();
  });
};

/**
 * Tries to determine a suitable protocol version to be used.
 * Tries to retrieve the hosts in the Cluster.
 * @param {Function} callback
 */
ControlConnection.prototype.init = function (callback) {
  if (this.initialized) {
    // Prevent multiple serial initializations
    return callback();
  }

  const contactPointsResolutionCb = (err) => {
    if (!err && this._contactPoints.size === 0) {
      err = new errors.NoHostAvailableError({}, 'No host could be resolved');
    }

    if (err) {
      return callback(err);
    }

    this.refresh(false, err => {
      this.initialized = !err;
      callback(err);
    });
  };

  utils.each(
    this.options.contactPoints,
    (name, eachNext) => this.parseEachContactPoint(name, eachNext),
    contactPointsResolutionCb);
};

ControlConnection.prototype.setHealthListeners = function (host, connection) {
  const self = this;
  let wasRefreshCalled = 0;

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
      self.log('warning',
        `Host ${host.address} used by the ControlConnection DOWN, ` +
        `connection to ${connection.endpointFriendlyName} will not longer by used`);
    } else {
      self.log('warning', `Connection to ${connection.endpointFriendlyName} used by the ControlConnection was closed`);
    }

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
  const self = this;
  let host;
  let connection = null;

  utils.whilst(
    function condition() {
      // while there isn't a valid connection
      if (connection) {
        return false;
      }
      const item = self.hostIterator.next();
      host = item.value;
      return (!item.done);
    },
    function whileIterator(next) {
      if (self.initialized) {
        // Only check distance once the load-balancing policies have been initialized
        const distance = self.profileManager.getDistance(host);
        if (!host.isUp() || distance === types.distance.ignored) {
          return next();
        }

        self.borrowHostConnection(host, function (err, c) {
          self.triedHosts[host.address] = err;
          connection = c;
          next();
        });
      } else {
        // Host is an endpoint string
        self.createConnection(host, (err, c) => {
          self.triedHosts[host] = err;
          connection = c;
          next();
        });
      }
    },
    function whilstEnded() {
      if (!connection) {
        return callback(new errors.NoHostAvailableError(self.triedHosts));
      }

      if (!self.initialized) {
        self.protocolVersion = connection.protocolVersion;
        self.encoder = connection.encoder;
      }

      self.connection = connection;
      callback();
    });
};

/** Default implementation for borrowing connections, that can be injected at constructor level */
ControlConnection.prototype.borrowHostConnection = function (host, callback) {
  // Borrow any open connection, regardless of the keyspace
  host.borrowConnection(null, null, callback);
};

/**
 * Default implementation for creating initial connections, that can be injected at constructor level
 * @param {String} contactPoint
 * @param {Function} callback
 */
ControlConnection.prototype.createConnection = function (contactPoint, callback) {
  const c = new Connection(contactPoint, null, this.options);
  c.open(err => {
    if (err) {
      setImmediate(() => c.close());
      return callback(err);
    }

    callback(null, c);
  });
};

/**
 * Gets the info from local and peer metadata, reloads the keyspaces metadata and rebuilds tokens.
 * @param {Boolean} initializing Determines whether this function was called in order to initialize the control
 * connection the first time
 * @param {Boolean} setCurrentHost
 * @param {Function} [callback]
 */
ControlConnection.prototype.refreshHosts = function (initializing, setCurrentHost, callback) {
  callback = callback || utils.noop;

  // Get a reference to the current connection as it might change from external events
  const c = this.connection;

  if (!c) {
    // it's possible that this was called as a result of a topology change, but the connection was lost
    // between scheduling time and now. This will be called again when there is a new connection.
    return callback();
  }

  const self = this;
  this.log('info', 'Refreshing local and peers info');

  utils.series([
    function getLocalInfo(next) {
      const request = new requests.QueryRequest(selectLocal, null, null);
      c.sendStream(request, null, function (err, result) {
        self.setLocalInfo(initializing, setCurrentHost, c, result);

        if (!err && !self.host) {
          return next(new errors.DriverInternalError('Information from system.local could not be retrieved'));
        }

        next(err);
      });
    },
    function getPeersInfo(next) {
      const request = new requests.QueryRequest(selectPeers, null, null);
      c.sendStream(request, null, function (err, result) {
        self.setPeersInfo(initializing, err, result, next);
      });
    },
    function resolveAndSetProtocolVersion(next) {
      if (!self.initialized) {
        // resolve protocol version from highest common version among hosts.
        const highestCommon = types.protocolVersion.getHighestCommon(c, self.hosts);
        const reconnect = highestCommon !== self.protocolVersion;

        // set protocol version on each host.
        self.protocolVersion = highestCommon;
        self.hosts.forEach(h => h.setProtocolVersion(self.protocolVersion));

        // if protocol version changed, reconnect the control connection with new version.
        if (reconnect) {
          self.log('info', `Reconnecting since the protocol version changed to 0x${highestCommon.toString(16)}`);
          c.decreaseVersion(self.protocolVersion);
          c.close(() =>
            setImmediate(() => c.open(err => {
              if (err) {
                c.close();
              }

              next(err);
            })));
          return;
        }
      }
      next();
    },
    function getKeyspaces(next) {
      // to acquire metadata we need to specify the cassandra version
      self.metadata.setCassandraVersion(self.host.getCassandraVersion());
      self.metadata.buildTokens(self.hosts);
      if (!self.options.isMetadataSyncEnabled) {
        self.metadata.initialized = true;
        return next();
      }
      self.metadata._refreshKeyspaces(false, true, () => {
        self.metadata.initialized = true;
        next();
      });
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
  const initializing = !this.initialized;
  callback = callback || utils.noop;

  if (this.isShuttingDown) {
    this.log('info', 'The ControlConnection will not be refreshed as the Client is being shutdown');
    return callback(new errors.NoHostAvailableError({}, 'ControlConnection is shutting down'));
  }

  // Reset host and connection
  this.host = null;
  this.connection = null;

  const self = this;

  utils.series([
    function getHostIterator(next) {
      if (reuseQueryPlan) {
        return next();
      }

      self.triedHosts = {};

      if (initializing) {
        self.log('info', 'Getting first connection');
        const hosts = Array.from(self._contactPoints);
        // Randomize order of contact points resolved.
        utils.shuffleArray(hosts);
        self.hostIterator = hosts[Symbol.iterator]();
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
      self.log('info',
        (initializing
          ? `ControlConnection using protocol version 0x${self.protocolVersion.toString(16)},`
          : `ControlConnection`) +
        ` connected to ${self.connection.endpointFriendlyName}`);

      self.refreshHosts(initializing, true, next);
    },
    function subscribeConnectionEvents(next) {
      self.connection.on('nodeTopologyChange', self.nodeTopologyChangeHandler.bind(self));
      self.connection.on('nodeStatusChange', self.nodeStatusChangeHandler.bind(self));
      self.connection.on('nodeSchemaChange', self.nodeSchemaChangeHandler.bind(self));
      const request = new requests.RegisterRequest(['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE']);
      self.connection.sendStream(request, null, next);
    }
  ], function refreshSeriesEnd(err) {
    // Refresh ended, possible scenarios:
    // - There was a failure obtaining a connection
    // - There was a failure in metadata retrieval
    // - There wasn't a failure but connection is now disconnected at this time
    // - Everything succeeded
    if (!err) {
      if (!self.connection.connected) {
        // Before refreshSeriesEnd() was invoked, the connection changed to a "not connected" state.
        // We have to avoid subscribing to 'down' or 'socketClosed' events after it was down / connection closed.
        // The connection is no longer valid and we should retry the whole thing
        self.log('info', f('Connection to %s was closed before finishing refresh', self.host.address));
        return self.refresh(false, callback);
      }

      if (initializing) {
        // The healthy connection used to initialize should be part of the Host pool
        self.host.pool.addExistingConnection(self.connection);
      }

      self.setHealthListeners(self.host, self.connection);
      self.reconnectionSchedule = self.reconnectionPolicy.newSchedule();
      self.emit('newConnection', null, self.connection, self.host);
      self.log('info', `ControlConnection connected to ${self.connection.endpointFriendlyName} and up to date`);

      return callback();
    }

    if (!self.connection) {
      self.log('error', 'ControlConnection failed to acquire a connection', err);
      if (!initializing && !self.isShuttingDown) {
        self.noOpenConnectionHandler();
        self.emit('newConnection', err);
      }

      return callback(err);
    }

    self.log('error', 'ControlConnection failed to retrieve topology and keyspaces information', err);
    self.triedHosts[self.connection.endpoint] = err;

    if (err && err.isSocketError && !initializing && self.host) {
      self.host.removeFromPool(self.connection);
    }

    // Retry the whole thing with the same query plan, in the background or foreground
    self.refresh(true, callback);
  });
};

/**
 * There isn't an open connection at the moment, try again later.
 */
ControlConnection.prototype.noOpenConnectionHandler = function () {
  const delay = this.reconnectionSchedule.next().value;
  this.log('warning', f('ControlConnection could not reconnect, scheduling reconnection in %dms', delay));
  const self = this;
  setTimeout(() => self.refresh(), delay);
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
  const self = this;
  clearTimeout(this.topologyChangeTimeout);
  // Use an additional timer to make sure that the refresh hosts is executed only AFTER the delay
  // In this case, the event debouncer doesn't help because it could not honor the sliding delay (ie: processNow)
  this.topologyChangeTimeout = setTimeout(() => self.scheduleRefreshHosts(), newNodeDelay);
};

/**
 * Handles a STATUS_CHANGE event
 */
ControlConnection.prototype.nodeStatusChangeHandler = function (event) {
  const self = this;
  const addressToTranslate = event.inet.address.toString();
  const port = this.options.protocolOptions.port;
  this.addressTranslator.translate(addressToTranslate, port, function translateCallback(endPoint) {
    const host = self.hosts.get(endPoint);
    if (!host) {
      self.log('warning', 'Received status change event but host was not found: ' + addressToTranslate);
      return;
    }
    const distance = self.profileManager.getDistance(host);
    if (event.up) {
      if (distance === types.distance.ignored) {
        return host.setUp(true);
      }
      clearTimeout(self.nodeStatusChangeTimeout);
      // Waits a couple of seconds before marking it as UP
      self.nodeStatusChangeTimeout = setTimeout(() => host.checkIsUp(), newNodeDelay);
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
  const self = this;
  let handler, cqlObject;
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
  const ksInfo = this.metadata.keyspaces[event.keyspace];
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
  this.debouncer.eventReceived({ handler, keyspace: keyspaceName, cqlObject: cqlObject, callback }, processNow);
};

/**
 * @param {String} keyspaceName
 * @param {Boolean} processNow
 * @param {Function} [callback]
 */
ControlConnection.prototype.scheduleKeyspaceRefresh = function (keyspaceName, processNow, callback) {
  this.debouncer.eventReceived({
    handler: cb => this.metadata.refreshKeyspace(keyspaceName, cb),
    keyspace: keyspaceName,
    callback
  }, processNow);
};

/**
 * @param {Function} [callback]
 */
ControlConnection.prototype.scheduleRefreshHosts = function (callback) {
  this.debouncer.eventReceived({
    handler: cb => this.refreshHosts(false, false, cb),
    all: true,
    callback
  }, false);
};

/**
 * Sets the information for the host used by the control connection.
 * @param {Boolean} initializing
 * @param {Connection} c
 * @param {Boolean} setCurrentHost Determines if the host retrieved must be set as the current host
 * @param result
 */
ControlConnection.prototype.setLocalInfo = function (initializing, setCurrentHost, c, result) {
  if (!result || !result.rows || !result.rows.length) {
    this.log('warning', 'No local info could be obtained');
    return;
  }

  const row = result.rows[0];

  let localHost;

  const endpoint = c.endpoint;

  if (initializing) {
    localHost = new Host(endpoint, this.protocolVersion, this.options, this.metadata);
    this.hosts.set(endpoint, localHost);
    this.log('info', `Adding host ${endpoint}`);
  } else {
    localHost = this.hosts.get(endpoint);

    if (!localHost) {
      this.log('error', 'Localhost could not be found');
      return;
    }
  }

  localHost.datacenter = row['data_center'];
  localHost.rack = row['rack'];
  localHost.tokens = row['tokens'];
  localHost.hostId = row['host_id'];
  localHost.cassandraVersion = row['release_version'];
  this.metadata.setPartitioner(row['partitioner']);
  this.log('info', 'Local info retrieved');

  if (setCurrentHost) {
    // Set the host as the one being used by the ControlConnection.
    this.host = localHost;
  }
};

/**
 * @param {Boolean} initializing Determines whether this function was called in order to initialize the control
 * connection the first time.
 * @param {Error} err
 * @param {ResultSet} result
 * @param {Function} callback
 */
ControlConnection.prototype.setPeersInfo = function (initializing, err, result, callback) {
  if (!result || !result.rows || err) {
    return callback(err);
  }

  // A map of peers, could useful for in case there are discrepancies
  const peers = {};
  const port = this.options.protocolOptions.port;
  const foundDataCenters = new Set();

  if (this.host && this.host.datacenter) {
    foundDataCenters.add(this.host.datacenter);
  }

  const self = this;

  utils.eachSeries(result.rows, function eachPeer(row, next) {
    self.getAddressForPeerHost(row, port, function getAddressForPeerCallback(endPoint) {
      if (!endPoint) {
        return next();
      }

      peers[endPoint] = true;
      let host = self.hosts.get(endPoint);
      let isNewHost = !host;

      if (isNewHost) {
        host = new Host(endPoint, self.protocolVersion, self.options, self.metadata);
        self.log('info', 'Adding host ' + endPoint);
        isNewHost = true;
      }

      host.datacenter = row['data_center'];
      host.rack = row['rack'];
      host.tokens = row['tokens'];
      host.hostId = row['host_id'];
      host.cassandraVersion = row['release_version'];

      if (host.datacenter) {
        foundDataCenters.add(host.datacenter);
      }

      if (isNewHost) {
        // Add it to the map (and trigger events) after all the properties
        // were set to avoid race conditions
        self.hosts.set(endPoint, host);

        if (!initializing) {
          // Set the distance at Host level, that way the connection pool is created with the correct settings
          self.profileManager.getDistance(host);

          // When we are not initializing, we start with the node set as DOWN
          host.setDown();
        }
      }

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
      const toRemove = [];
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
    if (initializing && self.options.localDataCenter) {
      const localDc = self.options.localDataCenter;

      if (!foundDataCenters.has(localDc)) {
        return callback(new errors.ArgumentError('localDataCenter was configured as \'' + localDc + '\', but only found' +
          ' hosts in data centers: [' + Array.from(foundDataCenters).join(', ') + ']'));
      }
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
  let address = row['rpc_address'];
  const peer = row['peer'];
  const bindAllAddress = '0.0.0.0';
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
  // eslint-disable-next-line prefer-const
  let timeout;
  const self = this;
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
  if (typeof waitReconnect === 'function') {
    callback = waitReconnect;
    waitReconnect = true;
  }

  const self = this;

  function queryOnConnection() {
    if (!self.connection || self.isShuttingDown) {
      return callback(new errors.NoHostAvailableError({}, 'ControlConnection is not connected at the time'));
    }

    const request = typeof cqlQuery === 'string' ? new requests.QueryRequest(cqlQuery, null, null) : cqlQuery;
    self.connection.sendStream(request, null, callback);
  }

  if (!this.connection && waitReconnect) {
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
  const currentHosts = this.hosts.clear();
  // Set the shutting down flag temporarily to avoid reconnects.
  this.isShuttingDown = true;
  const self = this;
  // Ignore any shutdown error
  utils.each(currentHosts, (h, next) => h.shutdown(false, () => next()), function shuttingDownFinished() {
    self.initialized = false;
    self.isShuttingDown = false;
    callback();
  });
};

/**
 * Gets a Map containing the original contact points and the addresses that each one resolved to.
 */
ControlConnection.prototype.getResolvedContactPoints = function () {
  return this._resolvedContactPoints;
};

/**
 * Gets the local IP address to which the control connection socket is bound to.
 * @returns {String|undefined}
 */
ControlConnection.prototype.getLocalAddress = function () {
  if (!this.connection) {
    return undefined;
  }

  return this.connection.getLocalAddress();
};

/**
 * Gets the address and port of host the control connection is connected to.
 * @returns {String|undefined}
 */
ControlConnection.prototype.getEndpoint = function () {
  if (!this.connection) {
    return undefined;
  }

  return this.connection.endpoint;
};

/**
 * Uses the DNS protocol to resolve a IPv4 and IPv6 addresses (A and AAAA records) for the hostname
 * @private
 * @param name
 * @param callback
 */
function resolveAll(name, callback) {
  const addresses = [];
  utils.parallel([
    function resolve4(next) {
      dns.resolve4(name, function resolve4Callback(err, arr) {
        if (arr) {
          arr.forEach(address => addresses.push({ address, isIPv6: false }));
        }
        // Ignore error
        next();
      });
    },
    function resolve6(next) {
      dns.resolve6(name, function resolve6Callback(err, arr) {
        if (arr) {
          arr.forEach(address => addresses.push({ address, isIPv6: true }));
        }
        // Ignore error
        next();
      });
    }
  ], function resolveAllCallback() {
    if (addresses.length === 0) {
      // In case dns.resolve*() methods don't yield a valid address for the host name
      // Use system call getaddrinfo() that might resolve according to host system definitions
      return dns.lookup(name, function (err, address, family) {
        if (err) {
          return callback(err);
        }

        addresses.push({ address, isIPv6: family === 6 });
        callback(null, addresses);
      });
    }
    callback(null, addresses);
  });
}

module.exports = ControlConnection;
