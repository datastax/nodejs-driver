/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const events = require('events');
const util = require('util');
const net = require('net');
const dns = require('dns');

const errors = require('./errors');
const Host = require('./host').Host;
const HostMap = require('./host').HostMap;
const Metadata = require('./metadata');
const EventDebouncer = require('./metadata/event-debouncer');
const Connection = require('./connection');
const requests = require('./requests');
const utils = require('./utils');
const types = require('./types');
const promiseUtils = require('./promise-utils');
const f = util.format;

const selectPeers = "SELECT * FROM system.peers";
const selectLocal = "SELECT * FROM system.local WHERE key='local'";
const newNodeDelay = 1000;
const metadataQueryAbortTimeout = 2000;
const schemaChangeTypes = {
  created: 'CREATED',
  updated: 'UPDATED',
  dropped: 'DROPPED'
};
const supportedProductTypeKey = 'PRODUCT_TYPE';
const supportedDbaas = 'DATASTAX_APOLLO';

/**
 * Represents a connection used by the driver to receive events and to check the status of the cluster.
 * <p>It uses an existing connection from the hosts' connection pool to maintain the driver metadata up-to-date.</p>
 */
class ControlConnection extends events.EventEmitter {

  /**
   * Creates a new instance of <code>ControlConnection</code>.
   * @param {Object} options
   * @param {ProfileManager} profileManager
   * @param {{borrowHostConnection: function, createConnection: function}} [context] An object containing methods to
   * allow dependency injection.
   */
  constructor(options, profileManager, context) {
    super();

    //TODO: use underscore for private properties and methods

    this.protocolVersion = null;
    this.hosts = new HostMap();
    this.setMaxListeners(0);
    this.log = utils.log;

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

    //TODO: Replace actual methods
    this.borrowAConnectionAsync = util.promisify(this.borrowAConnection);
    this.getAddressForPeerHostAsync = util.promisify(this.getAddressForPeerHost);
  }

  /**
   * Stores the contact point information and what it resolved to.
   * @param {String|null} address
   * @param {String} port
   * @param {String} name
   * @param {Boolean} isIPv6
   */
  addContactPoint(address, port, name, isIPv6) {
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
  }

  parseEachContactPoint(name, next) {
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
  }

  /**
   * Tries to determine a suitable protocol version to be used.
   * Tries to retrieve the hosts in the Cluster.
   * @param {Function} callback
   */
  init(callback) {
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

    if (!this.options.sni) {
      utils.each(
        this.options.contactPoints,
        (name, eachNext) => this.parseEachContactPoint(name, eachNext),
        contactPointsResolutionCb);
    } else {
      this.options.contactPoints.forEach(cp => this._contactPoints.add(cp));
      const address = this.options.sni.address;
      const separatorIndex = address.lastIndexOf(':');

      if (separatorIndex === -1) {
        return callback(new errors.DriverInternalError('The SNI endpoint address should contain ip/name and port'));
      }

      const nameOrIp = address.substr(0, separatorIndex);
      this.options.sni.port = address.substr(separatorIndex + 1);
      this.options.sni.addressResolver = new utils.AddressResolver({ nameOrIp, dns });
      this.options.sni.addressResolver.init(contactPointsResolutionCb);
    }
  }

  setHealthListeners(host, connection) {
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
  }

  /**
   * Iterates through the hostIterator and gets the following open connection.
   * @param callback
   */
  borrowAConnection(callback) {
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
  }

  /** Default implementation for borrowing connections, that can be injected at constructor level */
  borrowHostConnection(host, callback) {
    // Borrow any open connection, regardless of the keyspace
    host.borrowConnection(null, null, callback);
  }

  /**
   * Default implementation for creating initial connections, that can be injected at constructor level
   * @param {String} contactPoint
   * @param {Function} callback
   */
  createConnection(contactPoint, callback) {
    const c = new Connection(contactPoint, null, this.options);
    c.open(err => {
      if (err) {
        setImmediate(() => c.close());
        return callback(err);
      }

      callback(null, c);
    });
  }

  /**
   * Gets the info from local and peer metadata, reloads the keyspaces metadata and rebuilds tokens.
   * @param {Boolean} initializing Determines whether this function was called in order to initialize the control
   * connection the first time
   * @param {Boolean} setCurrentHost
   * @param {Function} [callback]
   */
  refreshHosts(initializing, setCurrentHost, callback) {
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
  }

  //TODO: Remove counterpart
  async refreshHostsAsync(initializing, setCurrentHost) {
    // Get a reference to the current connection as it might change from external events
    const c = this.connection;

    if (!c) {
      // it's possible that this was called as a result of a topology change, but the connection was lost
      // between scheduling time and now. This will be called again when there is a new connection.
      return;
    }

    this.log('info', 'Refreshing local and peers info');

    const rsLocal = await c.send(new requests.QueryRequest(selectLocal), null);
    this.setLocalInfo(initializing, setCurrentHost, c, rsLocal);

    if (!this.host) {
      throw new errors.DriverInternalError('Information from system.local could not be retrieved');
    }

    const rsPeers = await c.send(new requests.QueryRequest(selectPeers), null);
    await this.setPeersInfoAsync(initializing, rsPeers);

    if (!this.initialized) {
      // resolve protocol version from highest common version among hosts.
      const highestCommon = types.protocolVersion.getHighestCommon(c, this.hosts);
      const reconnect = highestCommon !== this.protocolVersion;

      // set protocol version on each host.
      this.protocolVersion = highestCommon;
      this.hosts.forEach(h => h.setProtocolVersion(this.protocolVersion));

      // if protocol version changed, reconnect the control connection with new version.
      if (reconnect) {
        this.log('info', `Reconnecting since the protocol version changed to 0x${highestCommon.toString(16)}`);
        c.decreaseVersion(this.protocolVersion);
        await c.closeAsync();

        try {
          await c.openAsync();
        } catch (err) {
          // Close in the background
          c.closeAsync();

          throw err;
        }
      }

      // To acquire metadata we need to specify the cassandra version
      this.metadata.setCassandraVersion(this.host.getCassandraVersion());
      this.metadata.buildTokens(this.hosts);

      if (!this.options.isMetadataSyncEnabled) {
        this.metadata.initialized = true;
        return;
      }

      await this.metadata._refreshKeyspaces(false, true);
      this.metadata.initialized = true;
    }
  }

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
  refresh(reuseQueryPlan, callback) {
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
      function getOptions(next) {
        if (!initializing) {
          return next();
        }

        self.getSupportedOptions(next);
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
  }

  //TODO: Remove callback-based counterpart
  async refreshAsync(reuseQueryPlan) {
    const initializing = !this.initialized;

    if (this.isShuttingDown) {
      this.log('info', 'The ControlConnection will not be refreshed as the Client is being shutdown');
      throw new errors.NoHostAvailableError({}, 'ControlConnection is shutting down');
    }

    // Reset host and connection
    this.host = null;
    this.connection = null;

    try {
      //TODO: Move to method
      if (initializing) {
        this.log('info', 'Getting first connection');
        const hosts = Array.from(this._contactPoints);
        // Randomize order of contact points resolved.
        utils.shuffleArray(hosts);
        this.hostIterator = hosts.values();
      } else if (!reuseQueryPlan) {
        this.triedHosts = {};
        this.log('info', 'Trying to acquire a connection to a new host');
        this.hostIterator = await promiseUtils.newQueryPlan(this.profileManager.getDefaultLoadBalancing(), null, null);
      }

      //TODO: Consider making borrowAConnection method a pure function
      await this.borrowAConnectionAsync();

      if (initializing) {
        await this.getSupportedOptionsAsync();
      }

      this.log('info',
        (initializing
          ? `ControlConnection using protocol version 0x${this.protocolVersion.toString(16)},`
          : `ControlConnection`) +
        ` connected to ${this.connection.endpointFriendlyName}`);

      this.refreshHostsAsync(initializing, true);

      this.connection.on('nodeTopologyChange', this.nodeTopologyChangeHandler.bind(this));
      this.connection.on('nodeStatusChange', this.nodeStatusChangeHandler.bind(this));
      this.connection.on('nodeSchemaChange', this.nodeSchemaChangeHandler.bind(this));
      const request = new requests.RegisterRequest(['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE']);
      await this.connection.send(request, null);

    } catch (err) {
      // - There was a failure obtaining a connection
      // - There was a failure in metadata retrieval
      if (!this.connection) {
        this.log('error', 'ControlConnection failed to acquire a connection', err);
        if (!initializing && !this.isShuttingDown) {
          this.noOpenConnectionHandler();
          this.emit('newConnection', err);
        }

        throw err;
      }

      this.log('error', 'ControlConnection failed to retrieve topology and keyspaces information', err);
      this.triedHosts[this.connection.endpoint] = err;

      if (err.isSocketError && !initializing && this.host) {
        this.host.removeFromPool(this.connection);
      }

      // Retry the whole thing with the same query plan, in the background or foreground
      return await this.refreshAsync(true);
    }

    if (!this.connection.connected) {
      // The connection changed to a "not connected" state.
      // We have to avoid subscribing to 'down' or 'socketClosed' events after it was down / connection closed.
      // The connection is no longer valid and we should retry the whole thing
      //TODO: Revisit scenario
      this.log('info', f('Connection to %s was closed before finishing refresh', this.host.address));
      return await this.refreshAsync(false);
    }

    if (initializing) {
      // The healthy connection used to initialize should be part of the Host pool
      this.host.pool.addExistingConnection(this.connection);
    }

    this.setHealthListeners(this.host, this.connection);
    this.reconnectionSchedule = this.reconnectionPolicy.newSchedule();
    this.emit('newConnection', null, this.connection, this.host);

    this.log('info', `ControlConnection connected to ${this.connection.endpointFriendlyName} and up to date`);
  }

  /**
   * There isn't an open connection at the moment, try again later.
   */
  noOpenConnectionHandler() {
    const delay = this.reconnectionSchedule.next().value;
    this.log('warning', f('ControlConnection could not reconnect, scheduling reconnection in %dms', delay));

    setTimeout(() => this.refresh(), delay);
  }

  getSupportedOptions(callback) {
    const c = this.connection;

    c.sendStream(requests.options, null, (err, response) => {
      if (!err) {
        // response.supported is a string multi map, decoded as an Object.
        const productType = response.supported && response.supported[supportedProductTypeKey];
        if (Array.isArray(productType) && productType[0] === supportedDbaas) {
          this.metadata.setProductTypeAsDbaas();
        }
      }

      callback(err);
    });
  }

  //TODO: Remove counterpart
  async getSupportedOptionsAsync() {
    const response = await this.connection.send(requests.options, null);

    // response.supported is a string multi map, decoded as an Object.
    const productType = response.supported && response.supported[supportedProductTypeKey];
    if (Array.isArray(productType) && productType[0] === supportedDbaas) {
      this.metadata.setProductTypeAsDbaas();
    }
  }

  /**
   * Handles a TOPOLOGY_CHANGE event
   */
  nodeTopologyChangeHandler(event) {
    this.log('info', 'Received topology change', event);
    // all hosts information needs to be refreshed as tokens might have changed
    const self = this;
    clearTimeout(this.topologyChangeTimeout);
    // Use an additional timer to make sure that the refresh hosts is executed only AFTER the delay
    // In this case, the event debouncer doesn't help because it could not honor the sliding delay (ie: processNow)
    this.topologyChangeTimeout = setTimeout(() => self.scheduleRefreshHosts(), newNodeDelay);
  }

  /**
   * Handles a STATUS_CHANGE event
   */
  nodeStatusChangeHandler(event) {
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
  }

  /**
   * Handles a SCHEMA_CHANGE event
   */
  nodeSchemaChangeHandler(event) {
    this.log('info', 'Schema change', event);
    if (!this.options.isMetadataSyncEnabled) {
      return;
    }
    this.handleSchemaChange(event, false);
  }

  /**
   * @param {{keyspace: string, isKeyspace: boolean, schemaChangeType, table, udt, functionName, aggregate}} event
   * @param {Boolean} processNow
   * @param {Function} [callback]
   */
  handleSchemaChange(event, processNow, callback) {
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
  }

  /**
   * @param {Function} handler
   * @param {String} keyspaceName
   * @param {String} cqlObject
   * @param {Boolean} processNow
   * @param {Function} [callback]
   */
  scheduleObjectRefresh(handler, keyspaceName, cqlObject, processNow, callback) {
    this.debouncer.eventReceived({ handler, keyspace: keyspaceName, cqlObject: cqlObject, callback }, processNow);
  }

  /**
   * @param {String} keyspaceName
   * @param {Boolean} processNow
   * @param {Function} [callback]
   */
  scheduleKeyspaceRefresh(keyspaceName, processNow, callback) {
    this.debouncer.eventReceived({
      handler: cb => this.metadata.refreshKeyspace(keyspaceName, cb),
      keyspace: keyspaceName,
      callback
    }, processNow);
  }

  /**
   * @param {Function} [callback]
   */
  scheduleRefreshHosts(callback) {
    this.debouncer.eventReceived({
      handler: cb => this.refreshHosts(false, false, cb),
      all: true,
      callback
    }, false);
  }

  /**
   * Sets the information for the host used by the control connection.
   * @param {Boolean} initializing
   * @param {Connection} c
   * @param {Boolean} setCurrentHost Determines if the host retrieved must be set as the current host
   * @param result
   */
  setLocalInfo(initializing, setCurrentHost, c, result) {
    if (!result || !result.rows || !result.rows.length) {
      this.log('warning', 'No local info could be obtained');
      return;
    }

    const row = result.rows[0];

    let localHost;

    // Note that with SNI enabled, we can trust that rpc_address will contain a valid value.
    const endpoint = !this.options.sni
      ? c.endpoint
      : `${row['rpc_address']}:${this.options.protocolOptions.port}`;

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
    setDseParameters(localHost, row);
    this.metadata.setPartitioner(row['partitioner']);
    this.log('info', 'Local info retrieved');

    if (setCurrentHost) {
      // Set the host as the one being used by the ControlConnection.
      this.host = localHost;
    }
  }

  /**
   * @param {Boolean} initializing Determines whether this function was called in order to initialize the control
   * connection the first time.
   * @param {Error} err
   * @param {ResultSet} result
   * @param {Function} callback
   */
  setPeersInfo(initializing, err, result, callback) {
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
        setDseParameters(host, row);

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
  }

  //TODO: Remove counterpart
  async setPeersInfoAsync(initializing, err, result) {
    if (!result || !result.rows || err) {
      return;
    }

    // A map of peers, could useful for in case there are discrepancies
    const peers = {};
    const port = this.options.protocolOptions.port;
    const foundDataCenters = new Set();

    if (this.host && this.host.datacenter) {
      foundDataCenters.add(this.host.datacenter);
    }

    for (const row of result.rows) {
      const endpoint = await this.getAddressForPeerHostAsync(row, port);

      if (!endpoint) {
        return;
      }

      peers[endpoint] = true;
      let host = this.hosts.get(endpoint);
      let isNewHost = !host;

      if (isNewHost) {
        host = new Host(endpoint, this.protocolVersion, this.options, this.metadata);
        this.log('info', `Adding host ${endpoint}`);
        isNewHost = true;
      }

      host.datacenter = row['data_center'];
      host.rack = row['rack'];
      host.tokens = row['tokens'];
      host.hostId = row['host_id'];
      host.cassandraVersion = row['release_version'];
      setDseParameters(host, row);

      if (host.datacenter) {
        foundDataCenters.add(host.datacenter);
      }

      if (isNewHost) {
        // Add it to the map (and trigger events) after all the properties
        // were set to avoid race conditions
        this.hosts.set(endpoint, host);

        if (!initializing) {
          // Set the distance at Host level, that way the connection pool is created with the correct settings
          this.profileManager.getDistance(host);

          // When we are not initializing, we start with the node set as DOWN
          host.setDown();
        }
      }
    }

    // Is there a difference in number between peers + local != hosts
    if (this.hosts.length > result.rows.length + 1) {
      // There are hosts in the current state that don't belong (nodes removed or wrong contactPoints)
      this.log('info', 'Removing nodes from the pool');
      const toRemove = [];

      this.hosts.forEach(h => {
        //It is not a peer and it is not local host
        if (!peers[h.address] && h !== this.host) {
          this.log('info', 'Removing host ' + h.address);
          toRemove.push(h.address);
          h.shutdown(true);
        }
      });

      this.hosts.removeMultiple(toRemove);
    }

    if (initializing && this.options.localDataCenter) {
      const localDc = this.options.localDataCenter;

      if (!foundDataCenters.has(localDc)) {
        throw new errors.ArgumentError(`localDataCenter was configured as '${
          localDc}', but only found hosts in data centers: [${Array.from(foundDataCenters).join(', ')}]`);
      }
    }

    this.log('info', 'Peers info retrieved');
  }

  /**
   * @param {Object|Row} row
   * @param {Number} defaultPort
   * @param {Function} callback The callback to invoke with the string representation of the host endpoint,
   *  containing the ip address and port.
   */
  getAddressForPeerHost(row, defaultPort, callback) {
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
  }

  /**
   * Waits for a connection to be available. If timeout expires before getting a connection it callbacks in error.
   * @param {Function} callback
   */
  waitForReconnection(callback) {
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
  }

  /**
   * Executes a query using the active connection
   * @param {String|Request} cqlQuery
   * @param {Boolean} [waitReconnect] Determines if it should wait for reconnection in case the control connection is not
   * connected at the moment. Default: true.
   * @param {Function} callback
   */
  query(cqlQuery, waitReconnect, callback) {
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
  }

  /** @returns {Encoder} The encoder used by the current connection */
  getEncoder() {
    if (!this.encoder) {
      throw new errors.DriverInternalError('Encoder is not defined');
    }
    return this.encoder;
  }

  shutdown() {
    // no need for callback as it all sync
    this.isShuttingDown = true;
    this.debouncer.shutdown();
    // Emit a "newConnection" event with Error, as it may clear timeouts that were waiting new connections
    this.emit('newConnection', new errors.DriverError('ControlConnection is being shutdown'));
    // Cancel timers
    clearTimeout(this.topologyChangeTimeout);
    clearTimeout(this.nodeStatusChangeTimeout);
    clearTimeout(this.reconnectionTimeout);
  }

  /**
   * Resets the Connection to its initial state.
   */
  reset(callback) {
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
  }

  /**
   * Gets a Map containing the original contact points and the addresses that each one resolved to.
   */
  getResolvedContactPoints() {
    return this._resolvedContactPoints;
  }

  /**
   * Gets the local IP address to which the control connection socket is bound to.
   * @returns {String|undefined}
   */
  getLocalAddress() {
    if (!this.connection) {
      return undefined;
    }

    return this.connection.getLocalAddress();
  }

  /**
   * Gets the address and port of host the control connection is connected to.
   * @returns {String|undefined}
   */
  getEndpoint() {
    if (!this.connection) {
      return undefined;
    }

    return this.connection.endpoint;
  }
}

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

/**
 * Parses the DSE workload and assigns it to a host.
 * @param {Host} host
 * @param {Row} row
 * @private
 */
function setDseParameters(host, row) {
  if (row['workloads'] !== undefined) {
    host.workloads = row['workloads'];
  }
  else if (row['workload']) {
    host.workloads = [ row['workload'] ];
  }
  else {
    host.workloads = utils.emptyArray;
  }

  if (row['dse_version'] !== undefined) {
    host.dseVersion = row['dse_version'];
  }
}

module.exports = ControlConnection;
