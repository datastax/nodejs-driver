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
const { Host, HostMap } = require('./host');
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

    this.protocolVersion = null;
    this.hosts = new HostMap();
    this.setMaxListeners(0);
    this.log = utils.log;
    Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});

    /**
     * Cluster metadata that is going to be shared between the Client and ControlConnection
     */
    this.metadata = new Metadata(this.options, this);
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

    this._addressTranslator = this.options.policies.addressResolution;
    this._reconnectionPolicy = this.options.policies.reconnection;
    this._reconnectionSchedule = this._reconnectionPolicy.newSchedule();
    this._isShuttingDown = false;

    // Reference to the encoder of the last valid connection
    this._encoder = null;
    this._debouncer = new EventDebouncer(options.refreshSchemaDelay, this.log.bind(this));
    this._profileManager = profileManager;
    this._triedHosts = null;
    this._resolvedContactPoints = new Map();
    this._contactPoints = new Set();

    // Timeout used for delayed handling of topology changes
    this._topologyChangeTimeout = null;
    // Timeout used for delayed handling of node status changes
    this._nodeStatusChangeTimeout = null;

    if (context && context.borrowHostConnection) {
      this._borrowHostConnection = context.borrowHostConnection;
    }

    if (context && context.createConnection) {
      this._createConnection = context.createConnection;
    }
  }

  /**
   * Stores the contact point information and what it resolved to.
   * @param {String|null} address
   * @param {String} port
   * @param {String} name
   * @param {Boolean} isIPv6
   */
  _addContactPoint(address, port, name, isIPv6) {
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

    // NODEJS-646
    //
    // We might have a frozen empty array if DNS resolution wasn't working when this name was
    // initially added, and if that's the case we can't add anything.  Detect that case and
    // reset to a mutable array.
    if (resolvedAddressedByName === undefined || resolvedAddressedByName === utils.emptyArray) {
      resolvedAddressedByName = [];
      this._resolvedContactPoints.set(name, resolvedAddressedByName);
    }

    resolvedAddressedByName.push(standardEndpoint);
  }

  async _parseContactPoint(name) {
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
      this._addContactPoint(addressOrName, port, name, net.isIPv6(addressOrName));
      return;
    }

    const addresses = await this._resolveAll(addressOrName);
    if (addresses.length > 0) {
      addresses.forEach(addressInfo => this._addContactPoint(addressInfo.address, port, name, addressInfo.isIPv6));
    } else {
      // Store that we attempted resolving the name but was not found
      this._addContactPoint(null, null, name, false);
    }
  }

  /**
   * Initializes the control connection by establishing a Connection using a suitable protocol
   * version to be used and retrieving cluster metadata.
   */
  async init() {
    if (this.initialized) {
      // Prevent multiple serial initializations
      return;
    }

    if (!this.options.sni) {
      // Parse and resolve contact points
      await Promise.all(this.options.contactPoints.map(name => this._parseContactPoint(name)));
    } else {
      this.options.contactPoints.forEach(cp => this._contactPoints.add(cp));
      const address = this.options.sni.address;
      const separatorIndex = address.lastIndexOf(':');

      if (separatorIndex === -1) {
        throw new new errors.DriverInternalError('The SNI endpoint address should contain ip/name and port');
      }

      const nameOrIp = address.substr(0, separatorIndex);
      this.options.sni.port = address.substr(separatorIndex + 1);
      this.options.sni.addressResolver = new utils.AddressResolver({ nameOrIp, dns });
      await this.options.sni.addressResolver.init();
    }

    if (this._contactPoints.size === 0) {
      throw new errors.NoHostAvailableError({}, 'No host could be resolved');
    }

    await this._initializeConnection();
  }

  _setHealthListeners(host, connection) {
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

      if (self._isShuttingDown) {
        // Don't attempt to reconnect when the ControlConnection is being shutdown
        return;
      }

      if (hostDown) {
        self.log('warning',
          `Host ${host.address} used by the ControlConnection DOWN, ` +
          `connection to ${connection.endpointFriendlyName} will not longer be used`);
      } else {
        self.log('warning', `Connection to ${connection.endpointFriendlyName} used by the ControlConnection was closed`);
      }

      promiseUtils.toBackground(self._refresh());
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
   * Iterates through the hostIterator and Gets the following open connection.
   * @param {Iterator<Host>} hostIterator
   * @returns {Connection!}
   */
  _borrowAConnection(hostIterator) {
    let connection = null;

    while (!connection) {
      const item = hostIterator.next();
      const host = item.value;

      if (item.done) {
        throw new errors.NoHostAvailableError(this._triedHosts);
      }

      // Only check distance once the load-balancing policies have been initialized
      const distance = this._profileManager.getDistance(host);
      if (!host.isUp() || distance === types.distance.ignored) {
        continue;
      }

      try {
        connection = this._borrowHostConnection(host);
      } catch (err) {
        this._triedHosts[host.address] = err;
      }
    }

    return connection;
  }

  /**
   * Iterates through the contact points and tries to open a connection.
   * @param {Iterator<string>} contactPointsIterator
   * @returns {Promise<void>}
   */
  async _borrowFirstConnection(contactPointsIterator) {
    let connection = null;

    while (!connection) {
      const item = contactPointsIterator.next();
      const contactPoint = item.value;

      if (item.done) {
        throw new errors.NoHostAvailableError(this._triedHosts);
      }

      try {
        connection = await this._createConnection(contactPoint);
      } catch (err) {
        this._triedHosts[contactPoint] = err;
      }
    }

    if (!connection) {
      const err = new errors.NoHostAvailableError(this._triedHosts);
      this.log('error', 'ControlConnection failed to acquire a connection');
      throw err;
    }

    this.protocolVersion = connection.protocolVersion;
    this._encoder = connection.encoder;
    this.connection = connection;
  }

  /** Default implementation for borrowing connections, that can be injected at constructor level */
  _borrowHostConnection(host) {
    // Borrow any open connection, regardless of the keyspace
    return host.borrowConnection();
  }

  /**
   * Default implementation for creating initial connections, that can be injected at constructor level
   * @param {String} contactPoint
   */
  async _createConnection(contactPoint) {
    const c = new Connection(contactPoint, null, this.options);

    try {
      await c.openAsync();
    } catch (err) {
      promiseUtils.toBackground(c.closeAsync());
      throw err;
    }

    return c;
  }

  /**
   * Gets the info from local and peer metadata, reloads the keyspaces metadata and rebuilds tokens.
   * <p>It throws an error when there's a failure or when reconnecting and there's no connection.</p>
   * @param {Boolean} initializing Determines whether this function was called in order to initialize the control
   * connection the first time
   * @param {Boolean} isReconnecting Determines whether the refresh is being done because the ControlConnection is
   * switching to use this connection to this host.
   */
  async _refreshHosts(initializing, isReconnecting) {
    // Get a reference to the current connection as it might change from external events
    const c = this.connection;

    if (!c) {
      if (isReconnecting) {
        throw new errors.DriverInternalError('Connection reference has been lost when reconnecting');
      }

      // it's possible that this was called as a result of a topology change, but the connection was lost
      // between scheduling time and now. This will be called again when there is a new connection.
      return;
    }

    this.log('info', 'Refreshing local and peers info');

    const rsLocal = await c.send(new requests.QueryRequest(selectLocal), null);
    this._setLocalInfo(initializing, isReconnecting, c, rsLocal);

    if (!this.host) {
      throw new errors.DriverInternalError('Information from system.local could not be retrieved');
    }

    const rsPeers = await c.send(new requests.QueryRequest(selectPeers), null);
    await this.setPeersInfo(initializing, rsPeers);

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
          promiseUtils.toBackground(c.closeAsync());

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

      await this.metadata.refreshKeyspacesInternal(false);
      this.metadata.initialized = true;
    }
  }

  async _refreshControlConnection(hostIterator) {

    if (this.options.sni) {
      this.connection = this._borrowAConnection(hostIterator);
    }
    else {
      try { this.connection = this._borrowAConnection(hostIterator); }
      catch(err) {

        /* NODEJS-632: refresh nodes before getting hosts for reconnect since some hostnames may have
         * shifted during the flight. */
        this.log("info", "ControlConnection could not reconnect using existing connections.  Refreshing contact points and retrying");
        this._contactPoints.clear();
        this._resolvedContactPoints.clear();
        await Promise.all(this.options.contactPoints.map(name => this._parseContactPoint(name)));
        const refreshedContactPoints = Array.from(this._contactPoints).join(',');
        this.log('info', `Refreshed contact points: ${refreshedContactPoints}`);
        await this._initializeConnection();
      }
    }
  }

  /**
   * Acquires a new connection and refreshes topology and keyspace metadata.
   * <p>When it fails obtaining a connection and there aren't any more hosts, it schedules reconnection.</p>
   * <p>When it fails obtaining the metadata, it marks connection and/or host unusable and retries using the same
   * iterator from query plan / host list</p>
   * @param {Iterator<Host>} [hostIterator]
   */
  async _refresh(hostIterator) {
    if (this._isShuttingDown) {
      this.log('info', 'The ControlConnection will not be refreshed as the Client is being shutdown');
      return;
    }

    // Reset host and connection
    this.host = null;
    this.connection = null;

    try {
      if (!hostIterator) {
        this.log('info', 'Trying to acquire a connection to a new host');
        this._triedHosts = {};
        hostIterator = await promiseUtils.newQueryPlan(this._profileManager.getDefaultLoadBalancing(), null, null);
      }

      await this._refreshControlConnection(hostIterator);
    } catch (err) {
      // There was a failure obtaining a connection or during metadata retrieval
      this.log('error', 'ControlConnection failed to acquire a connection', err);

      if (!this._isShuttingDown) {
        const delay = this._reconnectionSchedule.next().value;
        this.log('warning', `ControlConnection could not reconnect, scheduling reconnection in ${delay}ms`);
        setTimeout(() => this._refresh(), delay);
        this.emit('newConnection', err);
      }

      return;
    }

    this.log('info',`ControlConnection connected to ${this.connection.endpointFriendlyName}`);

    try {
      await this._refreshHosts(false, true);

      await this._registerToConnectionEvents();
    } catch (err) {
      this.log('error', 'ControlConnection failed to retrieve topology and keyspaces information', err);
      this._triedHosts[this.connection.endpoint] = err;

      if (err.isSocketError && this.host) {
        this.host.removeFromPool(this.connection);
      }

      // Retry the whole thing with the same query plan
      return await this._refresh(hostIterator);
    }

    this._reconnectionSchedule = this._reconnectionPolicy.newSchedule();
    this._setHealthListeners(this.host, this.connection);
    this.emit('newConnection', null, this.connection, this.host);

    this.log('info', `ControlConnection connected to ${this.connection.endpointFriendlyName} and up to date`);
  }

  /**
   * Acquires a connection and refreshes topology and keyspace metadata for the first time.
   * @returns {Promise<void>}
   */
  async _initializeConnection() {
    this.log('info', 'Getting first connection');

    // Reset host and connection
    this.host = null;
    this.connection = null;
    this._triedHosts = {};

    // Randomize order of contact points resolved.
    const contactPointsIterator = utils.shuffleArray(Array.from(this._contactPoints))[Symbol.iterator]();

    while (true) {
      await this._borrowFirstConnection(contactPointsIterator);

      this.log('info', `ControlConnection using protocol version 0x${
        this.protocolVersion.toString(16)}, connected to ${this.connection.endpointFriendlyName}`);

      try {
        await this._getSupportedOptions();
        await this._refreshHosts(true, true);
        await this._registerToConnectionEvents();

        // We have a valid connection, leave the loop
        break;

      } catch (err) {
        this.log('error', 'ControlConnection failed to retrieve topology and keyspaces information', err);
        this._triedHosts[this.connection.endpoint] = err;
      }
    }

    // The healthy connection used to initialize should be part of the Host pool
    this.host.pool.addExistingConnection(this.connection);

    this.initialized = true;
    this._setHealthListeners(this.host, this.connection);
    this.log('info', `ControlConnection connected to ${this.connection.endpointFriendlyName}`);
  }

  async _getSupportedOptions() {
    const response = await this.connection.send(requests.options, null);

    // response.supported is a string multi map, decoded as an Object.
    const productType = response.supported && response.supported[supportedProductTypeKey];
    if (Array.isArray(productType) && productType[0] === supportedDbaas) {
      this.metadata.setProductTypeAsDbaas();
    }
  }

  async _registerToConnectionEvents() {
    this.connection.on('nodeTopologyChange', this._nodeTopologyChangeHandler.bind(this));
    this.connection.on('nodeStatusChange', this._nodeStatusChangeHandler.bind(this));
    this.connection.on('nodeSchemaChange', this._nodeSchemaChangeHandler.bind(this));
    const request = new requests.RegisterRequest(['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE']);
    await this.connection.send(request, null);
  }

  /**
   * Handles a TOPOLOGY_CHANGE event
   */
  _nodeTopologyChangeHandler(event) {
    this.log('info', 'Received topology change', event);

    // all hosts information needs to be refreshed as tokens might have changed
    clearTimeout(this._topologyChangeTimeout);

    // Use an additional timer to make sure that the refresh hosts is executed only AFTER the delay
    // In this case, the event debouncer doesn't help because it could not honor the sliding delay (ie: processNow)
    this._topologyChangeTimeout = setTimeout(() =>
      promiseUtils.toBackground(this._scheduleRefreshHosts()), newNodeDelay);
  }

  /**
   * Handles a STATUS_CHANGE event
   */
  _nodeStatusChangeHandler(event) {
    const self = this;
    const addressToTranslate = event.inet.address.toString();
    const port = this.options.protocolOptions.port;
    this._addressTranslator.translate(addressToTranslate, port, function translateCallback(endPoint) {
      const host = self.hosts.get(endPoint);
      if (!host) {
        self.log('warning', 'Received status change event but host was not found: ' + addressToTranslate);
        return;
      }
      const distance = self._profileManager.getDistance(host);
      if (event.up) {
        if (distance === types.distance.ignored) {
          return host.setUp(true);
        }
        clearTimeout(self._nodeStatusChangeTimeout);
        // Waits a couple of seconds before marking it as UP
        self._nodeStatusChangeTimeout = setTimeout(() => host.checkIsUp(), newNodeDelay);
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
  _nodeSchemaChangeHandler(event) {
    this.log('info', 'Schema change', event);
    if (!this.options.isMetadataSyncEnabled) {
      return;
    }

    promiseUtils.toBackground(this.handleSchemaChange(event, false));
  }

  /**
   * Schedules metadata refresh and callbacks when is refreshed.
   * @param {{keyspace: string, isKeyspace: boolean, schemaChangeType, table, udt, functionName, aggregate}} event
   * @param {Boolean} processNow
   * @returns {Promise<void>}
   */
  handleSchemaChange(event, processNow) {
    const self = this;
    let handler, cqlObject;

    if (event.isKeyspace) {
      if (event.schemaChangeType === schemaChangeTypes.dropped) {
        handler = function removeKeyspace() {
          // if on the same event queue there is a creation, this handler is not going to be executed
          // it is safe to remove the keyspace metadata
          delete self.metadata.keyspaces[event.keyspace];
        };

        return this._scheduleObjectRefresh(handler, event.keyspace, null, processNow);
      }

      return this._scheduleKeyspaceRefresh(event.keyspace, processNow);
    }

    const ksInfo = this.metadata.keyspaces[event.keyspace];
    if (!ksInfo) {
      // it hasn't been loaded and it is not part of the metadata, don't mind
      return Promise.resolve();
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

    if (!handler) {
      // Forward compatibility
      return Promise.resolve();
    }

    // It's a cql object change clean the internal cache
    return this._scheduleObjectRefresh(handler, event.keyspace, cqlObject, processNow);
  }

  /**
   * @param {Function} handler
   * @param {String} keyspace
   * @param {String} cqlObject
   * @param {Boolean} processNow
   * @returns {Promise<void>}
   */
  _scheduleObjectRefresh(handler, keyspace, cqlObject, processNow) {
    return this._debouncer.eventReceived({ handler, keyspace, cqlObject }, processNow);
  }

  /**
   * @param {String} keyspace
   * @param {Boolean} processNow
   * @returns {Promise<void>}
   */
  _scheduleKeyspaceRefresh(keyspace, processNow) {
    return this._debouncer.eventReceived({
      handler: () => this.metadata.refreshKeyspace(keyspace),
      keyspace
    }, processNow);
  }

  /** @returns {Promise<void>} */
  _scheduleRefreshHosts() {
    return this._debouncer.eventReceived({
      handler: () => this._refreshHosts(false, false),
      all: true
    }, false);
  }

  /**
   * Sets the information for the host used by the control connection.
   * @param {Boolean} initializing
   * @param {Connection} c
   * @param {Boolean} setCurrentHost Determines if the host retrieved must be set as the current host
   * @param result
   */
  _setLocalInfo(initializing, setCurrentHost, c, result) {
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
   * @param {ResultSet} result
   */
  async setPeersInfo(initializing, result) {
    if (!result || !result.rows) {
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
      const endpoint = await this.getAddressForPeerHost(row, port);

      if (!endpoint) {
        continue;
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
          this._profileManager.getDistance(host);

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
   * Gets the address from a peers row and translates the address.
   * @param {Object|Row} row
   * @param {Number} defaultPort
   * @returns {Promise<string>}
   */
  getAddressForPeerHost(row, defaultPort) {
    return new Promise(resolve => {
      let address = row['rpc_address'];
      const peer = row['peer'];
      const bindAllAddress = '0.0.0.0';

      if (!address) {
        this.log('error', f('No rpc_address found for host %s in %s\'s peers system table. %s will be ignored.',
          peer, this.host.address, peer));
        return resolve(null);
      }

      if (address.toString() === bindAllAddress) {
        this.log('warning', f('Found host with 0.0.0.0 as rpc_address, using listen_address (%s) to contact it instead.' +
          ' If this is incorrect you should avoid the use of 0.0.0.0 server side.', peer));
        address = peer;
      }

      this._addressTranslator.translate(address.toString(), defaultPort, resolve);
    });
  }

  /**
   * Uses the DNS protocol to resolve a IPv4 and IPv6 addresses (A and AAAA records) for the hostname.
   * It returns an Array of addresses that can be empty and logs the error.
   * @private
   * @param name
   */
  async _resolveAll(name) {
    const addresses = [];
    const resolve4 = util.promisify(dns.resolve4);
    const resolve6 = util.promisify(dns.resolve6);
    const lookup = util.promisify(dns.lookup);

    // Ignore errors for resolve calls
    const ipv4Promise = resolve4(name).catch(() => {}).then(r => r || utils.emptyArray);
    const ipv6Promise = resolve6(name).catch(() => {}).then(r => r || utils.emptyArray);

    let arr;
    arr = await ipv4Promise;
    arr.forEach(address => addresses.push({address, isIPv6: false}));

    arr = await ipv6Promise;
    arr.forEach(address => addresses.push({address, isIPv6: true}));

    if (addresses.length === 0) {
      // In case dns.resolve*() methods don't yield a valid address for the host name
      // Use system call getaddrinfo() that might resolve according to host system definitions
      try {
        arr = await lookup(name, { all: true });
        arr.forEach(({address, family}) => addresses.push({address, isIPv6: family === 6}));
      } catch (err) {
        this.log('error', `Host with name ${name} could not be resolved`, err);
      }
    }

    return addresses;
  }

  /**
   * Waits for a connection to be available. If timeout expires before getting a connection it callbacks in error.
   * @returns {Promise<void>}
   */
  _waitForReconnection() {
    return new Promise((resolve, reject) => {
      const callback = promiseUtils.getCallback(resolve, reject);

      // eslint-disable-next-line prefer-const
      let timeout;

      function newConnectionListener(err) {
        clearTimeout(timeout);
        callback(err);
      }

      this.once('newConnection', newConnectionListener);

      timeout = setTimeout(() => {
        this.removeListener('newConnection', newConnectionListener);
        callback(new errors.OperationTimedOutError('A connection could not be acquired before timeout.'));
      }, metadataQueryAbortTimeout);
    });
  }

  /**
   * Executes a query using the active connection
   * @param {String|Request} cqlQuery
   * @param {Boolean} [waitReconnect] Determines if it should wait for reconnection in case the control connection is not
   * connected at the moment. Default: true.
   */
  async query(cqlQuery, waitReconnect = true) {
    const queryOnConnection = async () => {
      if (!this.connection || this._isShuttingDown) {
        throw new errors.NoHostAvailableError({}, 'ControlConnection is not connected at the time');
      }

      const request = typeof cqlQuery === 'string' ? new requests.QueryRequest(cqlQuery, null, null) : cqlQuery;
      return await this.connection.send(request, null);
    };

    if (!this.connection && waitReconnect) {
      // Wait until its reconnected (or timer elapses)
      await this._waitForReconnection();
    }

    return await queryOnConnection();
  }

  /** @returns {Encoder} The encoder used by the current connection */
  getEncoder() {
    if (!this._encoder) {
      throw new errors.DriverInternalError('Encoder is not defined');
    }
    return this._encoder;
  }

  /**
   * Cancels all timers and shuts down synchronously.
   */
  shutdown() {
    this._isShuttingDown = true;
    this._debouncer.shutdown();
    // Emit a "newConnection" event with Error, as it may clear timeouts that were waiting new connections
    this.emit('newConnection', new errors.DriverError('ControlConnection is being shutdown'));
    // Cancel timers
    clearTimeout(this._topologyChangeTimeout);
    clearTimeout(this._nodeStatusChangeTimeout);
  }

  /**
   * Resets the Connection to its initial state.
   */
  async reset() {
    // Reset the internal state of the ControlConnection for future initialization attempts
    const currentHosts = this.hosts.clear();

    // Set the shutting down flag temporarily to avoid reconnects.
    this._isShuttingDown = true;

    // Shutdown all individual pools, ignoring any shutdown error
    await Promise.all(currentHosts.map(h => h.shutdown()));

    this.initialized = false;
    this._isShuttingDown = false;
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
