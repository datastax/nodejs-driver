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

const utils = require('./utils');
const types = require('./types');
const HostConnectionPool = require('./host-connection-pool');
const PrepareHandler = require('./prepare-handler');
const promiseUtils = require('./promise-utils');

const healthResponseCountInterval = 200;

/**
 * Represents a Cassandra node.
 * @extends EventEmitter
 */
class Host extends events.EventEmitter {

  /**
   * Creates a new Host instance.
   */
  constructor(address, protocolVersion, options, metadata) {
    super();
    /**
     * Gets ip address and port number of the node separated by `:`.
     * @type {String}
     */
    this.address = address;
    this.setDownAt = 0;
    this.log = utils.log;

    /**
     * Gets the timestamp of the moment when the Host was marked as UP.
     * @type {Number|null}
     * @ignore
     * @internal
     */
    this.isUpSince = null;
    Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false });

    /**
     * The host pool.
     * @internal
     * @ignore
     * @type {HostConnectionPool}
     */
    Object.defineProperty(this, 'pool', { value: new HostConnectionPool(this, protocolVersion), enumerable: false });

    this.pool.on('open', err => promiseUtils.toBackground(this._onNewConnectionOpen(err)));
    this.pool.on('remove', () => this._checkPoolState());

    /**
     * Gets string containing the Cassandra version.
     * @type {String}
     */
    this.cassandraVersion = null;

    /**
     * Gets data center name of the node.
     * @type {String}
     */
    this.datacenter = null;

    /**
     * Gets rack name of the node.
     * @type {String}
     */
    this.rack = null;

    /**
     * Gets the tokens assigned to the node.
     * @type {Array}
     */
    this.tokens = null;

    /**
     * Gets the id of the host.
     * <p>This identifier is used by the server for internal communication / gossip.</p>
     * @type {Uuid}
     */
    this.hostId = null;

    /**
     * Gets string containing the DSE version or null if not set.
     * @type {String}
     */
    this.dseVersion = null;

    /**
     * Gets the DSE Workloads the host is running.
     * <p>
     *   This is based on the "workload" or "workloads" columns in {@code system.local} and {@code system.peers}.
     * <p/>
     * <p>
     *   Workload labels may vary depending on the DSE version in use;e.g. DSE 5.1 may report two distinct workloads:
     *   <code>Search</code> and <code>Analytics</code>, while DSE 5.0 would report a single
     *   <code>SearchAnalytics</code> workload instead. The driver simply returns the workload labels as reported by
     *   DSE, without any form of pre-processing.
     * <p/>
     * <p>When the information is unavailable, this property returns an empty array.</p>
     * @type {Array<string>}
     */
    this.workloads = utils.emptyArray;

    // the distance as last set using the load balancing policy
    this._distance = types.distance.ignored;
    this._healthResponseCounter = 0;

    // Make some of the private instance variables not enumerable to prevent from showing when inspecting
    Object.defineProperty(this, '_metadata', { value: metadata, enumerable: false });
    Object.defineProperty(this, '_healthResponseCountTimer', { value: null, enumerable: false, writable: true });

    this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
    this.reconnectionDelay = 0;
  }

  /**
   * Marks this host as not available for query coordination, when the host was previously marked as UP, otherwise its
   * a no-op.
   * @internal
   * @ignore
   */
  setDown() {
    // Multiple events signaling that a host is failing could cause multiple calls to this method
    if (this.setDownAt !== 0) {
      // the host is already marked as Down
      return;
    }
    if (this.pool.isClosing()) {
      // the pool is being closed/shutdown, don't mind
      return;
    }
    this.setDownAt = Date.now();
    if (this.pool.coreConnectionsLength > 0) {
      // According to the distance, there should be connections open to it => issue a warning
      this.log('warning', `Host ${this.address} considered as DOWN. Reconnection delay ${this.reconnectionDelay}ms.`);
    }
    else {
      this.log('info', `Host ${this.address} considered as DOWN.`);
    }
    this.emit('down');
    this._checkPoolState();
  }

  /**
   * Marks this host as available for querying.
   * @param {Boolean} [clearReconnection]
   * @internal
   * @ignore
   */
  setUp(clearReconnection) {
    if (!this.setDownAt) {
      //The host is already marked as UP
      return;
    }
    this.log('info', `Setting host ${this.address} as UP`);
    this.setDownAt = 0;
    this.isUpSince = Date.now();
    //if it was unhealthy and now it is not, lets reset the reconnection schedule.
    this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
    if (clearReconnection) {
      this.pool.clearNewConnectionAttempt();
    }
    this.emit('up');
  }

  /**
   * Resets the reconnectionSchedule and tries to issue a reconnection immediately.
   * @internal
   * @ignore
   */
  checkIsUp() {
    if (this.isUp()) {
      return;
    }
    this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
    this.reconnectionDelay = 0;
    this.pool.attemptNewConnectionImmediate();
  }

  /**
   * @param {Boolean} [waitForPending] When true, it waits for in-flight operations to be finish before closing the
   * connections.
   * @returns {Promise<void>}
   * @internal
   * @ignore
   */
  shutdown(waitForPending) {
    if (this._healthResponseCountTimer) {
      clearInterval(this._healthResponseCountTimer);
    }
    if (waitForPending) {
      this.pool.drainAndShutdown();
      // Gracefully draining and shutting down the pool is being done in the background
      return Promise.resolve();
    }
    return this.pool.shutdown();
  }

  /**
   * Determines if the node is UP now (seen as UP by the driver).
   * @returns {boolean}
   */
  isUp() {
    return !this.setDownAt;
  }

  /**
   * Determines if the host can be considered as UP.
   * Deprecated: Use {@link Host#isUp()} instead.
   * @returns {boolean}
   */
  canBeConsideredAsUp() {
    const self = this;
    function hasTimePassed() {
      return new Date().getTime() - self.setDownAt >= self.reconnectionDelay;
    }
    return !this.setDownAt || hasTimePassed();
  }

  /**
   * Sets the distance of the host relative to the client using the load balancing policy.
   * @param {Number} distance
   * @internal
   * @ignore
   */
  setDistance(distance) {
    const previousDistance = this._distance;
    this._distance = distance || types.distance.local;
    if (this.options.pooling.coreConnectionsPerHost) {
      this.pool.coreConnectionsLength = this.options.pooling.coreConnectionsPerHost[this._distance] || 0;
    }
    else {
      this.pool.coreConnectionsLength = 1;
    }
    if (this._distance === previousDistance) {
      return this._distance;
    }
    if (this._healthResponseCountTimer) {
      clearInterval(this._healthResponseCountTimer);
    }
    if (this._distance === types.distance.ignored) {
      // this host was local/remote and now must be ignored
      this.emit('ignore');
      this.pool.drainAndShutdown();
    }
    else {
      if (!this.isUp()) {
        this.checkIsUp();
      }
      // Reset the health check timer
      this._healthResponseCountTimer = setInterval(() => {
        this._healthResponseCounter = this.pool.getAndResetResponseCounter();
      }, healthResponseCountInterval);
    }
    return this._distance;
  }

  /**
   * Changes the protocol version of a given host
   * @param {Number} value
   * @internal
   * @ignore
   */
  setProtocolVersion(value) {
    this.pool.protocolVersion = value;
  }

  /**
   * Gets the least busy connection from the pool.
   * @param {Connection} [previousConnection] When provided, the pool should attempt to obtain a different connection.
   * @returns {Connection!}
   * @throws {Error}
   * @throws {BusyConnectionError}
   * @internal
   * @ignore
   */
  borrowConnection(previousConnection) {
    return this.pool.borrowConnection(previousConnection);
  }

  /**
   * Creates all the connection in the pool.
   * @param {string} keyspace
   * @internal
   * @ignore
   */
  warmupPool(keyspace) {
    return this.pool.warmup(keyspace);
  }

  /**
   * Starts creating the pool in the background.
   * @internal
   * @ignore
   */
  initializePool() {
    this.pool.increaseSize();
  }
  /**
   * Gets any connection that is already opened or null if not found.
   * @returns {Connection}
   * @internal
   * @ignore
   */
  getActiveConnection() {
    if (!this.isUp() || !this.pool.connections.length) {
      return null;
    }
    return this.pool.connections[0];
  }

  /**
   * Internal method to get the amount of responses dequeued in the last interval (between 200ms and 400ms) on all
   * connections to the host.
   * @returns {Number}
   * @internal
   * @ignore
   */
  getResponseCount() {
    // Last interval plus the current count
    return this._healthResponseCounter + this.pool.responseCounter;
  }

  /**
   * Checks the health of a connection in the pool
   * @param {Connection} connection
   * @internal
   * @ignore
   */
  checkHealth(connection) {
    if (connection.timedOutOperations <= this.options.socketOptions.defunctReadTimeoutThreshold) {
      return;
    }
    this.removeFromPool(connection);
  }

  /**
   * @param {Connection} connection
   * @internal
   * @ignore
   */
  removeFromPool(connection) {
    this.pool.remove(connection);
    this._checkPoolState();
  }

  /**
   * Internal method that gets the amount of in-flight requests on all connections to the host.
   * @internal
   * @ignore
   */
  getInFlight() {
    return this.pool.getInFlight();
  }

  /**
   * Validates that the internal state of the connection pool.
   * If the pool size is smaller than expected, schedule a new connection attempt.
   * If the amount of connections is 0 for not ignored hosts, the host must be down.
   * @private
   */
  _checkPoolState() {
    if (this.pool.isClosing()) {
      return;
    }
    if (this.pool.connections.length < this.pool.coreConnectionsLength) {
      // the pool needs to grow / reconnect
      if (!this.pool.hasScheduledNewConnection()) {
        this.reconnectionDelay = this.reconnectionSchedule.next().value;
        this.pool.scheduleNewConnectionAttempt(this.reconnectionDelay);
      }
    }
    const shouldHaveConnections = this._distance !== types.distance.ignored && this.pool.coreConnectionsLength > 0;
    if (shouldHaveConnections && this.pool.connections.length === 0) {
      // Mark as DOWN, if its UP
      this.setDown();
    }
  }

  /**
   * Executed after an scheduled new connection attempt finished
   * @private
   */
  async _onNewConnectionOpen(err) {
    if (err) {
      this._checkPoolState();
      return;
    }
    if (!this.isUp() && this.options.rePrepareOnUp) {
      this.log('info', `Re-preparing all queries on host ${this.address} before setting it as UP`);
      const allPrepared = this._metadata.getAllPrepared();
      try {
        await PrepareHandler.prepareAllQueries(this, allPrepared);
      }
      catch (err) {
        this.log('warning', `Failed re-preparing on host ${this.address}: ${err}`, err);
      }
    }
    this.setUp();
    this.pool.increaseSize();
  }

  /**
   * Returns an array containing the Cassandra Version as an Array of Numbers having the major version in the first
   * position.
   * @returns {Array.<Number>}
   */
  getCassandraVersion() {
    if (!this.cassandraVersion) {
      return utils.emptyArray;
    }
    return this.cassandraVersion.split('-')[0].split('.').map(x => parseInt(x, 10));
  }

  /**
   * Gets the DSE version of the host as an Array, containing the major version in the first position.
   * In case the cluster is not a DSE cluster, it returns an empty Array.
   * @returns {Array}
   */
  getDseVersion() {
    if (!this.dseVersion) {
      return utils.emptyArray;
    }
    return this.dseVersion.split('-')[0].split('.').map(x => parseInt(x, 10));
  }
}

/**
 * Represents an associative-array of {@link Host hosts} that can be iterated.
 * It creates an internal copy when adding or removing, making it safe to iterate using the values()
 * method within async operations.
 * @extends events.EventEmitter
 * @constructor
 */
class HostMap extends events.EventEmitter{
  constructor() {
    super();

    this._items = new Map();
    this._values = null;

    Object.defineProperty(this, 'length', { get: () => this.values().length, enumerable: true });

    /**
     * Emitted when a host is added to the map
     * @event HostMap#add
     */
    /**
     * Emitted when a host is removed from the map
     * @event HostMap#remove
     */
  }

  /**
   * Executes a provided function once per map element.
   * @param callback
   */
  forEach(callback) {
    const items = this._items;
    for (const [ key, value ] of items) {
      callback(value, key);
    }
  }

  /**
   * Gets a {@link Host host} by key or undefined if not found.
   * @param {String} key
   * @returns {Host}
   */
  get(key) {
    return this._items.get(key);
  }

  /**
   * Returns an array of host addresses.
   * @returns {Array.<String>}
   */
  keys() {
    return Array.from(this._items.keys());
  }

  /**
   * Removes an item from the map.
   * @param {String} key The key of the host
   * @fires HostMap#remove
   */
  remove(key) {
    const value = this._items.get(key);
    if (value === undefined) {
      return;
    }

    // Clear cache
    this._values = null;

    // Copy the values
    const copy = new Map(this._items);
    copy.delete(key);

    this._items = copy;
    this.emit('remove', value);
  }

  /**
   * Removes multiple hosts from the map.
   * @param {Array.<String>} keys
   * @fires HostMap#remove
   */
  removeMultiple(keys) {
    // Clear value cache
    this._values = null;

    // Copy the values
    const copy = new Map(this._items);
    const removedHosts = [];

    for (const key of keys) {
      const h = copy.get(key);

      if (!h) {
        continue;
      }

      removedHosts.push(h);
      copy.delete(key);
    }

    this._items = copy;
    removedHosts.forEach(h => this.emit('remove', h));
  }

  /**
   * Adds a new item to the map.
   * @param {String} key The key of the host
   * @param {Host} value The host to be added
   * @fires HostMap#remove
   * @fires HostMap#add
   */
  set(key, value) {
    // Clear values cache
    this._values = null;

    const originalValue = this._items.get(key);
    if (originalValue) {
      //The internal structure does not change
      this._items.set(key, value);
      //emit a remove followed by a add
      this.emit('remove', originalValue);
      this.emit('add', value);
      return;
    }

    // Copy the values
    const copy = new Map(this._items);
    copy.set(key, value);
    this._items = copy;
    this.emit('add', value);
    return value;
  }

  /**
   * Returns a shallow copy of a portion of the items into a new array object.
   * Backward-compatibility.
   * @param {Number} [begin]
   * @param {Number} [end]
   * @returns {Array}
   * @ignore
   */
  slice(begin, end) {
    if (!begin && !end) {
      // Avoid making a copy of the copy
      return this.values();
    }

    return this.values().slice(begin || 0, end);
  }

  /**
   * Deprecated: Use set() instead.
   * @ignore
   * @deprecated
   */
  push(k, v) {
    this.set(k, v);
  }

  /**
   * Returns a shallow copy of the values of the map.
   * @returns {Array.<Host>}
   */
  values() {
    if (!this._values) {
      // Cache the values
      this._values = Object.freeze(Array.from(this._items.values()));
    }

    return this._values;
  }

  /**
   * Removes all items from the map.
   * @returns {Array.<Host>} The previous items
   */
  clear() {
    const previousItems = this.values();

    // Clear cache
    this._values = null;

    // Clear items
    this._items = new Map();

    // Emit events
    previousItems.forEach(h => this.emit('remove', h));

    return previousItems;
  }

  inspect() {
    return this._items;
  }

  toJSON() {
    // Node.js 10 and below don't support Object.fromEntries()
    if (Object.fromEntries) {
      return Object.fromEntries(this._items);
    }

    const obj = {};
    for (const [ key, value ] of this._items) {
      obj[key] = value;
    }

    return obj;
  }
}

module.exports = {
  Host,
  HostMap
};