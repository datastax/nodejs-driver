"use strict";
var util = require('util');
var events = require('events');

var utils = require('./utils');
var types = require('./types');
var HostConnectionPool = require('./host-connection-pool');
var PrepareHandler = require('./prepare-handler');

/**
 * Creates a new Host instance.
 * @classdesc
 * Represents a Cassandra node.
 * @extends EventEmitter
 * @constructor
 */
function Host(address, protocolVersion, options, metadata) {
  events.EventEmitter.call(this);
  /**
   * Gets ip address and port number of the node separated by `:`.
   * @type {String}
   */
  this.address = address;
  this.setDownAt = 0;
  Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false});
  /**
   * @type {HostConnectionPool}
   */
  Object.defineProperty(this, 'pool', { value: new HostConnectionPool(this, protocolVersion), enumerable: false});
  var self = this;
  this.pool.on('open', this._onNewConnectionOpen.bind(this));
  this.pool.on('remove', function onConnectionRemovedFromPool() {
    self._checkPoolState();
  });
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
  // the distance as last set using the load balancing policy
  this._distance = types.distance.ignored;
  Object.defineProperty(this, '_metadata', { value: metadata, enumerable: false });
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
}

util.inherits(Host, events.EventEmitter);

/**
 * Marks this host as not available for query coordination.
 * @internal
 * @ignore
 */
Host.prototype.setDown = function() {
  // multiple events signaling that a host is failing could cause multiple calls to this method
  if (this.setDownAt !== 0) {
    // the host is already marked as Down
    return;
  }
  if (this.pool.isClosing()) {
    // the pool is being closed/shutdown, don't mind
    return;
  }
  this.setDownAt = Date.now();
  if (this._distance !== types.distance.ignored) {
    this.log('warning',
      util.format('Host %s considered as DOWN. Reconnection delay %dms.', this.address, this.reconnectionDelay));
  }
  else {
    this.log('info', util.format('Host %s considered as DOWN.', this.address));
  }
  this.emit('down');
  this._checkPoolState();
};

/**
 * Marks this host as available for querying.
 * @param {Boolean} [clearReconnection]
 * @internal
 * @ignore
 */
Host.prototype.setUp = function (clearReconnection) {
  if (!this.setDownAt) {
    //The host is already marked as UP
    return;
  }
  this.log('info', util.format('Setting host %s as UP', this.address));
  this.setDownAt = 0;
  //if it was unhealthy and now it is not, lets reset the reconnection schedule.
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
  if (clearReconnection) {
    this.pool.clearNewConnectionAttempt();
  }
  this.emit('up');
};

/**
 * Resets the reconnectionSchedule and tries to issue a reconnection immediately.
 * @internal
 * @ignore
 */
Host.prototype.checkIsUp = function () {
  if (this.isUp()) {
    return;
  }
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
  this.reconnectionDelay = 0;
  this.pool.attemptNewConnectionImmediate();
};

/**
 * @param {Boolean} waitForPending When true, it waits for in-flight operations to be finish before closing the
 * connections.
 * @param {Function} [callback]
 * @internal
 * @ignore
 */
Host.prototype.shutdown = function (waitForPending, callback) {
  callback = callback || utils.noop;
  if (waitForPending) {
    this.pool.drainAndShutdown();
    // Gracefully draining and shutting down the pool is being done in the background, it's not required
    // for the shutting down to be over to callback
    return callback();
  }
  this.pool.shutdown(callback);
};

/**
 * Determines if the node is UP now (seen as UP by the driver).
 * @returns {boolean}
 */
Host.prototype.isUp = function () {
  return !this.setDownAt;
};

/**
 * Determines if the host can be considered as UP
 * @returns {boolean}
 */
Host.prototype.canBeConsideredAsUp = function () {
  var self = this;
  function hasTimePassed() {
    return new Date().getTime() - self.setDownAt >= self.reconnectionDelay;
  }
  return !this.setDownAt || hasTimePassed();
};

/**
 * Sets the distance of the host relative to the client using the load balancing policy.
 * @param {Number} distance
 * @internal
 * @ignore
 */
Host.prototype.setDistance = function (distance) {
  var previousDistance = this._distance;
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
  if (this._distance === types.distance.ignored) {
    // this host was local/remote and now must be ignored
    this.emit('ignore');
    this.pool.drainAndShutdown();
  }
  else if (!this.isUp()) {
    this.checkIsUp();
  }
  return this._distance;
};

/**
 * Changes the protocol version of a given host
 * @param {Number} value
 * @internal
 * @ignore
 */
Host.prototype.setProtocolVersion = function (value) {
  this.pool.protocolVersion = value;
};

/**
 * It gets an open connection to the host.
 * If there isn't an available connections, it will open a new one according to the pooling options.
 * @param {Function} callback
 * @internal
 * @ignore
 */
Host.prototype.borrowConnection = function (callback) {
  this.pool.borrowConnection(callback);
};

/**
 * Creates all the connection in the pool.
 * @param {Function} callback
 * @internal
 * @ignore
 */
Host.prototype.warmupPool = function (callback) {
  this.pool.create(true, callback);
};

/**
 * Gets any connection that is already opened or null if not found.
 * @returns {Connection}
 * @internal
 * @ignore
 */
Host.prototype.getActiveConnection = function () {
  if (!this.isUp() || !this.pool.connections.length) {
    return null;
  }
  return this.pool.connections[0];
};

/**
 * Checks the health of a connection in the pool
 * @param {Connection} connection
 * @internal
 * @ignore
 */
Host.prototype.checkHealth = function (connection) {
  if (connection.timedOutOperations <= this.options.socketOptions.defunctReadTimeoutThreshold) {
    return;
  }
  this.removeFromPool(connection);
};

/**
 * @param {Connection} connection
 * @internal
 * @ignore
 */
Host.prototype.removeFromPool = function (connection) {
  this.pool.remove(connection);
  this._checkPoolState();
};

/**
 * Validates that the internal state of the connection pool.
 * If the pool size is smaller than expected, schedule a new connection attempt.
 * If the amount of connections is 0 for not ignored hosts, the host must be down.
 * @private
 */
Host.prototype._checkPoolState = function () {
  if (this.pool.isClosing()) {
    return;
  }
  if (this.pool.connections.length < this.pool.coreConnectionsLength) {
    // the pool still needs to grow
    if (!this.pool.hasScheduledNewConnection()) {
      this.reconnectionDelay = this.reconnectionSchedule.next().value;
      this.pool.scheduleNewConnectionAttempt(this.reconnectionDelay);
    }
  }
  if (this._distance !== types.distance.ignored &&
      this.pool.connections.length === 0 &&
      this.pool.coreConnectionsLength > 0) {
    this.setDown();
  }
};

/**
 * Executed after an scheduled new connection attempt finished
 * @private
 */
Host.prototype._onNewConnectionOpen = function (err) {
  if (err) {
    this._checkPoolState();
    return;
  }
  var self = this;
  function setUpAndContinue(err) {
    if (err) {
      self.log('warning', util.format('Failed re-preparing on host %s: %s', self.address, err), err);
    }
    self.setUp();
    self.pool.increaseSize();
  }
  if (this.isUp() || !this.options.rePrepareOnUp) {
    return setUpAndContinue();
  }
  this.log('info', util.format('Re-preparing all queries on host %s before setting it as UP', this.address));
  var allPrepared = this._metadata.getAllPrepared();
  PrepareHandler.prepareAllQueries(this, allPrepared, setUpAndContinue);
};

/**
 * Returns an array containing the Cassandra Version as an Array of Numbers having the major version in the first
 * position.
 * @returns {Array.<Number>}
 */
Host.prototype.getCassandraVersion = function () {
  if (!this.cassandraVersion) {
    return utils.emptyArray;
  }
  return this.cassandraVersion.split('-')[0].split('.').map(function eachMap(x) {
    return parseInt(x, 10);
  });
};

Host.prototype.log = utils.log;

/**
 * Represents an associative-array of {@link Host hosts} that can be iterated.
 * It creates an internal copy when adding or removing, making it safe to iterate using the values() method within async operations.
 * @extends events.EventEmitter
 * @constructor
 */
function HostMap() {
  events.EventEmitter.call(this);
  this._items = {};
  this._values = null;
  Object.defineProperty(this, 'length', { get: function () { return this.values().length; }, enumerable: true });
}

util.inherits(HostMap, events.EventEmitter);

/**
 * Emitted when a host is added to the map
 * @event HostMap#add
 */
/**
 * Emitted when a host is removed from the map
 * @event HostMap#remove
 */

/**
 * Executes a provided function once per map element.
 * @param callback
 */
HostMap.prototype.forEach = function (callback) {
  //Use a new reference, allowing the map to be modified.
  var items = this._items;
  for (var key in items) {
    if (!items.hasOwnProperty(key)) {
      continue;
    }
    callback(items[key], key);
  }
};

/**
 * Gets a {@link Host host} by key or undefined if not found.
 * @param {String} key
 * @returns {Host}
 */
HostMap.prototype.get = function (key) {
  return this._items[key];
};

/**
 * Returns an array of host addresses.
 * @returns {Array.<String>}
 */
HostMap.prototype.keys = function () {
  return Object.keys(this._items);
};

/**
 * Removes an item from the map.
 * @param {String} key The key of the host
 * @fires HostMap#remove
 */
HostMap.prototype.remove = function (key) {
  if (!this._items.hasOwnProperty(key)) {
    //it's not part of it, do nothing
    return;
  }
  //clear cache
  this._values = null;
  //copy the values
  var copy = utils.extend({}, this._items);
  var h = copy[key];
  delete copy[key];
  this._items = copy;
  this.emit('remove', h);
};

/**
 * Removes multiple hosts from the map.
 * @param {Array.<String>} keys
 * @fires HostMap#remove
 */
HostMap.prototype.removeMultiple = function (keys) {
  //clear value cache
  this._values = null;
  //copy the values
  var copy = utils.extend({}, this._items);
  var removedHosts = [];
  for (var i = 0; i < keys.length; i++) {
    var h = copy[keys[i]];
    if (!h) {
      continue;
    }
    removedHosts.push(h);
    delete copy[keys[i]];
  }
  this._items = copy;
  removedHosts.forEach(function (h) {
    this.emit('remove', h);
  }, this);
};

/**
 * Adds a new item to the map.
 * @param {String} key The key of the host
 * @param {Host} value The host to be added
 * @fires HostMap#remove
 * @fires HostMap#add
 */
HostMap.prototype.set = function (key, value) {
  //clear values cache
  this._values = null;
  var originalValue = this._items[key];
  if (originalValue) {
    //The internal structure does not change
    this._items[key] = value;
    //emit a remove followed by a add
    this.emit('remove', originalValue);
    this.emit('add', value);
    return;
  }
  //copy the values
  var copy = utils.extend({}, this._items);
  copy[key] = value;
  this._items = copy;
  this.emit('add', value);
  return value;
};

/**
 * Returns a shallow copy of a portion of the items into a new array object.
 * Backward-compatibility.
 * @param {Number} [begin]
 * @param {Number} [end]
 * @returns {Array}
 * @ignore
 */
HostMap.prototype.slice = function (begin, end) {
  if (!begin && !end) {
    //avoid making a copy of the copy
    return this.values();
  }
  begin = begin || 0;
  return this.values().slice(begin, end);
};
//Backward-compatibility
HostMap.prototype.push = HostMap.prototype.set;

/**
 * Returns a shallow copy of the values of the map.
 * @returns {Array.<Host>}
 */
HostMap.prototype.values = function () {
  if (!this._values) {
    //cache the values
    var values = [];
    for (var key in this._items) {
      if (!this._items.hasOwnProperty(key)) {
        continue;
      }
      values.push(this._items[key]);
    }
    this._values = Object.freeze(values);
  }
  return this._values;
};

/**
 * Removes all items from the map.
 * @returns {Array.<Host>} The previous items
 */
HostMap.prototype.clear = function () {
  var previousItems = this.values();
  // Clear cache
  this._values = null;
  // Clear items
  this._items = {};
  for (var i = 0; i < previousItems.length; i++) {
    this.emit('remove', previousItems[i]);
  }
  return previousItems;
};

HostMap.prototype.inspect = function() {
  return this._items;
};

HostMap.prototype.toJSON = function() {
  return this._items;
};

exports.Host = Host;
exports.HostMap = HostMap;
