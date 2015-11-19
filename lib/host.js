"use strict";
var util = require('util');
var events = require('events');
var async = require('async');

var Connection = require('./connection');
var utils = require('./utils');
var types = require('./types');
/**
 * Used to get the index of the connection with less in-flight requests
 * @type {number}
 * @private
 */
var connectionIndex = 0;
var connectionIndexOverflow = Math.pow(2, 15);
/**
 * Represents a Cassandra node.
 * @extends EventEmitter
 * @constructor
 */
function Host(address, protocolVersion, options) {
  events.EventEmitter.call(this);
  /**
   * Gets ip address and port number of the node separated by `:`.
   * @type {String}
   */
  this.address = address;
  this.unhealthyAt = 0;
  Object.defineProperty(this, 'options', { value: options, enumerable: false, writable: false});
  /**
   * @type {HostConnectionPool}
   */
  Object.defineProperty(this, 'pool', { value: new HostConnectionPool(this, protocolVersion), enumerable: false});
  this.pool.on('idleRequestError', this.setDown.bind(this));
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
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
}

util.inherits(Host, events.EventEmitter);

/**
 * Sets this host as unavailable
 * @internal
 * @ignore
 */
Host.prototype.setDown = function() {
  //the multiple connections and events signaling that a host is failing
  //could call #setDown() multiple times
  if (!this.canBeConsideredAsUp()) {
    //The host is already marked as Down
    return;
  }
  if (this.closing) {
    //This host is not down, the pool is being shutdown
    return;
  }
  this.reconnectionDelay = this.reconnectionSchedule.next().value;
  this.log('warning', util.format('Host %s considered as DOWN. Reconnection delay %dms.', this.address, this.reconnectionDelay));

  var wasUp = this.isUp();
  this.unhealthyAt = new Date().getTime();
  // Only emit a down event if was previously up.
  if(wasUp) {
    this.emit('down');
  }
  this.pool.scheduleReconnection(this.reconnectionDelay);
};

/**
 * Sets the host as available for querying
 * @internal
 * @ignore
 */
Host.prototype.setUp = function () {
  if (!this.unhealthyAt) {
    //The host is already marked as UP
    return;
  }
  this.log('info', 'Setting host ' + this.address + ' as UP');
  this.unhealthyAt = 0;
  //if it was unhealthy and now it is not, lets reset the reconnection schedule.
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
  this.pool.clearReconnection();
  this.emit('up');
};

/**
 * @param {Function} callback
 * @internal
 * @ignore
 */
Host.prototype.shutdown = function (callback) {
  this.closing = true;
  this.pool.shutdown(callback);
};

/**
 * Determines if the node is UP now (seen as UP by the driver).
 * @returns {boolean}
 */
Host.prototype.isUp = function () {
  return !this.unhealthyAt;
};

/**
 * Determines if the host can be considered as UP
 * @returns {boolean}
 */
Host.prototype.canBeConsideredAsUp = function () {
  var self = this;
  function hasTimePassed() {
    return new Date().getTime() - self.unhealthyAt >= self.reconnectionDelay;
  }
  return !this.unhealthyAt || hasTimePassed();
};

/**
 * Sets the distance of the host relative to the client.
 * It affects how the connection pool of the host nature (min/max size)
 * @param distance
 * @internal
 * @ignore
 */
Host.prototype.setDistance = function (distance) {
  distance = distance || types.distance.local;
  this.pool.coreConnectionsLength = this.options.pooling.coreConnectionsPerHost[distance];
  if (distance === types.distance.ignored && this.pool.connections.length > 0) {
    //this host was local/remote and now must be ignored
    //close all the connections to it
    this.pool.shutdown(noop);
  }
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
 * @param callback
 * @internal
 * @ignore
 */
Host.prototype.borrowConnection = function (callback) {
  this.pool.borrowConnection(callback);
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
  this.pool.checkHealth(connection);
};

/**
 * Returns an array containing the Cassandra Version as an Array of Numbers having the major version in the first
 * position.
 * @returns {Array.<Number>}
 */
Host.prototype.getCassandraVersion = function () {
  if (!this.cassandraVersion) return utils.emptyArray;
  return this.cassandraVersion.split('-')[0].split('.').map(function eachMap(x) {
    return parseInt(x, 10);
  });
};

Host.prototype.log = utils.log;

/**
 * Represents a pool of connections to a host
 * @param {Host} host
 * @param {Number} protocolVersion Initial protocol version
 * @constructor
 * @ignore
 */
function HostConnectionPool(host, protocolVersion) {
  events.EventEmitter.call(this);
  this.address = host.address;
  this.options = host.options;
  this.host = host;
  this.protocolVersion = protocolVersion;
  this.coreConnectionsLength = 1;
  //Use like an immutable array
  this.connections = utils.emptyArray;
  /**
   * Timeout instance for next reconnection attempt, if any.
   * @type {Object|null}
   */
  this.reconnectionTimeout = null;
  this.setMaxListeners(0);
}

util.inherits(HostConnectionPool, events.EventEmitter);

HostConnectionPool.prototype.borrowConnection = function (callback) {
  var c;
  var self = this;
  async.series([
    function createPoolIfNeeded(next) {
      self._maybeCreatePool(next)
    },
    function getLeastBusy(next) {
      if (self.connections.length === 0) {
        //something happen in the middle between the creation pool and now.
        var err = Error('No connection available');
        err.isServerUnhealthy = true;
        return next(err);
      }
      c = HostConnectionPool.minInFlight(self.connections);
      next();
    }
  ], function seriesEnd(err) {
    callback(err, c);
  });
};

/**
 * Gets the connection with the minimum number of in-flight requests.
 * Only checks for index + 1 and index, to avoid a loop through all the connections.
 * @param {Array.<Connection>} connections
 * @returns {Connection}
 */
HostConnectionPool.minInFlight = function(connections) {
  var length = connections.length;
  if (length === 1) {
    return connections[0];
  }
  var index = ++connectionIndex;
  if (connectionIndex >= connectionIndexOverflow) {
    connectionIndex = 0;
  }
  var current = connections[index % length];
  var previous = connections[(index - 1) % length];
  if (previous.getInFlight() < current.getInFlight()) {
    return previous;
  }
  return current;
};

/**
 * Create the min amount of connections, if the pool is empty
 */
HostConnectionPool.prototype._maybeCreatePool = function (callback) {
  //The value of this.coreConnectionsLength can change over time
  //when an existing pool is being resized (by setting the distance).
  if (this.connections.length >= this.coreConnectionsLength) {
    return callback();
  }
  if (this.creating) {
    if (this.connections.length > 0) {
      //we already have a valid connection
      //let the connection grow continue in the background
      return callback();
    }
    //subscribe and move on
    return this.once('creation', callback);
  }
  this.once('creation', callback);
  this.creating = true;
  //Use a copy
  var connections = this.connections.slice(0);
  var self = this;
  async.whilst(
    function condition() {
      return connections.length < self.coreConnectionsLength;
    },
    function iterator(next) {
      var c = self._createConnection();
      connections.push(c);
      c.open(function createOpenCallback(err) {
        if (err) {
          setImmediate(function createOpenError() {
            c.close();
          });
        }
        next(err);
      });
    }, function whilstEnded(err) {
      if (!err) {
        //set the new reference
        self.connections = connections;
      }
      self.creating = false;
      self.emit('creation', err);
  });
};

/** @returns {Connection} */
HostConnectionPool.prototype._createConnection = function () {
  var c = new Connection(this.address, this.protocolVersion, this.options);
  var self = this;
  //Relay the event idleRequestError
  c.on('idleRequestError', function idleErrorCallback(err) {
    //The pool will emit the event
    self.emit('idleRequestError', err);
  });
  return c;
};

HostConnectionPool.prototype.checkHealth = function (connection) {
  if (connection.timedOutHandlers > this.options.socketOptions.defunctReadTimeoutThreshold) {
    //defunct connection: locate in the connection pool
    //Locating an object by position in the array is a O(n), but normally there should be between 1 to 8 connections.
    var index = this.connections.indexOf(connection);
    if (index < 0) {
      //it was already removed from the connections and it's closing
      return;
    }
    //Remove the connection from the pool, using an pool copy
    var newConnections = this.connections.slice(0);
    newConnections.splice(index, 1);
    this.connections = newConnections;
    //close the connection
    setImmediate(function checkHealthClose() {
      connection.close(noop);
    });
  }
};

HostConnectionPool.prototype._forceShutdown = function () {
  //kills all the connections
  if (!this.connections.length) {
    return;
  }
  this.log('info', 'Closing force ' + this.connections.length + ' connections to ' + this.address);
  var activeConnections = this.connections;
  this.connections = utils.emptyArray;
  async.each(activeConnections, function forceShutdownEachCallback(c, next) {
    c.close(function forceShutdownCloseCallback() {
      //don't mind if there was an error
      next();
    });
  });
};

/**
 * Closes all open connections and schedules reconnection
 * @param {Number} delay
 */
HostConnectionPool.prototype.scheduleReconnection = function (delay) {
  this._forceShutdown();
  this.reconnectionTimeout = setTimeout(this._attemptReconnection.bind(this), delay);
};

/**
 * Prevents reconnection timeout from triggering
 */
HostConnectionPool.prototype.clearReconnection = function () {
  if (!this.reconnectionTimeout) {
    return;
  }
  clearTimeout(this.reconnectionTimeout);
  this.reconnectionTimeout = null;
};

HostConnectionPool.prototype._attemptReconnection = function () {
  this.reconnectionTimeout = null;
  var c = this._createConnection();
  var self = this;
  c.open(function attempOpenCallback(err) {
    if (err) {
      self.log('info', util.format('Reconnection attempt to host %s failed', self.address), err);
      self.host.setDown();
      return c.close();
    }
    self.host.setUp();
    self.connections = [ c ];
    setImmediate(function afterReconnectionSuccess() {
      self._maybeCreatePool(noop);
    });
  });
};

/**
 * @param {Function} callback
 * @returns {*}
 */
HostConnectionPool.prototype.shutdown = function (callback) {
  this.clearReconnection();
  if (!this.connections.length) {
    return callback();
  }
  if (this.shuttingDown) {
    return this.once('shutdown', callback);
  }
  this.log('info', 'Closing ' + this.connections.length + ' connections to ' + this.address);
  this.shuttingDown = true;
  var self = this;
  var connections = this.connections;
  //Create a new reference
  this.connections = utils.emptyArray;
  async.each(connections, function closeEach(c, next) {
    c.close(next);
  }, function shutdownEachEnded(err) {
    self.shuttingDown = false;
    callback(err);
    self.emit('shutdown', err);
  });
};

HostConnectionPool.prototype.log = utils.log;

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

HostMap.prototype.inspect = function() {
  return this._items;
};

HostMap.prototype.toJSON = function() {
  return this._items;
};

function noop () {}

exports.HostConnectionPool = HostConnectionPool;
exports.Host = Host;
exports.HostMap = HostMap;
