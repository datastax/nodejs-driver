var util = require('util');
var events = require('events');
var async = require('async');

var Connection = require('./connection.js');
var utils = require('./utils.js');
var types = require('./types');
/**
 * Represents a Cassandra node.
 * @constructor
 */
function Host(address, protocolVersion, options) {
  events.EventEmitter.call(this);
  this.address = address;
  this.unhealthyAt = 0;
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  this.pool = new HostConnectionPool(address, protocolVersion, options);
  this.pool.on('idleRequestError', this.setDown.bind(this));
  this.datacenter = null;
  this.rack = null;
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
}

util.inherits(Host, events.EventEmitter);

/**
 * Sets this host as unavailable
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
  this.log('warning', 'Setting host ' + this.address + ' as DOWN');
  this.unhealthyAt = new Date().getTime();
  this.reconnectionDelay = this.reconnectionSchedule.next().value;
  this.emit('down');
  this.pool.forceShutdown();
};

Host.prototype.setUp = function () {
  if (this.unhealthyAt) {
    this.log('info', 'Setting host ' + this.address + ' as UP');
    this.unhealthyAt = 0;
    //if it was unhealthy and now it is not, lets reset the reconnection schedule.
    this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
  }
};

Host.prototype.shutdown = function (callback) {
  this.closing = true;
  this.pool.shutdown(callback);
};

/**
 * Determines if the node is UP now (seen as UP by the driver).
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
    return new Date().getTime() - self.unhealthyAt > self.reconnectionDelay;
  }
  return !this.unhealthyAt || hasTimePassed();
};

/**
 * Sets the distance of the host relative to the client.
 * It affects how the connection pool of the host nature (min/max size)
 * @param distance
 */
Host.prototype.setDistance = function (distance) {
  distance = distance || types.distance.local;
  this.pool.coreConnectionsLength = this.options.pooling.coreConnectionsPerHost[distance];
  this.pool.maxConnectionsLength = this.pool.coreConnectionsLength;
  if (this.options.pooling.maxConnectionsPerHost) {
    this.pool.maxConnectionsLength = this.options.pooling.maxConnectionsPerHost[distance];
  }
};

/**
 * Changes the protocol version of a given host
 * @param {Number} value
 */
Host.prototype.setProtocolVersion = function (value) {
  this.pool.protocolVersion = value;
};

/**
 * It gets an available open connection to the host
 * @param callback
 */
Host.prototype.borrowConnection = function (callback) {
  this.pool.borrowConnection(callback);
};

Host.prototype.log = utils.log;

/**
 * Represents a pool of connections to a host
 * @constructor
 */
function HostConnectionPool(address, protocolVersion, options) {
  events.EventEmitter.call(this);
  this.address = address;
  this.protocolVersion = protocolVersion;
  Object.defineProperty(this, "options", { value: options, enumerable: false, writable: false});
  Object.defineProperty(this, "connections", { value: null, enumerable: false, writable: true});
  this.coreConnectionsLength = 1;
  this.maxConnectionsLength = 1;
  this.setMaxListeners(0);
}

util.inherits(HostConnectionPool, events.EventEmitter);

HostConnectionPool.prototype.borrowConnection = function (callback) {
  var self = this;
  async.waterfall([
    self._maybeCreatePool.bind(self),
    function getLeastBusy(next) {
      if (!self.connections || self.connections.length === 0) {
        //something happen in the middle between the creation pool and now.
        var err = Error('No connection available');
        err.isServerUnhealthy = true;
        return next(err);
      }
      self.connections.sort(utils.funcCompare('getInFlight'));
      next(null, self.connections[0]);
    }
  ], function waterfallEnd(err, results) {
    if (util.isArray(results)) {
      results = results[0];
    }
    callback(err, results);
  });
};

/**
 * Create the min amount of connections, if the pool is empty
 */
HostConnectionPool.prototype._maybeCreatePool = function (callback) {
  //The parameter this.coreConnectionsLength could change over time
  //It can result of a created pool being resized (setting the distance).
  if (!this.creating && this.connections && this.connections.length >= this.coreConnectionsLength) {
    return callback();
  }
  this.once('creation', callback);
  if (this.creating) {
    //Its being created so it will emit after it finished
    return;
  }
  this.creating = true;
  if (!this.connections) {
    this.connections = [];
  }
  var self = this;
  async.whilst(
    function condition() {
      return self.connections.length < self.coreConnectionsLength;
    },
    function iterator(next) {
      var c = new Connection(self.address, self.protocolVersion, self.options);
      //Relay the event idleRequestError
      c.on('idleRequestError', function (err) {
        //The pool will emit the event
        self.emit('idleRequestError', err);
      });
      self.connections.push(c);
      c.open(next);
    }, function (err) {
      if (err) {
        //reset the state
        self.connections = null;
      }
      self.creating = false;
      self.emit('creation', err);
  });
};

HostConnectionPool.prototype.forceShutdown = function () {
  //kills all the connections
  if (!this.connections) {
    return;
  }
  this.log('info', 'Closing force ' + this.connections.length + ' connections to ' + this.address);
  var activeConnections = this.connections;
  this.connections = null;
  async.each(activeConnections, function (c, next) {
    c.close(function () {
      //don't mind if there was an error
      next();
    });
  });
};

HostConnectionPool.prototype.shutdown = function (callback) {
  if (!this.connections) {
    return callback();
  }
  if (this.shuttingDown) {
    return this.once('shutdown', callback);
  }
  this.log('info', 'Closing ' + this.connections.length + ' connections to ' + this.address);
  this.shuttingDown = true;
  var self = this;
  async.each(this.connections, function (c, next) {
    c.close(next);
  }, function (err) {
    self.connections = null;
    self.shuttingDown = false;
    callback(err);
    self.emit('shutdown', err);
  });
};

HostConnectionPool.prototype.log = utils.log;

/**
 * Represents an associative-array that can be iterated and cloned
 * @constructor
 */
function HostMap() {
  this.items = {};
}

/**
 * Adds a new item to the map
 */
HostMap.prototype.push = function (key, value) {
  //if it wasn't already added
  this.length += (!this.items.hasOwnProperty(key))? 1 : 0;
  return this.items[key] = value;
};

/**
 * Removes an item from the map
 */
HostMap.prototype.remove = function (key) {
  if (!this.items.hasOwnProperty(key)) {
    //it's not part of it, do nothing
    return;
  }
  this.length--;
  delete this.items[key];
};


HostMap.prototype.length = 0;

/**
 * Gets an item by key
 * @param {String} key
 */
HostMap.prototype.get = function (key) {
  return this.items[key];
};

HostMap.prototype.forEach = function (callback) {
  for (var key in this.items) {
    if (!this.items.hasOwnProperty(key)) {
      continue;
    }
    callback(this.items[key], key);
  }
};

/**
 * Returns an array of keys
 * @returns {Array}
 */
HostMap.prototype.keys = function () {
  return Object.keys(this.items);
};

/**
 * Returns a shallow copy of a portion of the items into a new array object.
 * @param {Number} [begin]
 * @param {Number} [end]
 * @returns {Array}
 */
HostMap.prototype.slice = function (begin, end) {
  begin = begin || 0;
  end = end || Number.MAX_VALUE;
  var result = [];
  var index = 0;
  for (var key in this.items) {
    if (!this.items.hasOwnProperty(key)) {
      continue;
    }
    if (index >= begin && index < end) {
      result.push(this.items[key]);
    }
    index++;
    if (index >= end) {
      break;
    }
  }
  return result;
};

exports.HostConnectionPool = HostConnectionPool;
exports.Host = Host;
exports.HostMap = HostMap;
