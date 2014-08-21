var util = require('util');
var events = require('events');
var async = require('async');

var Connection = require('./connection.js').Connection;
var utils = require('./utils.js');
/**
 * Represents a Cassandra node.
 * @constructor
 */
function Host(address, protocolVersion, options) {
  this.address = address;
  this.unhealthyAt = 0;
  this.options = options;
  this.pool = new HostConnectionPool(address, protocolVersion, options);
  this.datacenter = null;
  this.rack = null;
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
}

/**
 * Sets this host as unavailable
 */
Host.prototype.setDown = function() {
  this.unhealthyAt = new Date().getTime();
};

Host.prototype.setUp = function () {
  this.unhealthyAt = 0;
  this.reconnectionSchedule = this.options.policies.reconnection.newSchedule();
};

/**
 * Determines if the host can be considered as UP
 * @returns {boolean}
 */
Host.prototype.canBeConsideredAsUp = function () {
  var self = this;
  function hasTimePassed() {
    return new Date().getTime() - self.unhealthyAt > self.reconnectionSchedule.next().value;
  }
  return !this.unhealthyAt || hasTimePassed();
};

/**
 * Sets the distance of the host relative to the client.
 * It affects how the connection pool of the host nature (min/max size)
 * @param distance
 */
Host.prototype.setDistance = function (distance) {
  this.pool.coreConnectionsLength = this.options.poolOptions.coreConnectionsPerHost[distance];
  this.pool.maxConnectionsLength = this.options.poolOptions.maxConnectionsPerHost[distance];
};

/**
 * It gets an available open connection to the host
 * @param callback
 */
Host.prototype.borrowConnection = function (callback) {
  this.pool.borrowConnection(callback);
};
/**
 * Represents a pool of connections to a host
 * @constructor
 */
function HostConnectionPool(address, protocolVersion, options) {
  events.EventEmitter.call(this);
  this.address = address;
  this.protocolVersion = protocolVersion;
  this.options = options;
  this.connections = null;
  this.coreConnectionsLength = 1;
  this.maxConnectionsLength = 1;
  this.setMaxListeners(0);
}

util.inherits(HostConnectionPool, events.EventEmitter);

HostConnectionPool.prototype.borrowConnection = function (callback) {
  var self = this;
  async.waterfall([
    this._maybeCreatePool.bind(this),
    //TODO: this.maybeSpawnNewConnection,
    function getLeastBusy(next) {
      self.connections.sort(utils.propCompare('inFlight'));
      next(null, self.connections[0]);
    }
  ], function (err, results) {
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
  if (this.created && this.connections.length >= this.coreConnectionsLength) {
    return callback();
  }
  this.once('creation', callback);
  if (this.creating) {
    //Its being created so it will emit after it finished
    return;
  }
  this.creating = true;
  this.connections = [];
  var self = this;
  async.whilst(
    function condition() {
      return self.connections.length < self.coreConnectionsLength;
    },
    function iterator(next) {
      var c = new Connection(self.address, self.options);
      self.connections.push(c);
      c.open(next);
    }, function (err) {
      self.created = true;
      self.creating = false;
      self.emit('creation', err);
  });
};

HostConnectionPool.prototype._maybeSpawnNewConnection = function (inFlight) {
  throw new Error("not implemented");
};

/**
 * Represents an associative-array that can be iterated and its values cloned
 * @constructor
 */
function HostMap() {
  this.items = {};
}

/**
 * Adds a new item to the map
 */
HostMap.prototype.push = function (key, value) {
  this.length++;
  return this.items[key] = value;
};

HostMap.prototype.length = 0;

/**
 * Gets an item by key
 * @param [key]
 */
HostMap.prototype.get = function (key) {
  return this.items[key];
};

HostMap.prototype.forEach = function (callback) {
  for (var key in this.items) {
    if (!this.items.hasOwnProperty(key)) {
      continue;
    }
    callback(this.items[key]);
  }
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
