var util = require('util');
var events = require('events');
var async = require('async');

var Connection = require('./connection.js').Connection;
var utils = require('./utils.js');
/**
 * Represents a Cassandra node.
 * @constructor
 */
function Host(address) {
  this.isUp = true;
  this.address = address;
  this.unhealthyAt = 0;
}

/**
 * Sets this host as unavailable
 */
Host.prototype.setDown = function() {
  throw new Error("Not implemented");
};

Host.prototype.setUp = function () {
  throw new Error("Not implemented");
};

/**
 * Represents a pool of connections to a host
 * @constructor
 */
function HostConnectionPool(host, distance, protocolVersion, options) {
  events.EventEmitter.call(this);
  this.host = host;
  this.distance = distance;
  this.protocolVersion = protocolVersion;
  this.options = options;
  this.connections = null;
  this.setMaxListeners(0);
}

util.inherits(HostConnectionPool, events.EventEmitter);

HostConnectionPool.prototype.borrowConnection = function (callback) {
  var self = this;
  async.waterfall([
    this.maybeCreatePool.bind(this),
    //this.maybeSpawnNewConnection,
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
HostConnectionPool.prototype.maybeCreatePool = function (callback) {
  if (this.created) {
    return callback();
  }
  this.on('creation', callback);
  if (this.connections) {
    //Its being created so it will emit after it finished
    return;
  }
  this.connections = [];
  var self = this;
  async.times(this.options.poolOptions.coreConnections[this.distance], function (n, next) {
    var c = new Connection(self.options);
    self.connections.push(c);
    c.open(next);
  }, function (err) {
    self.created = true;
    self.emit('creation', err);
  });
};

HostConnectionPool.prototype.maybeSpawnNewConnection = function (inFlight) {
};

exports.HostConnectionPool = HostConnectionPool;
exports.Host = Host;
