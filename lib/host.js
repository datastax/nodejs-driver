var util = require('util');
var events = require('events');
var async = require('async');

var Connection = require('./connection.js');
var utils = require('./utils.js');

/**
 * Represents a Cassandra node.
 * @constructor
 */
function Host(name, reconnectionPolicy) {
  this.isUp = true;
  this.name = name;
  this.unhealthyAt = 0;
  this.schedule = reconnectionPolicy.newSchedule();
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

HostConnectionPool.prototype.borrowConnection = function (keyspace, callback) {
  async.series([
    this.maybeCreatePool,
    function getLeastBusy(next) {
      this.connections.sort(utils.propCompare('inFlight'));
    }
  ], callback);
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
    var c = new Connection(options);
    self.connections.push(c);
    c.open(next);
  }, function (err) {
    self.created = true;
    self.emit('creation', callback);
  });
};

HostConnectionPool.prototype.maybeSpawnNewConnection = function (inFlight) {
};

exports.Host = Host;
exports.HostConnectionPool = HostConnectionPool;