var util = require('util');
var async = require('async');
var errors = require('./errors.js');

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 */
function RequestHandler(options)
{
  this.loadBalancingPolicy = options.policies.loadBalancingPolicy;
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {function} callback
 */
RequestHandler.prototype._getNextConnection = function (callback) {
  var self = this;
  //TODO: Store tried hosts
  var hostIterator = self.loadBalancingPolicy.newQueryPlan(function (err, iterator) {
    var item;
    while (item = iterator.next() && !item.done) {
      var host = item.value;
      if (!host.canBeConsideredAsUp()) {
        continue;
      }
      //TODO: Get host pool
      //TODO: Get a connection from the pool

      //TODO: Set the keyspace to the connection
    }
  });
};

/**
 * Gets an available connection and sends the request
 */
RequestHandler.prototype.send = function (request, options, callback) {
  this._getNextConnection(function (err, c) {
    //c.sendStream()
    throw new Error("Not implemented");
  });
};

/**
 * Gets an open connection to using the provided hosts Array, without using the load balancing policy.
 * Invoked before the Client can access topology of the cluster.
 * @param {HostMap} hostMap
 * @param {Function} callback
 */
RequestHandler.prototype.getFirstConnection = function (hostMap, callback) {
  var connection = null;
  var index = 0;
  var openingErrors = [];
  var hosts = hostMap.slice(0);
  async.doUntil(function iterator(next) {
    var h = hosts[index];
    h.borrowConnection(function (err, c) {
      if (err) {
        openingErrors.push(err);
      }
      else {
        connection = c;
      }
      next();
    });
  }, function condition () {
    return connection || ++index > hosts.length;
  }, function done(err) {
    if (connection == null) {
      err = new errors.NoHostAvailableError(openingErrors);
    }
    callback(err, connection);
  });
};

module.exports = RequestHandler;