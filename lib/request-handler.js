var util = require('util');
var async = require('async');

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
RequestHandler.prototype.getNextConnection = function (callback) {
  var self = this;
  var hostIterator = self.loadBalancingPolicy.newQueryPlan(function (err, iterator) {
    var item;
    while (item = iterator.next() && !item.done) {
      var host = item.value;
      if (!host.canBeConsideredAsUp()) {
        continue;
      }
      //TODO all the rest

      //TODO: Set the keyspace to the connection
    }
  });
};

/**
 * Gets an available connection and sends the request
 */
RequestHandler.prototype.send = function (request, options, callback) {
  getNextConnection(function (err, c) {
    //c.sendStream()
    throw new Error("Not implemented");
  });
};