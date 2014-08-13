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
  async.waterfall([
    function getHosts(next) {
      //TODO
      var hosts = null;
      next(null, hosts);
    },
    function iterateHosts(hosts, next) {
      //TODO
      var connection = null;
      next(null, connection);
    },
    function setKeyspace(connection, next) {
      //TODO: Set the keyspace to the connection
      next(null, connection);
    }
  ], callback);
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