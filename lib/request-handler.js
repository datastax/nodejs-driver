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
  var triedHosts = {};
  var hostIterator = self.loadBalancingPolicy.newQueryPlan(function (err, iterator) {
    var host;
    var connection = null;
    async.whilst(
      function condition() {
        var item = iterator.next();
        host = item.value;
        return (
            !item.done &&
            !connection);
      },
      function whileIterator(next) {
        if (!host.canBeConsideredAsUp()) {
          return next();
        }
        host.borrowConnection(function (err, c) {
          if (err) {
            triedHosts[host.address] = err;
          }
          else {
            connection = c;
          }
          next();
        });
      },
      function whileEnd(err) {
        if (!connection) {
          return callback(new errors.NoHostAvailableError(triedHosts));
        }
        //TODO: Set the keyspace to the connection
        callback(null, connection);
      });
  });
};

/**
 * Gets an available connection and sends the request
 */
RequestHandler.prototype.send = function (request, options, callback) {
  var self = this;
  this._getNextConnection(function (err, c) {
    if (err) {
      //No connection available
      return callback(err);
    }
    c.sendStream(request, null, function (err, response) {
      if (err) {
        //Something bad happened, maybe retry or do something about it
        return self.handleError(err, callback);
      }
      callback(null, response);
    })
  });
};

/**
 * Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
 * @param {Error} err
 * @param {Function} callback
 */
RequestHandler.prototype.handleError = function (err, callback) {
  //TODO: Implement
  callback(err);
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
    if (!connection) {
      err = new errors.NoHostAvailableError(openingErrors);
    }
    callback(err, connection);
  });
};

module.exports = RequestHandler;