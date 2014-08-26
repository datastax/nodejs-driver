var util = require('util');
var async = require('async');

var errors = require('./errors.js');
var types = require('./types.js');
var retry = require('./policies/retry.js');

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 * @param {String} keyspace
 * @param {Object} options
 * @constructor
 */
function RequestHandler(keyspace, options)
{
  this.keyspace = keyspace;
  this.loadBalancingPolicy = options.policies.loadBalancing;
  this.retryPolicy = options.policies.retry;
  //current request being executed.
  this.request = null;
  //the options for the request.
  this.requestOptions = null;
  //The host selected by the load balancing policy to execute the request
  this.host = null;
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {function} callback
 */
RequestHandler.prototype.getNextConnection = function (callback) {
  var self = this;
  var triedHosts = {};
  self.loadBalancingPolicy.newQueryPlan(function (err, iterator) {
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
            //Cannot get a connection
            //Its a bad host
            host.setDown();
            triedHosts[host.address] = err;
          }
          else {
            self.host = host;
            connection = c;
          }
          next();
        });
      },
      function whileEnd(err) {
        if (err) {
          return callback(err);
        }
        if (!connection) {
          return callback(new errors.NoHostAvailableError(triedHosts));
        }
        //TODO: Set the keyspace to the connection (within the iteration, to continue with the next)
        callback(null, connection, host);
      });
  });
};

/**
 * Gets an available connection and sends the request
 */
RequestHandler.prototype.send = function (request, options, callback) {
  this.request = request;
  this.requestOptions = options;
  var self = this;
  this.getNextConnection(function (err, c) {
    if (err) {
      //No connection available
      return callback(err);
    }
    c.sendStream(request, options, function (err, response) {
      if (err) {
        //Something bad happened, maybe retry or do something about it
        return self.handleError(err, callback);
      }
      //it looks good, lets ensure this host is marked as UP
      self.host.setUp();
      callback(null, response);
    });
  });
};

/**
 * Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
 * @param {Error} err
 * @param {Function} callback
 */
RequestHandler.prototype.handleError = function (err, callback) {
  var decisionInfo = {decision: retry.RetryPolicy.retryDecision.rethrow};
  if (err && err.isServerUnhealthy) {
    this.host.setDown();
    return this.retry(callback);
  }
  if (err instanceof errors.ResponseError) {
    var requestInfo = {
      request: this.request,
      handler: this
    };
    switch (err.code) {
      case types.responseErrorCodes.unprepared:
        return this.prepareAndRetry(callback);
      case types.responseErrorCodes.overloaded:
      case types.responseErrorCodes.isBootstrapping:
      case types.responseErrorCodes.truncateError:
        //always retry
        return this.retry(callback);
      case types.responseErrorCodes.unavailableException:
        decisionInfo = this.retryPolicy.onUnavailable(requestInfo, err.consistency, err.required, err.alive);
        break;
      case types.responseErrorCodes.readTimeout:
        decisionInfo = this.retryPolicy.onReadTimeout(requestInfo, err.consistency, err.received, err.blockFor, err.isDataPresent);
        break;
      case types.responseErrorCodes.writeTimeout:
        decisionInfo = this.retryPolicy.onWriteTimeout(requestInfo, err.consistency, err.received, err.blockFor, err.writeType);
        break;
    }
  }
  if (decisionInfo && decisionInfo.decision === retry.RetryPolicy.retryDecision.retry) {
    return this.retry(callback);
  }
  //throw ex
  callback(err);
};

RequestHandler.prototype.retry = function (callback) {
  this.send(this.request, this.requestOptions, callback);
};

/**
 * Prepares the query and retries on the SAME host
 * @param callback
 */
RequestHandler.prototype.prepareAndRetry = function (callback) {
  callback(new Error('Not implemented'));
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
  var host = null;
  async.doWhilst(function iterator(next) {
    host = hosts[index];
    host.borrowConnection(function (err, c) {
      if (err) {
        openingErrors.push(err);
      }
      else {
        connection = c;
      }
      next();
    });
  }, function condition () {
    return !connection && (++index < hosts.length);
  }, function done(err) {
    if (!connection) {
      err = new errors.NoHostAvailableError(openingErrors);
    }
    callback(err, connection, host);
  });
};

module.exports = RequestHandler;