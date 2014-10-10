var util = require('util');
var async = require('async');

var errors = require('./errors.js');
var types = require('./types.js');
var utils = require('./utils.js');
var requests = require('./requests.js');
var retry = require('./policies/retry.js');

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 * @param {?Client} client Client instance used to retrieve and set the keyspace. It can be null.
 * @param {Object} options
 * @constructor
 */
function RequestHandler(client, options)
{
  this.client = client;
  this.options = options;
  this.loadBalancingPolicy = options.policies.loadBalancing;
  this.retryPolicy = options.policies.retry;
  this.retryCount = 0;
  //current request being executed.
  this.request = null;
  //the options for the request.
  this.requestOptions = null;
  //The host selected by the load balancing policy to execute the request
  this.host = null;
  this.stackContainer = {};
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {object} queryOptions
 * @param {function} callback
 */
RequestHandler.prototype.getNextConnection = function (queryOptions, callback) {
  var self = this;
  var keyspace;
  if (this.client) {
    keyspace = this.client.keyspace;
  }
  this.loadBalancingPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    if (err) return callback(err);
    self.iterateThroughHosts(iterator, callback);
  });
};

/**
 * Uses the iterator to try to acquire a connection from a host
 * @param {{next: function}}iterator
 * @param {Function} callback callback(err, connection, host) to use
 */
RequestHandler.prototype.iterateThroughHosts = function (iterator, callback) {
  var triedHosts = {};
  var host;
  var connection = null;
  var self = this;
  async.whilst(
    function condition() {
      //while there isn't a valid connection
      if (connection) {
        return false;
      }
      var item = iterator.next();
      host = item.value;
      return (!item.done);
    },
    function whileIterator(next) {
      if (!host.canBeConsideredAsUp()) {
        triedHosts[host.address] = 'Host considered as DOWN';
        return next();
      }
      //the distance relative to the client, using the load balancing policy
      var distance = self.loadBalancingPolicy.getDistance(host);
      host.setDistance(distance);
      if (distance === types.distance.ignored) {
        //If its marked as ignore by the load balancing policy, move on.
        return next();
      }
      async.waterfall([
        host.borrowConnection.bind(host),
        function changingKeyspace(c, waterfallNext) {
          //There isn't a client, so there isn't an active keyspace
          if (!self.client) return waterfallNext(null, c);
          //Try to change, if the connection is on the same connection it wont change it.
          c.changeKeyspace(self.client.keyspace, function (err) {
            waterfallNext(err, c);
          });
        }
      ], function (err, c) {
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
    function whilstEnded(err) {
      if (err) {
        return callback(err);
      }
      if (!connection) {
        return callback(new errors.NoHostAvailableError(triedHosts));
      }
      callback(null, connection, host);
    });
};

RequestHandler.prototype.log = utils.log;

/**
 * Gets an available connection and sends the request
 */
RequestHandler.prototype.send = function (request, options, callback) {
  if (this.request === null) {
    //noinspection JSUnresolvedFunction
    Error.captureStackTrace(this.stackContainer);
  }
  this.request = request;
  this.requestOptions = options;
  var self = this;
  this.getNextConnection(options, function (err, c) {
    if (err) {
      //No connection available
      return callback(err);
    }
    self.connection = c;
    self.sendOnConnection(request, options, callback);
  });
};

/**
 * Sends a request to the current connection. Handling retry if necessary.
 * @param request
 * @param options
 * @param callback
 */
RequestHandler.prototype.sendOnConnection = function (request, options, callback) {
  var self = this;
  this.connection.sendStream(request, options, function (err, response) {
    if (err) {
      //Something bad happened, maybe retry or do something about it
      return self.handleError(err, callback);
    }
    //it looks good, lets ensure this host is marked as UP
    self.host.setUp();
    if (response) {
      //Private _queriedHost for now, in the future change by full info with trace
      response._queriedHost = self.host.address;
      if (response.keyspaceSet) {
        self.client.keyspace = response.keyspaceSet;
      }
    }
    callback(null, response);
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
      handler: this,
      nbRetry: this.retryCount
    };
    switch (err.code) {
      case types.responseErrorCodes.unprepared:
        return this.prepareAndRetry(err.queryId, callback);
      case types.responseErrorCodes.overloaded:
      case types.responseErrorCodes.isBootstrapping:
      case types.responseErrorCodes.truncateError:
        //always retry
        return this.retry(callback);
      case types.responseErrorCodes.unavailableException:
        decisionInfo = this.retryPolicy.onUnavailable(requestInfo, err.consistencies, err.required, err.alive);
        break;
      case types.responseErrorCodes.readTimeout:
        decisionInfo = this.retryPolicy.onReadTimeout(requestInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
        break;
      case types.responseErrorCodes.writeTimeout:
        decisionInfo = this.retryPolicy.onWriteTimeout(requestInfo, err.consistencies, err.received, err.blockFor, err.writeType);
        break;
    }
  }
  if (decisionInfo && decisionInfo.decision === retry.RetryPolicy.retryDecision.retry) {
    this.retryCount++;
    return this.retry(callback);
  }
  //throw ex
  utils.fixStack(this.stackContainer.stack, err);
  callback(err);
};

RequestHandler.prototype.retry = function (callback) {
  this.log('info', 'Retrying request');
  this.send(this.request, this.requestOptions, callback);
};

/**
 * Prepares the query and retries on the SAME host
 * @param {Buffer} queryId
 * @param {Function} callback
 */
RequestHandler.prototype.prepareAndRetry = function (queryId, callback) {
  this.log('info', util.format('Query 0x%s not prepared on host %s, preparing and retrying', queryId.toString('hex'), this.host.address));
  //this.request should contain the Execute request, it should stay that way
  if (!(this.request instanceof requests.ExecuteRequest)) {
    return callback(new errors.DriverInternalError('Obtained unprepared from request'));
  }
  if (!this.request.query) {
    return callback(new errors.DriverInternalError('Request should contain query field'));
  }
  var self = this;
  this.connection.prepareOnce(this.request.query, function (err, response) {
    if (err) return callback(err);
    if (!Buffer.isBuffer(response.id) ||
      response.id.length !== self.request.queryId.length ||
      response.id.toString('hex') !== self.request.queryId.toString('hex')
      ) {
      return callback(new errors.DriverInternalError(util.format(
        'Unexpected difference between query ids %s !== %s',
        response.id.toString('hex'),
        self.request.queryId.toString('hex'))));
    }
    self.sendOnConnection(self.request, self.requestOptions, callback);
  });
};

/**
 * Gets an open connection to using the provided hosts Array, without using the load balancing policy.
 * Invoked before the Client can access topology of the cluster.
 * @param {HostMap} hostMap
 * @param {Function} callback
 */
RequestHandler.prototype.getFirstConnection = function (hostMap, callback) {
  this.log('info', 'Getting first connection available');
  var connection = null;
  var index = 0;
  var openingErrors = {};
  var hosts = hostMap.slice(0);
  var host = null;
  async.doWhilst(function iterator(next) {
    host = hosts[index];
    host.borrowConnection(function (err, c) {
      if (err) {
        openingErrors[host.address] = err;
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