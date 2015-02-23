var util = require('util');
var async = require('async');

var errors = require('./errors');
var types = require('./types');
var utils = require('./utils');
var requests = require('./requests');
var retry = require('./policies/retry');

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 * @param {?Client} client Client instance used to retrieve and set the keyspace. It can be null.
 * @param {Object} options
 * @constructor
 */
function RequestHandler(client, options) {
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
  this.triedHosts = {};
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {QueryOptions} queryOptions
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
        self.triedHosts[host.address] = 'Host considered as DOWN';
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
          self.triedHosts[host.address] = err;
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
        return callback(new errors.NoHostAvailableError(self.triedHosts));
      }
      callback(null, connection, host);
    });
};

RequestHandler.prototype.log = utils.log;

/**
 * Reuses current connection or gets a new available connection and sends the request.
 * @param request
 * @param {QueryOptions} options
 * @param {Function} callback
 */
RequestHandler.prototype.send = function (request, options, callback) {
  if (this.request === null) {
    //noinspection JSUnresolvedFunction
    Error.captureStackTrace(this.stackContainer);
  }
  this.request = request;
  this.requestOptions = options;
  if (this.connection) {
    return this.sendOnConnection(request, options, callback);
  }
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
 * Sends a request to the current connection, adapting the result and retrying, if necessary.
 * @param request
 * @param {QueryOptions} options
 * @param {Function} callback
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
      var result = new types.ResultSet(response, self.host.address, self.triedHosts, self.request.consistency);
      if (response.schemaChange) {
        self.client.waitForSchemaAgreement(function (err) {
          if (err) {
            //we issue a warning but we continue with the normal flow
            self.log('warning', 'There was an error while waiting for the schema agreement between nodes', err);
          }
          callback(null, result);
        });
        return;
      }
      if (response.keyspaceSet) {
        self.client.keyspace = response.keyspaceSet;
      }
    }
    callback(null, result);
  });
};

/**
 * Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
 * @param {Error} err
 * @param {Function} callback
 */
RequestHandler.prototype.handleError = function (err, callback) {
  var decisionInfo = {decision: retry.RetryPolicy.retryDecision.rethrow};
  //noinspection JSUnresolvedVariable
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
        //noinspection JSUnresolvedVariable
        return this.prepareAndRetry(err.queryId, callback);
      case types.responseErrorCodes.overloaded:
      case types.responseErrorCodes.isBootstrapping:
      case types.responseErrorCodes.truncateError:
        //always retry
        return this.retry(callback);
      case types.responseErrorCodes.unavailableException:
        //noinspection JSUnresolvedVariable
        decisionInfo = this.retryPolicy.onUnavailable(requestInfo, err.consistencies, err.required, err.alive);
        break;
      case types.responseErrorCodes.readTimeout:
        //noinspection JSUnresolvedVariable
        decisionInfo = this.retryPolicy.onReadTimeout(requestInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
        break;
      case types.responseErrorCodes.writeTimeout:
        //noinspection JSUnresolvedVariable
        decisionInfo = this.retryPolicy.onWriteTimeout(requestInfo, err.consistencies, err.received, err.blockFor, err.writeType);
        break;
    }
  }
  if (decisionInfo && decisionInfo.decision === retry.RetryPolicy.retryDecision.retry) {
    return this.retry(callback);
  }
  //throw ex
  this.connection = null;
  utils.fixStack(this.stackContainer.stack, err);
  callback(err);
};

/**
 * Retries on different host. Deletes connection and calls send.
 */
RequestHandler.prototype.retry = function (callback) {
  this.retryCount++;
  this.log('info', 'Retrying request');
  this.connection = null;
  this.send(this.request, this.requestOptions, callback);
};

/**
 * Prepares the query and retries on the SAME host
 * @param {Buffer} queryId
 * @param {Function} callback
 */
RequestHandler.prototype.prepareAndRetry = function (queryId, callback) {
  this.log('info', util.format('Query 0x%s not prepared on host %s, preparing and retrying', queryId.toString('hex'), this.host.address));
  var self = this;
  function retryRequest (err) {
    if (err) {
      //Could not be prepared or retried, just send the error to caller
      return callback(err)
    }
    self.sendOnConnection(self.request, self.requestOptions, callback);
  }
  // Each one has item.query = <query string>, item.info = <place to get/put queryId>
  var items;
  if (this.request instanceof requests.ExecuteRequest) {
    items = [{
      query: this.request.query,
      info: this.request
    }];
  }
  else if (this.request instanceof requests.BatchRequest) {
    items = this.request.queries;
  }
  if (items) {
    var prepared = {};
    return async.eachSeries(items, prepareQuery, retryRequest);
    function prepareQuery(item, next) {
      var preparedResponse = prepared[item.query];
      if (preparedResponse) {
        updateQuery(preparedResponse);
        return next();
      }
      self.connection.sendStream(
        new requests.PrepareRequest(item.query),
        null,
        function (err, response) {
          if (!err) {
            updateQuery(response);
            prepared[item.query] = response;
          }
          next(err);
        }
      );
      function updateQuery(response) {
        if (!Buffer.isBuffer(response.id) ||
          response.id.length !== item.info.queryId.length ||
          response.id.toString('hex') !== item.info.queryId.toString('hex')
        ) {
          self.log('warning', util.format('Unexpected difference between query ids for query "%s" (%s !== %s)',
            item.query,
            response.id.toString('hex'),
            item.info.queryId.toString('hex')));
          item.info.queryId = response.id;
        }
      }
    }
  }
  return callback(new errors.DriverInternalError('Obtained unprepared from wrong request'));
};

/**
 * Gets an open connection to using the provided hosts Array, without using the load balancing policy.
 * Invoked before the Client can access topology of the cluster.
 * @param {HostMap} hostMap
 * @param {Function} callback
 */
RequestHandler.prototype.getFirstConnection = function (hostMap, callback) {
  // TODO: This does not belong here. Only used by ControlConnection and
  // does not reference any RequestHandler attributes.
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
