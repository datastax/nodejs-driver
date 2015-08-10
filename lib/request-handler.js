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
  this.loadBalancingPolicy = options.policies.loadBalancing;
  this.retryPolicy = options.policies.retry;
  this.logEmitter = options.logEmitter;
  this.retryCount = 0;
  //current request being executed.
  this.request = null;
  //the options for the request.
  this.requestOptions = null;
  //The host selected by the load balancing policy to execute the request
  /** @type {Host} */
  this.host = null;
  /** @type {Connection} */
  this.connection = null;
  /** @type {Function} */
  this.retryHandler = null;
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
  if (this.hostIterator) {
    return self.iterateThroughHosts(this.hostIterator, callback);
  }
  this.loadBalancingPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    if (err) return callback(err);
    self.hostIterator = iterator;
    self.iterateThroughHosts(self.hostIterator, callback);
  });
};

/**
 * Uses the iterator to try to acquire a connection from a host
 * @param {{next: function}} iterator
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
          self.triedHosts[host.address] = err;
          host.setDown();
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
 * Gets an available connection and sends the request
 * @param request
 * @param {QueryOptions} options
 * @param {Function} callback
 */
RequestHandler.prototype.send = function (request, options, callback) {
  if (this.request === null) {
    //Set the first time
    //noinspection JSUnresolvedFunction
    Error.captureStackTrace(this.stackContainer);
    if (options && options.retry) {
      this.retryPolicy = options.retry;
    }
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
 * Sends a request to the current connection, adapting the result and retrying, if necessary.
 * @param request
 * @param {QueryOptions} options
 * @param {Function} callback
 */
RequestHandler.prototype.sendOnConnection = function (request, options, callback) {
  var self = this;
  this.connection.sendStream(request, options, function readCallback(err, response) {
    if (err) {
      //Something bad happened, maybe retry or do something about it
      return self.handleError(err, callback);
    }
    //it looks good, lets ensure this host is marked as UP
    self.host.setUp();
    response = response || utils.emptyObject;
    var result = new types.ResultSet(response, self.host.address, self.triedHosts, self.request.consistency);
    if (result.info.warnings) {
      //log the warnings
      result.info.warnings.forEach(function (message, i, warnings) {
        self.log('warning', util.format(
          'Received warning (%d of %d) "%s" for query "%s"',
          i + 1,
          warnings.length,
          message,
          self.request.query));
      });
    }
    if (response.schemaChange) {
      self.client.waitForSchemaAgreement(self.connection, function (err) {
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
    callback(null, result);
  });
};

/**
 * Executed once there is a request timeout
 * @param {Error} err
 * @param {Function} callback
 */
RequestHandler.prototype.onTimeout = function (err, callback) {
  this.log('warning', err.message);
  this.host.checkHealth(this.connection);
  //Always retry on next host for PREPARE requests and when specified by user
  var executeOnNextHost =
    (this.requestOptions && this.requestOptions.retryOnTimeout) ||
    this.request instanceof requests.PrepareRequest;
  if (executeOnNextHost) {
    return this.send(this.request, this.requestOptions, callback);
  }
  callback(err);
};

/**
 * Sets the keyspace in any connection that is already opened.
 * @param {Function} callback
 */
RequestHandler.prototype.setKeyspace = function (callback) {
  var connection;
  var hosts = this.client.hosts.values();
  for (var i = 0; i < hosts.length; i++) {
    this.host = hosts[i];
    connection = this.host.getActiveConnection();
    if (connection) {
      break;
    }
  }
  if (!connection) {
    return callback(new errors.DriverInternalError('No active connection found'));
  }
  connection.changeKeyspace(this.client.keyspace, callback);
};

/**
 * Sends multiple prepare requests on the next connection available
 * @param {Array.<string>} queries
 * @param {Array.<function>} callbacksArray
 * @param {QueryOptions} options
 * @param {function} callback
 */
RequestHandler.prototype.prepareMultiple = function (queries, callbacksArray, options, callback) {
  //noinspection JSUnresolvedFunction
  Error.captureStackTrace(this.stackContainer);
  var self = this;
  this.retryHandler = function () {
    //Use a custom retryHandler
    self.prepareMultiple(queries, callbacksArray, options, callback);
  };
  this.getNextConnection(options, function (err, c) {
    if (err) {
      //No connection available, no point on retrying
      return callback(err);
    }
    self.connection = c;
    self.prepareOnConnection(queries, callbacksArray, callback);
  });
};

/**
 * Serially prepares multiple queries on the current connection and handles retry if necessary
 * @param {Array.<string>} queries
 * @param {Array.<function>} callbacksArray
 * @param {function} callback
 */
RequestHandler.prototype.prepareOnConnection = function (queries, callbacksArray, callback) {
  var self = this;
  var index = 0;
  async.eachSeries(queries, function (query, next) {
    self.connection.prepareOnce(query, function (err, response) {
      if (callbacksArray) {
        callbacksArray[index++](err, response);
      }
      next(err);
    });
  }, function (err) {
    if (err) {
      return self.handleError(err, callback);
    }
    callback();
  });
};

/**
 * Checks if the exception is either a Cassandra response error or a socket exception to retry or failover if necessary.
 * @param {Error} err
 * @param {Function} callback
 */
RequestHandler.prototype.handleError = function (err, callback) {
  //Add the error to the tried hosts
  this.triedHosts[this.host.address] = err;
  var decisionInfo = { decision: retry.RetryPolicy.retryDecision.rethrow };
  //noinspection JSUnresolvedVariable
  if (err && err.isServerUnhealthy) {
    this.host.setDown();
    return this.retry(callback);
  }
  if (err instanceof errors.OperationTimedOutError) {
    return this.onTimeout(err, callback);
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
  //Fill error information and return it
  utils.fixStack(this.stackContainer.stack, err);
  if (this.request instanceof requests.QueryRequest || this.request instanceof requests.ExecuteRequest) {
    err['query'] = this.request.query;
  }
  callback(err);
};

RequestHandler.prototype.retry = function (callback) {
  this.retryCount++;
  this.log('info', 'Retrying request');
  if (this.retryHandler) {
    return this.retryHandler(callback);
  }
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
  if (this.request instanceof requests.ExecuteRequest) {
    //Its a single Execute of a prepared statement
    this.prepareOnConnection([this.request.query], [function (err, response) {
      if (err) return; //let the error be handle at general callback level
      if (!Buffer.isBuffer(response.id) ||
        response.id.length !== self.request.queryId.length ||
        response.id.toString('hex') !== self.request.queryId.toString('hex')
      ) {
        self.log('warning', util.format('Unexpected difference between query ids for query "%s" (%s !== %s)',
          self.request.query,
          response.id.toString('hex'),
          self.request.queryId.toString('hex')));
        self.request.queryId = response.id;
      }
    }], retryRequest);
    return;
  }
  if (this.request instanceof requests.BatchRequest) {
    //Its a BATCH of prepared statements
    //We need to prepare all the different queries in the batch in the current host
    var queries = {};
    this.request.queries.forEach(function (item) {
      //Unique queries
      queries[item.query] = item.info;
    });
    var queryStrings = Object.keys(queries);
    var singleCallbacks = new Array(queryStrings.length);
    queryStrings.forEach(function (query, i) {
      var info = queries[query];
      singleCallbacks[i] = function singlePrepareCallback(err, response) {
        if (err) return; //let the error be handle at general callback level
        if (!Buffer.isBuffer(response.id) ||
          response.id.length !== info.queryId.length ||
          response.id.toString('hex') !== info.queryId.toString('hex')
        ) {
          self.log('warning', util.format('Unexpected difference between query ids for query "%s" (%s !== %s)',
            info.query,
            response.id.toString('hex'),
            info.queryId.toString('hex')));
          info.queryId = response.id;
        }
      }
    });
    this.prepareOnConnection(queryStrings, singleCallbacks, retryRequest);
    return;
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
  var connection = null;
  var index = 0;
  var openingErrors = {};
  var hosts = hostMap.values();
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