"use strict";
var util = require('util');

var errors = require('./errors');
var types = require('./types');
var utils = require('./utils');
var requests = require('./requests');
var retry = require('./policies/retry');

var maxSyncHostIterations = 20;
var retryOnNextHostDecision = {
  decision: retry.RetryPolicy.retryDecision.retry,
  useCurrentHost: false,
  consistency: undefined
};

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 * @param {Client} client Client instance used to retrieve and set the keyspace.
 * @param {LoadBalancingPolicy} loadBalancingPolicy The load-balancing policy to use for the executions.
 * @param {RetryPolicy} retryPolicy The retry policy to use for the executions.
 * @constructor
 */
function RequestHandler(client, loadBalancingPolicy, retryPolicy) {
  this.client = client;
  this.loadBalancingPolicy = loadBalancingPolicy;
  this.retryPolicy = retryPolicy;
  this.profileManager = client.profileManager;
  this.logEmitter = client.options.logEmitter;
  this.retryCount = 0;
  //current request being executed.
  this.request = null;
  //the options for the request.
  this.requestOptions = utils.emptyObject;
  //The host selected by the load balancing policy to execute the request
  /** @type {Host} */
  this.host = null;
  /** @type {Connection} */
  this.connection = null;
  /** @type {Function} */
  this.retryHandler = null;
  this.stackContainer = null;
  this.triedHosts = {};
  this.hostIterations = 0;
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {QueryOptions} queryOptions
 * @param {function} callback
 */
RequestHandler.prototype.getNextConnection = function (queryOptions, callback) {
  var self = this;
  var keyspace = this.client.keyspace;
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
  /** @type {Host} */
  var host;
  // get a host that is UP in a sync loop
  while (true) {
    var item = iterator.next();
    if (item.done) {
      return callback(new errors.NoHostAvailableError(this.triedHosts));
    }
    host = item.value;
    // set the distance relative to the client first
    var distance = this.profileManager.getDistance(host);
    if (distance === types.distance.ignored) {
      //If its marked as ignore by the load balancing policy, move on.
      continue;
    }
    if (host.isUp()) {
      break;
    }
    this.triedHosts[host.address] = 'Host considered as DOWN';
  }
  var self = this;
  this.getPooledConnection(host, function iterateSingleCallback(err, connection) {
    if (connection) {
      // Connection acquired
      self.host = host;
      return callback(null, connection);
    }
    if (err) {
      self.triedHosts[host.address] = err;
    }
    if (++self.hostIterations > maxSyncHostIterations) {
      //avoid a large number sync recursive calls
      self.hostIterations = 0;
      return process.nextTick(function iterateOnNextTick() {
        self.iterateThroughHosts(iterator, callback);
      });
    }
    //move to next host
    return self.iterateThroughHosts(iterator, callback);
  });
};

/**
 * Gets connection from the host connection pool
 * @param {Host} host
 * @param {Function} callback callback(err, connection, host) to use
 */
RequestHandler.prototype.getPooledConnection = function (host, callback) {
  // Implementation detail: avoid the overhead of async flow control functions
  var self = this;
  host.borrowConnection(function (err, connection) {
    if (err) {
      return callback(err);
    }
    if (!self.client.keyspace || self.client.keyspace === connection.keyspace) {
      // connection is ready
      return callback(null, connection);
    }
    // switch keyspace
    connection.changeKeyspace(self.client.keyspace, function (err) {
      if (err) {
        host.removeFromPool(connection);
        return callback(err);
      }
      callback(null, connection);
    });
  });
};

RequestHandler.prototype.log = utils.log;

/**
 * Gets an available connection and sends the request
 * @param request
 * @param {QueryOptions|null} options
 * @param {Function} callback
 */
RequestHandler.prototype.send = function (request, options, callback) {
  if (this.request === null) {
    options = options || utils.emptyObject;
    if (options.captureStackTrace) {
      Error.captureStackTrace(this.stackContainer = {});
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
      return self.client.handleSchemaAgreementAndRefresh(self.connection, response.schemaChange, function schemaCb() {
        callback(null, result);
      });
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
    this.requestOptions.retryOnTimeout || this.request instanceof requests.PrepareRequest;
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
  Error.captureStackTrace(this.stackContainer = {});
  var self = this;
  this.retryHandler = function () {
    // Use a custom retryHandler
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
  utils.eachSeries(queries, function (query, next) {
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
 * Checks if the error and determines if it should be ignored, retried or rethrown.
 * @param {Object} err
 * @param {Function} callback
 */
RequestHandler.prototype.handleError = function (err, callback) {
  // add the error to the tried hosts
  this.triedHosts[this.host.address] = err;
  err['coordinator'] = this.host.address;
  if ((err instanceof errors.ResponseError) && err.code ===  types.responseErrorCodes.unprepared) {
    //noinspection JSUnresolvedVariable
    return this.prepareAndRetry(err.queryId, callback);
  }
  var decisionInfo = this.getDecision(err);
  if (err.isSocketError) {
    this.host.removeFromPool(this.connection);
  }
  if (!decisionInfo || decisionInfo.decision === retry.RetryPolicy.retryDecision.rethrow) {
    // callback in error
    if (this.requestOptions.captureStackTrace) {
      utils.fixStack(this.stackContainer.stack, err);
    }
    if (this.request instanceof requests.QueryRequest || this.request instanceof requests.ExecuteRequest) {
      err['query'] = this.request.query;
    }
    return callback(err);
  }
  if (decisionInfo.decision === retry.RetryPolicy.retryDecision.ignore) {
    //Return an empty response
    return callback(
      null,
      new types.ResultSet(utils.emptyObject, this.host.address, this.triedHosts, this.request.consistency));
  }
  return this.retry(decisionInfo.consistency, decisionInfo.useCurrentHost, callback);
};

/**
 * @returns {{decision, useCurrentHost, consistency}}
 */
RequestHandler.prototype.getDecision = function (err) {
  var requestInfo = {
    request: this.request,
    handler: this,
    nbRetry: this.retryCount
  };
  var self = this;
  function onRequestError() {
    if (self.retryHandler) {
      return retryOnNextHostDecision;
    }
    return self.retryPolicy.onRequestError(requestInfo, self.request.consistency, err);
  }
  if (err.isSocketError) {
    if (!err.wasRequestWritten) {
      // the request was definitely not applied, it's safe to retry
      return retryOnNextHostDecision;
    }
    return onRequestError();
  }
  if (err instanceof errors.OperationTimedOutError) {
    this.log('warning', err.message);
    this.host.checkHealth(this.connection);
    if (this.request instanceof requests.PrepareRequest) {
      // always retry on next host for PREPARE requests
      return retryOnNextHostDecision;
    }
    requestInfo.retryOnTimeout = this.requestOptions.retryOnTimeout !== false;
    return onRequestError();
  }
  if (err instanceof errors.ResponseError) {
    switch (err.code) {
      case types.responseErrorCodes.overloaded:
      case types.responseErrorCodes.isBootstrapping:
      case types.responseErrorCodes.truncateError:
        return onRequestError();
      case types.responseErrorCodes.unavailableException:
        //noinspection JSUnresolvedVariable
        return this.retryPolicy.onUnavailable(requestInfo, err.consistencies, err.required, err.alive);
      case types.responseErrorCodes.readTimeout:
        //noinspection JSUnresolvedVariable
        return this.retryPolicy.onReadTimeout(
          requestInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
      case types.responseErrorCodes.writeTimeout:
        //noinspection JSUnresolvedVariable
        return this.retryPolicy.onWriteTimeout(
          requestInfo, err.consistencies, err.received, err.blockFor, err.writeType);
    }
  }
  return { decision: retry.RetryPolicy.retryDecision.rethrow }
};

/**
 * @param {Number} consistency
 * @param {Boolean} useCurrentHost
 * @param {Function} callback
 */
RequestHandler.prototype.retry = function (consistency, useCurrentHost, callback) {
  this.log('info', 'Retrying request');
  if (this.retryHandler) {
    // custom retry handler (not a QueryRequest / ExecuteRequest / BatchRequest)
    return this.retryHandler(callback);
  }
  this.retryCount++;
  if (typeof consistency === 'number') {
    this.request.consistency = consistency;
  }
  if (useCurrentHost !== false) {
    // retry on the current host by default
    return this.sendOnConnection(this.request, this.requestOptions, callback);
  }
  // use the next host in the query plan to send the request
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

module.exports = RequestHandler;