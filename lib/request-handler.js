'use strict';
var util = require('util');

var errors = require('./errors');
var types = require('./types');
var utils = require('./utils');
var requests = require('./requests');
var retry = require('./policies/retry');

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
 * Borrows a connection iterating from the query plan one or more times, until finding an open connection with the
 * keyspace set.
 * It invokes the callback with the err, connection and host as parameters.
 * The error can only be a NoHostAvailableError instance.
 * @param {Iterator} iterator
 * @param {Object} triedHosts
 * @param {ProfileManager} profileManager
 * @param {String} keyspace
 * @param {Function} callback
 */
RequestHandler.borrowNextConnection = function(iterator, triedHosts, profileManager, keyspace, callback) {
  triedHosts = triedHosts || {};
  var host = getNextHost(iterator, profileManager, triedHosts);
  if (host === null) {
    return callback(new errors.NoHostAvailableError(triedHosts));
  }

  RequestHandler.borrowFromHost(host, keyspace, function borrowFromHostCallback(err, connection) {
    if (err) {
      triedHosts[host.address] = err;
      if (connection) {
        host.removeFromPool(connection);
      }
      // The error occurred on a different tick, so there is no risk of issuing a large number sync recursive calls
      return RequestHandler.borrowNextConnection(iterator, triedHosts, profileManager, keyspace, callback);
    }
    callback(null, connection, host);
  });
};

/**
 * Borrows a connection from the provided host, changing the current keyspace, if necessary.
 * @param {Host} host
 * @param {String} keyspace
 * @param {Function} callback
 */
RequestHandler.borrowFromHost = function (host, keyspace, callback) {
  host.borrowConnection(function (err, connection) {
    if (err) {
      return callback(err);
    }
    if (!keyspace || keyspace === connection.keyspace) {
      // Connection is ready to be used
      return callback(null, connection);
    }
    connection.changeKeyspace(keyspace, function (err) {
      if (err) {
        return callback(err, connection);
      }
      callback(null, connection);
    });
  });
};

/**
 * Gets the next host from the query plan.
 * @param {Iterator} iterator
 * @param {ProfileManager} profileManager
 * @param {Object} triedHosts
 * @return {Host|null}
 * @private
 */
function getNextHost(iterator, profileManager, triedHosts) {
  var host;
  // Get a host that is UP in a sync loop
  while (true) {
    var item = iterator.next();
    if (item.done) {
      return null;
    }
    host = item.value;
    // set the distance relative to the client first
    var distance = profileManager.getDistance(host);
    if (distance === types.distance.ignored) {
      //If its marked as ignore by the load balancing policy, move on.
      continue;
    }
    if (host.isUp()) {
      break;
    }
    triedHosts[host.address] = 'Host considered as DOWN';
  }
  return host;
}

/**
 * Gets a connection from the next host according to the load balancing policy
 * @param {QueryOptions} queryOptions
 * @param {function} callback
 * @private
 */
RequestHandler.prototype._getNextConnection = function (queryOptions, callback) {
  var self = this;
  var keyspace = this.client.keyspace;
  if (this.hostIterator) {
    return self._iterateThroughHosts(this.hostIterator, callback);
  }
  this.loadBalancingPolicy.newQueryPlan(keyspace, queryOptions, function newQueryPlanCallback(err, iterator) {
    if (err) {
      return callback(err);
    }
    self.hostIterator = iterator;
    self._iterateThroughHosts(self.hostIterator, callback);
  });
};

/**
 * Uses the iterator to try to acquire a connection from a host
 * @param {Iterator} iterator
 * @param {Function} callback callback(err, connection, host) to use
 * @private
 */
RequestHandler.prototype._iterateThroughHosts = function (iterator, callback) {
  var self = this;
  RequestHandler.borrowNextConnection(iterator, this.triedHosts, this.client.profileManager, this.client.keyspace,
    function borrowCallback(err, c, h) {
      if (c) {
        // Connection acquired
        self.host = h;
        return callback(null, c);
      }
      return callback(err);
    });
};

/**
 * Gets connection from the host connection pool
 * @param {Host} host
 * @param {Function} callback callback(err, connection, host) to use
 * @private
 */
RequestHandler.prototype._getPooledConnection = function (host, callback) {
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
  // TODO: Get the new query plan
  this._getNextConnection(options, function (err, c) {
    if (err) {
      //No connection available
      return callback(err);
    }
    self.connection = c;
    self._sendOnConnection(request, options, callback);
  });
};

/**
 * Sends a request to the current connection, adapting the result and retrying, if necessary.
 * @param request
 * @param {QueryOptions} options
 * @param {Function} callback
 * @private
 */
RequestHandler.prototype._sendOnConnection = function (request, options, callback) {
  var self = this;
  this.connection.sendStream(request, options, function readCallback(err, response) {
    if (err) {
      //Something bad happened, maybe retry or do something about it
      return self._handleError(err, callback);
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
 * Sets the keyspace in any connection that is already opened.
 * @param {Function} callback
 */
RequestHandler.prototype.setKeyspace = function (callback) {
  //TODO: Change to static method
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
 * Checks if the error and determines if it should be ignored, retried or rethrown.
 * @param {Object} err
 * @param {Function} callback
 * @private
 */
RequestHandler.prototype._handleError = function (err, callback) {
  // add the error to the tried hosts
  this.triedHosts[this.host.address] = err;
  err['coordinator'] = this.host.address;
  if ((err instanceof errors.ResponseError) && err.code === types.responseErrorCodes.unprepared) {
    //noinspection JSUnresolvedVariable
    return this._prepareAndRetry(err.queryId, callback);
  }
  var decisionInfo = this._getDecision(err);
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
  return this._retry(decisionInfo.consistency, decisionInfo.useCurrentHost, callback);
};

/**
 * @returns {{decision, useCurrentHost, consistency}}
 */
RequestHandler.prototype._getDecision = function (err) {
  var operationInfo = {
    query: this.request && this.request.query,
    options: this.requestOptions,
    nbRetry: this.retryCount,
    // handler, request and retryOnTimeout properties are deprecated and should be removed in the next major version
    handler: this,
    request: this.request,
    retryOnTimeout: false
  };
  var self = this;
  function onRequestError() {
    if (self.retryHandler) {
      return retryOnNextHostDecision;
    }
    return self.retryPolicy.onRequestError(operationInfo, self.request.consistency, err);
  }
  if (err.isSocketError) {
    if (err.requestNotWritten) {
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
    operationInfo.retryOnTimeout = this.requestOptions.retryOnTimeout !== false;
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
        return this.retryPolicy.onUnavailable(operationInfo, err.consistencies, err.required, err.alive);
      case types.responseErrorCodes.readTimeout:
        //noinspection JSUnresolvedVariable
        return this.retryPolicy.onReadTimeout(
          operationInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
      case types.responseErrorCodes.writeTimeout:
        //noinspection JSUnresolvedVariable
        return this.retryPolicy.onWriteTimeout(
          operationInfo, err.consistencies, err.received, err.blockFor, err.writeType);
    }
  }
  return { decision: retry.RetryPolicy.retryDecision.rethrow };
};

/**
 * @param {Number} consistency
 * @param {Boolean} useCurrentHost
 * @param {Function} callback
 * @private
 */
RequestHandler.prototype._retry = function (consistency, useCurrentHost, callback) {
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
    return this._sendOnConnection(this.request, this.requestOptions, callback);
  }
  // use the next host in the query plan to send the request
  this.send(this.request, this.requestOptions, callback);
};

/**
 * Issues a PREPARE request on the current connection.
 * If there's a socket or timeout issue, it moves to next host and executes the original request.
 * @param {Buffer} queryId
 * @param {Function} callback
 * @private
 */
RequestHandler.prototype._prepareAndRetry = function (queryId, callback) {
  this.log('info', util.format('Query 0x%s not prepared on host %s, preparing and retrying',
    queryId.toString('hex'), this.host.address));
  var info = this.client.metadata.getPreparedById(queryId);
  if (!info) {
    return callback(
      new errors.DriverInternalError(util.format('Unprepared response invalid, id: %s', queryId.toString('hex'))));
  }
  if (info.keyspace && info.keyspace !== this.connection.keyspace) {
    return callback(new Error(util.format('Query was prepared on keyspace %s, can\'t execute it on %s (%s)',
      info.keyspace, this.connection.keyspace, info.query)));
  }
  var self = this;
  this.connection.prepareOnce(info.query, function (err) {
    if (err) {
      if (!err.isSocketError && err instanceof errors.OperationTimedOutError) {
        self.log('warning', util.format('Unexpected error when re-preparing query on host %s'));
      }
      // There was a failure re-preparing on this connection.
      // Execute the original request on the next connection and forget about the PREPARE-UNPREPARE flow.
      return self.send(self.request, self.requestOptions, callback);
    }
    self._sendOnConnection(self.request, self.requestOptions, callback);
  });
};

module.exports = RequestHandler;
