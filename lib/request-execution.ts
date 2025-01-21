/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const errors = require('./errors');
const requests = require('./requests');
const retry = require('./policies/retry');
const types = require('./types');
const utils = require('./utils');
const promiseUtils = require('./promise-utils');

const retryOnCurrentHost = Object.freeze({
  decision: retry.RetryPolicy.retryDecision.retry,
  useCurrentHost: true,
  consistency: undefined
});

const rethrowDecision = Object.freeze({ decision: retry.RetryPolicy.retryDecision.rethrow });

/**
 * An internal representation of an error that occurred during the execution of a request.
 */
const errorCodes = {
  none: 0,
  // Socket error
  socketError: 1,
  // Socket error before the request was written to the wire
  socketErrorBeforeRequestWritten: 2,
  // OperationTimedOutError
  clientTimeout: 3,
  // Response error "unprepared"
  serverErrorUnprepared: 4,
  // Response error "overloaded", "is_bootstrapping" and "truncateError":
  serverErrorOverloaded: 5,
  serverErrorReadTimeout: 6,
  serverErrorUnavailable: 7,
  serverErrorWriteTimeout: 8,
  // Any other server error (different from the ones detailed above)
  serverErrorOther: 9
};

const metricsHandlers = new Map([
  [ errorCodes.none, (metrics, err, latency) => metrics.onSuccessfulResponse(latency) ],
  [ errorCodes.socketError, (metrics, err) => metrics.onConnectionError(err) ],
  [ errorCodes.clientTimeout, (metrics, err) => metrics.onClientTimeoutError(err) ],
  [ errorCodes.serverErrorOverloaded, (metrics, err) => metrics.onOtherError(err) ],
  [ errorCodes.serverErrorReadTimeout, (metrics, err) => metrics.onReadTimeoutError(err) ],
  [ errorCodes.serverErrorUnavailable, (metrics, err) => metrics.onUnavailableError(err) ],
  [ errorCodes.serverErrorWriteTimeout, (metrics, err) => metrics.onWriteTimeoutError(err) ],
  [ errorCodes.serverErrorOther, (metrics, err) => metrics.onOtherError(err) ]
]);

const metricsRetryHandlers = new Map([
  [ errorCodes.socketError, (metrics, err) => metrics.onOtherErrorRetry(err) ],
  [ errorCodes.clientTimeout, (metrics, err) => metrics.onClientTimeoutRetry(err) ],
  [ errorCodes.serverErrorOverloaded, (metrics, err) => metrics.onOtherErrorRetry(err) ],
  [ errorCodes.serverErrorReadTimeout, (metrics, err) => metrics.onReadTimeoutRetry(err) ],
  [ errorCodes.serverErrorUnavailable, (metrics, err) => metrics.onUnavailableRetry(err) ],
  [ errorCodes.serverErrorWriteTimeout, (metrics, err) => metrics.onWriteTimeoutRetry(err) ],
  [ errorCodes.serverErrorOther, (metrics, err) => metrics.onOtherErrorRetry(err) ]
]);

class RequestExecution {
  /**
   * Encapsulates a single flow of execution against a coordinator, handling individual retries and failover.
   * @param {RequestHandler!} parent
   * @param {Host!} host
   * @param {Connection!} connection
   */
  constructor(parent, host, connection) {
    this._parent = parent;
    /** @type {OperationState} */
    this._operation = null;
    this._host = host;
    this._connection = connection;
    this._cancelled = false;
    this._startTime = null;
    this._retryCount = 0;
    // The streamId information is not included in the request.
    // A pointer to the parent request can be used, except when changing the consistency level from the retry policy
    this._request = this._parent.request;

    // Mark that it launched a new execution
    parent.speculativeExecutions++;
  }

  /**
   * Sends the request using the active connection.
   */
  start() {
    this._sendOnConnection();
  }

  /**
   * Borrows the next connection available using the query plan and sends the request.
   * @returns {Promise<void>}
   */
  async restart() {
    try {
      const { host, connection } = this._parent.getNextConnection();

      this._connection = connection;
      this._host = host;
    } catch (err) {
      return this._parent.handleNoHostAvailable(err, this);
    }

    // It could be a new connection from the pool, we should make sure it's in the correct keyspace.
    const keyspace = this._parent.client.keyspace;
    if (keyspace && keyspace !== this._connection.keyspace) {
      try {
        await this._connection.changeKeyspace(keyspace);
      } catch (err) {
        // When its a socket error, attempt to retry.
        // Otherwise, rethrow the error to the user.
        return this._handleError(err, RequestExecution._getErrorCode(err));
      }
    }

    if (this._cancelled) {
      // No need to send the request or invoke any callback
      return;
    }

    this._sendOnConnection();
  }

  /**
   * Sends the request using the active connection.
   * @private
   */
  _sendOnConnection() {
    this._startTime = process.hrtime();

    this._operation =
      this._connection.sendStream(this._request, this._parent.executionOptions, (err, response, length) => {
        const errorCode = RequestExecution._getErrorCode(err);

        this._trackResponse(process.hrtime(this._startTime), errorCode, err, length);

        if (this._cancelled) {
          // Avoid handling the response / err
          return;
        }

        if (errorCode !== errorCodes.none) {
          return this._handleError(errorCode, err);
        }

        if (response.schemaChange) {
          return promiseUtils.toBackground(
            this._parent.client
              .handleSchemaAgreementAndRefresh(this._connection, response.schemaChange)
              .then(agreement => {
                if (this._cancelled) {
                  // After the schema agreement method was started, this execution was cancelled
                  return;
                }

                this._parent.setCompleted(null, this._getResultSet(response, agreement));
              })
          );
        }

        if (response.keyspaceSet) {
          this._parent.client.keyspace = response.keyspaceSet;
        }

        if (response.meta && response.meta.newResultId && this._request.queryId) {
          // Update the resultId on the existing prepared statement.
          // Eventually would want to update the result metadata as well (NODEJS-433)
          const info = this._parent.client.metadata.getPreparedById(this._request.queryId);
          info.meta.resultId = response.meta.newResultId;
        }

        this._parent.setCompleted(null, this._getResultSet(response));
      });
  }

  _trackResponse(latency, errorCode, err, length) {
    // Record metrics
    RequestExecution._invokeMetricsHandler(errorCode, this._parent.client.metrics, err, latency);

    // Request tracker
    const tracker = this._parent.client.options.requestTracker;

    if (tracker === null) {
      return;
    }

    // Avoid using instanceof as property check is faster
    const query = this._request.query || this._request.queries;
    const parameters = this._request.params;
    const requestLength = this._request.length;

    if (err) {
      tracker.onError(this._host, query, parameters, this._parent.executionOptions, requestLength, err, latency);
    } else {
      tracker.onSuccess(this._host, query, parameters, this._parent.executionOptions, requestLength, length, latency);
    }
  }

  _getResultSet(response, agreement) {
    const rs = new types.ResultSet(response, this._host.address, this._parent.triedHosts, this._parent.speculativeExecutions,
      this._request.consistency, agreement === undefined || agreement);

    if (rs.rawPageState) {
      rs.nextPageAsync = this._parent.getNextPageHandler();
    }

    return rs;
  }

  /**
   * Gets the method of the {ClientMetrics} instance depending on the error code and invokes it.
   * @param {Number} errorCode
   * @param {ClientMetrics} metrics
   * @param {Error} err
   * @param {Array} latency
   * @private
   */
  static _invokeMetricsHandler(errorCode, metrics, err, latency) {
    const handler = metricsHandlers.get(errorCode);
    if (handler !== undefined) {
      handler(metrics, err, latency);
    }

    if (!err || err instanceof errors.ResponseError) {
      metrics.onResponse(latency);
    }
  }

  /**
   * Gets the method of the {ClientMetrics} instance related to retry depending on the error code and invokes it.
   * @param {Number} errorCode
   * @param {ClientMetrics} metrics
   * @param {Error} err
   * @private
   */
  static _invokeMetricsHandlerForRetry(errorCode, metrics, err) {
    const handler = metricsRetryHandlers.get(errorCode);

    if (handler !== undefined) {
      handler(metrics, err);
    }
  }

  /**
   * Allows the handler to cancel the current request.
   * When the request has been already written, we can unset the callback and forget about it.
   */
  cancel() {
    this._cancelled = true;

    if (this._operation === null) {
      return;
    }

    this._operation.cancel();
  }

  /**
   * Determines if the current execution was cancelled.
   */
  wasCancelled() {
    return this._cancelled;
  }

  _handleError(errorCode, err) {
    this._parent.triedHosts[this._host.address] = err;
    err['coordinator'] = this._host.address;

    if (errorCode === errorCodes.serverErrorUnprepared) {
      return this._prepareAndRetry(err.queryId);
    }

    if (errorCode === errorCodes.socketError || errorCode === errorCodes.socketErrorBeforeRequestWritten) {
      this._host.removeFromPool(this._connection);
    } else if (errorCode === errorCodes.clientTimeout) {
      this._parent.log('warning', err.message);
      this._host.checkHealth(this._connection);
    }

    const decisionInfo = this._getDecision(errorCode, err);

    if (!decisionInfo || decisionInfo.decision === retry.RetryPolicy.retryDecision.rethrow) {
      if (this._request instanceof requests.QueryRequest || this._request instanceof requests.ExecuteRequest) {
        err['query'] = this._request.query;
      }
      return this._parent.setCompleted(err);
    }

    const metrics = this._parent.client.metrics;

    if (decisionInfo.decision === retry.RetryPolicy.retryDecision.ignore) {
      metrics.onIgnoreError(err);

      // Return an empty ResultSet
      return this._parent.setCompleted(null, this._getResultSet(utils.emptyObject));
    }

    RequestExecution._invokeMetricsHandlerForRetry(errorCode, metrics, err);

    return this._retry(decisionInfo.consistency, decisionInfo.useCurrentHost);
  }

  /**
   * Gets a decision whether or not to retry based on the error information.
   * @param {Number} errorCode
   * @param {Error} err
   * @returns {{decision, useCurrentHost, consistency}}
   */
  _getDecision(errorCode, err) {
    const operationInfo = {
      query: this._request && this._request.query,
      executionOptions: this._parent.executionOptions,
      nbRetry: this._retryCount
    };

    const retryPolicy = operationInfo.executionOptions.getRetryPolicy();

    switch (errorCode) {
      case errorCodes.socketErrorBeforeRequestWritten:
        // The request was definitely not applied, it's safe to retry.
        // Retry on the current host as there might be other connections open, in case it fails to obtain a connection
        // on the current host, the driver will immediately retry on the next host.
        return retryOnCurrentHost;
      case errorCodes.socketError:
      case errorCodes.clientTimeout:
      case errorCodes.serverErrorOverloaded:
        if (operationInfo.executionOptions.isIdempotent()) {
          return retryPolicy.onRequestError(operationInfo, this._request.consistency, err);
        }
        return rethrowDecision;
      case errorCodes.serverErrorUnavailable:
        return retryPolicy.onUnavailable(operationInfo, err.consistencies, err.required, err.alive);
      case errorCodes.serverErrorReadTimeout:
        return retryPolicy.onReadTimeout(
          operationInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
      case errorCodes.serverErrorWriteTimeout:
        if (operationInfo.executionOptions.isIdempotent()) {
          return retryPolicy.onWriteTimeout(
            operationInfo, err.consistencies, err.received, err.blockFor, err.writeType);
        }
        return rethrowDecision;
      default:
        return rethrowDecision;
    }
  }

  static _getErrorCode(err) {
    if (!err) {
      return errorCodes.none;
    }

    if (err.isSocketError) {
      if (err.requestNotWritten) {
        return errorCodes.socketErrorBeforeRequestWritten;
      }
      return errorCodes.socketError;
    }

    if (err instanceof errors.OperationTimedOutError) {
      return errorCodes.clientTimeout;
    }

    if (err instanceof errors.ResponseError) {
      switch (err.code) {
        case types.responseErrorCodes.overloaded:
        case types.responseErrorCodes.isBootstrapping:
        case types.responseErrorCodes.truncateError:
          return errorCodes.serverErrorOverloaded;
        case types.responseErrorCodes.unavailableException:
          return errorCodes.serverErrorUnavailable;
        case types.responseErrorCodes.readTimeout:
          return errorCodes.serverErrorReadTimeout;
        case types.responseErrorCodes.writeTimeout:
          return errorCodes.serverErrorWriteTimeout;
        case types.responseErrorCodes.unprepared:
          return errorCodes.serverErrorUnprepared;
      }
    }

    return errorCodes.serverErrorOther;
  }

  /**
   * @param {Number|undefined} consistency
   * @param {Boolean} useCurrentHost
   * @param {Object} [meta]
   * @private
   */
  _retry(consistency, useCurrentHost, meta) {
    if (this._cancelled) {
      // No point in retrying
      return;
    }

    this._parent.log('info', 'Retrying request');
    this._retryCount++;

    if (meta || (typeof consistency === 'number' && this._request.consistency !== consistency)) {
      this._request = this._request.clone();
      if (typeof consistency === 'number') {
        this._request.consistency = consistency;
      }
      // possible that we are retrying because we had to reprepare.  In this case it is also possible
      // that our known metadata had changed, therefore we update it on the request.
      if (meta) {
        this._request.meta = meta;
      }
    }

    if (useCurrentHost !== false) {
      // Reusing the existing connection is suitable for the most common scenarios, like server read timeouts that
      // will be fixed with a new request.
      // To cover all scenarios (e.g., where a different connection to the same host might mean something different),
      // we obtain a new connection from the host pool.
      // When there was a socket error, the connection provided was already removed from the pool earlier.
      try {
        this._connection = this._host.borrowConnection(this._connection);
      } catch (err) {
        // All connections are busy (`BusyConnectionError`) or there isn't a ready connection in the pool (`Error`)
        // The retry policy declared the intention to retry on the current host but its not available anymore.
        // Use the next host
        return promiseUtils.toBackground(this.restart());
      }

      return this._sendOnConnection();
    }

    // Use the next host in the query plan to send the request in the background
    promiseUtils.toBackground(this.restart());
  }

  /**
   * Issues a PREPARE request on the current connection.
   * If there's a socket or timeout issue, it moves to next host and executes the original request.
   * @param {Buffer} queryId
   * @private
   */
  _prepareAndRetry(queryId) {
    const connection = this._connection;

    this._parent.log('info',
      `Query 0x${queryId.toString('hex')} not prepared on` +
      ` host ${connection.endpointFriendlyName}, preparing and retrying`);

    const info = this._parent.client.metadata.getPreparedById(queryId);

    if (!info) {
      return this._parent.setCompleted(new errors.DriverInternalError(
        `Unprepared response invalid, id: 0x${queryId.toString('hex')}`));
    }

    const version = this._connection.protocolVersion;

    if (!types.protocolVersion.supportsKeyspaceInRequest(version) && info.keyspace && info.keyspace !== connection.keyspace) {
      return this._parent.setCompleted(
        new Error(`Query was prepared on keyspace ${info.keyspace}, can't execute it on ${connection.keyspace} (${info.query})`));
    }

    const self = this;
    this._connection.prepareOnce(info.query, info.keyspace, function (err, result) {
      if (err) {
        if (!err.isSocketError && err instanceof errors.OperationTimedOutError) {
          self._parent.log('warning',
            `Unexpected timeout error when re-preparing query on host ${connection.endpointFriendlyName}`);
        }

        // There was a failure re-preparing on this connection.
        // Execute the original request on the next connection and forget about the PREPARE-UNPREPARE flow.
        return self._retry(undefined, false);
      }

      // It's possible that when re-preparing we got new metadata (i.e. if schema changed), update cache.
      info.meta = result.meta;
      // pass the metadata so it can be used in retry.
      self._retry(undefined, true, result.meta);
    });
  }
}

module.exports = RequestExecution;