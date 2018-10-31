'use strict';

const util = require('util');
const errors = require('./errors');
const requests = require('./requests');
const retry = require('./policies/retry');
const types = require('./types');
const utils = require('./utils');

const retryOnCurrentHost = Object.freeze({
  decision: retry.RetryPolicy.retryDecision.retry,
  useCurrentHost: true,
  consistency: undefined
});

class RequestExecution {
  /**
   * Encapsulates a single flow of execution against a coordinator, handling individual retries and failover.
   * @param {RequestHandler} parent
   */
  constructor(parent) {
    this._parent = parent;
    /** @type {OperationState} */
    this._operation = null;
    this._host = null;
    this._cancelled = false;
    this._retryCount = 0;
    // The streamId information is not included in the request.
    // A pointer to the parent request can be used, except when changing the consistency level from the retry policy
    this._request = this._parent.request;
  }

  /**
   * Starts the execution by borrowing the next connection available using the query plan.
   * It invokes the callback when a connection is acquired, if any.
   * @param {Function} [getHostCallback] Callback to be invoked when a connection to a host was successfully acquired.
   */
  start(getHostCallback) {
    const self = this;
    getHostCallback = getHostCallback || utils.noop;
    this._parent.getNextConnection(function nextConnectionCallback(err, connection, host) {
      if (self._cancelled) {
        // No need to send the request or invoke any callback
        return;
      }
      if (err) {
        return self._parent.handleNoHostAvailable(err, self);
      }
      self._connection = connection;
      self._host = host;
      getHostCallback(host);
      if (self._retryCount === 0) {
        self._parent.speculativeExecutions++;
      }
      self._sendOnConnection();
    });
  }

  _sendOnConnection() {
    const self = this;
    this._operation =
      this._connection.sendStream(this._request, this._parent.requestOptions, function responseCb(err, response) {
        if (self._cancelled) {
          // Avoid handling the response / err
          return;
        }
        if (err) {
          return self._handleError(err);
        }
        const result = self._getResultSet(response);
        if (response.schemaChange) {
          return self._parent.client.handleSchemaAgreementAndRefresh(
            self._connection, response.schemaChange, function schemaCb(){
              if (self._cancelled) {
                // After the schema agreement method was started, this execution was cancelled
                return;
              }
              self._parent.setCompleted(null, result);
            });
        }
        if (response.keyspaceSet) {
          self._parent.client.keyspace = response.keyspaceSet;
        }
        self._parent.setCompleted(null, result);
      });
  }

  _getResultSet(response) {
    return new types.ResultSet(response, this._host.address, this._parent.triedHosts, this._parent.speculativeExecutions,
      this._request.consistency);
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

  _handleError(err) {
    this._parent.triedHosts[this._host.address] = err;
    err['coordinator'] = this._host.address;
    if (err.code === types.responseErrorCodes.unprepared && (err instanceof errors.ResponseError)) {
      return this._prepareAndRetry(err.queryId);
    }
    const decisionInfo = this._getDecision(err);
    if (err.isSocketError) {
      this._host.removeFromPool(this._connection);
    }
    if (!decisionInfo || decisionInfo.decision === retry.RetryPolicy.retryDecision.rethrow) {
      if (this._request instanceof requests.QueryRequest || this._request instanceof requests.ExecuteRequest) {
        err['query'] = this._request.query;
      }
      return this._parent.setCompleted(err);
    }
    if (decisionInfo.decision === retry.RetryPolicy.retryDecision.ignore) {
      // Return an empty ResultSet
      return this._parent.setCompleted(null, this._getResultSet(utils.emptyObject));
    }
    return this._retry(decisionInfo.consistency, decisionInfo.useCurrentHost);
  }

  /**
   * Gets a decision whether or not to retry based on the error information.
   * @param {Error} err
   * @returns {{decision, useCurrentHost, consistency}}
   */
  _getDecision(err) {
    const operationInfo = {
      query: this._request && this._request.query,
      options: this._parent.requestOptions,
      nbRetry: this._retryCount,
      // handler, request and retryOnTimeout properties are deprecated and should be removed in the next major version
      handler: this,
      request: this._request,
      retryOnTimeout: false
    };
    const self = this;
    function onRequestError() {
      return self._parent.retryPolicy.onRequestError(operationInfo, self._request.consistency, err);
    }
    if (err.isSocketError) {
      if (err.requestNotWritten) {
        // The request was definitely not applied, it's safe to retry.
        // Retry on the current host as there might be other connections open, in case it fails to obtain a connection
        // on the current host, the driver will immediately retry on the next host.
        return retryOnCurrentHost;
      }
      return onRequestError();
    }
    if (err instanceof errors.OperationTimedOutError) {
      this._parent.log('warning', err.message);
      this._host.checkHealth(this._connection);
      operationInfo.retryOnTimeout = this._parent.requestOptions.retryOnTimeout !== false;
      return onRequestError();
    }
    if (err instanceof errors.ResponseError) {
      switch (err.code) {
        case types.responseErrorCodes.overloaded:
        case types.responseErrorCodes.isBootstrapping:
        case types.responseErrorCodes.truncateError:
          return onRequestError();
        case types.responseErrorCodes.unavailableException:
          return this._parent.retryPolicy.onUnavailable(operationInfo, err.consistencies, err.required, err.alive);
        case types.responseErrorCodes.readTimeout:
          return this._parent.retryPolicy.onReadTimeout(
            operationInfo, err.consistencies, err.received, err.blockFor, err.isDataPresent);
        case types.responseErrorCodes.writeTimeout:
          return this._parent.retryPolicy.onWriteTimeout(
            operationInfo, err.consistencies, err.received, err.blockFor, err.writeType);
      }
    }
    return { decision: retry.RetryPolicy.retryDecision.rethrow };
  }

  /**
   * @param {Number|undefined} consistency
   * @param {Boolean} useCurrentHost
   * @private
   */
  _retry(consistency, useCurrentHost) {
    if (this._cancelled) {
      // No point in retrying
      return;
    }

    this._parent.log('info', 'Retrying request');
    this._retryCount++;

    if (typeof consistency === 'number' && this._request.consistency !== consistency) {
      this._request = this._request.clone();
      this._request.consistency = consistency;
    }

    if (useCurrentHost !== false) {
      // Use existing host (default).
      const keyspace = this._parent.client.keyspace;
      // Reusing the existing connection is suitable for the most common scenarios, like server read timeouts that
      // will be fixed with a new request.
      // To cover all scenarios (e.g., where a different connection to the same host might mean something different),
      // we obtain a new connection from the host pool.
      // When there was a socket error, the connection provided was already removed from the pool earlier.
      return this._host.borrowConnection(keyspace, this._connection, (err, connection) => {
        if (err) {
          // All connections are busy (`BusyConnectionError`) or there isn't a ready connection in the pool (`Error`)
          // The retry policy declared the intention to retry on the current host but its not available anymore.
          // Use the next host
          return this.start();
        }

        this._connection = connection;
        this._sendOnConnection();
      });
    }

    // Use the next host in the query plan to send the request
    this.start();
  }

  /**
   * Issues a PREPARE request on the current connection.
   * If there's a socket or timeout issue, it moves to next host and executes the original request.
   * @param {Buffer} queryId
   * @private
   */
  _prepareAndRetry(queryId) {
    this._parent.log('info', util.format('Query 0x%s not prepared on host %s, preparing and retrying',
      queryId.toString('hex'), this._host.address));
    const info = this._parent.client.metadata.getPreparedById(queryId);
    if (!info) {
      return this._parent.setCompleted(
        new errors.DriverInternalError(util.format('Unprepared response invalid, id: %s', queryId.toString('hex'))));
    }
    if (info.keyspace && info.keyspace !== this._connection.keyspace) {
      return this._parent.setCompleted(
        new Error(util.format('Query was prepared on keyspace %s, can\'t execute it on %s (%s)',
          info.keyspace, this._connection.keyspace, info.query)));
    }
    const self = this;
    this._connection.prepareOnce(info.query, function (err) {
      if (err) {
        if (!err.isSocketError && err instanceof errors.OperationTimedOutError) {
          self._parent.log('warning', util.format('Unexpected error when re-preparing query on host %s'));
        }
        // There was a failure re-preparing on this connection.
        // Execute the original request on the next connection and forget about the PREPARE-UNPREPARE flow.
        return self._retry(undefined, false);
      }
      self._retry(undefined, true);
    });
  }
}

module.exports = RequestExecution;