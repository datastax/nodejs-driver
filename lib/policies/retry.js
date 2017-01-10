"use strict";
var util = require('util');

var errors = require('../errors');

/** @module policies/retry */
/**
 * Base and default RetryPolicy.
 * Determines what to do when the drivers runs into an specific Cassandra exception
 * @constructor
 */
function RetryPolicy() {

}

//noinspection JSUnusedLocalSymbols
/**
 * Determines what to do when the driver gets an UnavailableException response from a Cassandra node.
 * @param {OperationInfo} info
 * @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
 * the exception.
 * @param {Number} required The number of replicas whose response is required to achieve the
 * required [consistency]{@link module:types~consistencies}.
 * @param {Number} alive The number of replicas that were known to be alive when the request had been processed
 * (since an unavailable exception has been triggered, there will be alive &lt; required)
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onUnavailable = function (info, consistency, required, alive) {
  if (info.nbRetry > 0) {
    return this.rethrowResult();
  }
  return this.retryResult(undefined, false);
};

/**
 * Determines what to do when the driver gets a ReadTimeoutException response from a Cassandra node.
 * @param {OperationInfo} info
 * @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
 * the exception.
 * @param {Number} received The number of nodes having answered the request.
 * @param {Number} blockFor The number of replicas whose response is required to achieve the
 * required [consistency]{@link module:types~consistencies}.
 * @param {Boolean} isDataPresent When <code>false</code>, it means the replica that was asked for data has not responded.
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onReadTimeout = function (info, consistency, received, blockFor, isDataPresent) {
  if (info.nbRetry > 0) {
    return this.rethrowResult();
  }
  return ((received >= blockFor && !isDataPresent) ?
    this.retryResult() :
    this.rethrowResult());
};

/**
 * Determines what to do when the driver gets a WriteTimeoutException response from a Cassandra node.
 * @param {OperationInfo} info
 * @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
 * the exception.
 * @param {Number} received The number of nodes having acknowledged the request.
 * @param {Number} blockFor The number of replicas whose acknowledgement is required to achieve the required
 * [consistency]{@link module:types~consistencies}.
 * @param {String} writeType A <code>string</code> that describes the type of the write that timed out ("SIMPLE"
 * / "BATCH" / "BATCH_LOG" / "UNLOGGED_BATCH" / "COUNTER").
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onWriteTimeout = function (info, consistency, received, blockFor, writeType) {
  if (info.nbRetry > 0) {
    return this.rethrowResult();
  }
  // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidates
  return writeType === "BATCH_LOG" ? this.retryResult() : this.rethrowResult();
};

/**
 * Defines whether to retry and at which consistency level on an unexpected error.
 * <p>
 * This method might be invoked in the following situations:
 * </p>
 * <ol>
 * <li>On a client timeout, while waiting for the server response
 * (see [socketOptions.readTimeout]{@link ClientOptions}), being the error an instance of
 * [OperationTimedOutError]{@link module:errors~OperationTimedOutError}.</li>
 * <li>On a connection error (socket closed, etc.).</li>
 * <li>When the contacted host replies with an error, such as <code>overloaded</code>, <code>isBootstrapping</code>,
 * </code>serverError, etc. In this case, the error is instance of [ResponseError]{@link module:errors~ResponseError}.
 * </li>
 * </ol>
 * <p>
 * Note that when this method is invoked, <em>the driver cannot guarantee that the mutation has been effectively
 * applied server-side</em>; a retry should only be attempted if the request is known to be idempotent.
 * </p>
 * @param {OperationInfo} info
 * @param {Number|undefined} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
 * the exception.
 * @param {Error} err The error that caused this request to fail.
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onRequestError = function (info, consistency, err) {
  if (err instanceof errors.OperationTimedOutError && !info.options.retryOnTimeout) {
    return this.rethrowResult();
  }
  // The default implementation triggers a retry on the next host in the query plan with the same consistency level,
  // regardless of the statement's idempotence, for historical reasons.
  return this.retryResult(undefined, false);
};

/**
 * Returns a {@link DecisionInfo} to retry the request with the given [consistency]{@link module:types~consistencies}.
 * @param {Number|undefined} [consistency] When specified, it retries the request with the given consistency.
 * @param {Boolean} [useCurrentHost] When specified, determines if the retry should be made using the same coordinator.
 * Default: true.
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.retryResult = function (consistency, useCurrentHost) {
  return {
    decision: RetryPolicy.retryDecision.retry,
    consistency: consistency,
    useCurrentHost: useCurrentHost !== false
  };
};

/**
 * Returns a {@link DecisionInfo} to callback in error when a err is obtained for a given request.
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.rethrowResult = function () {
  return { decision: RetryPolicy.retryDecision.rethrow };
};

/**
 * Determines the retry decision for the retry policies.
 * @type {Object}
 * @property {Number} rethrow
 * @property {Number} retry
 * @property {Number} ignore
 * @static
 */
RetryPolicy.retryDecision = {
  rethrow:  0,
  retry:    1,
  ignore:   2
};

/**
 * Creates a new instance of <code>IdempotenceAwareRetryPolicy</code>.
 * @classdesc
 * A retry policy that avoids retrying non-idempotent statements.
 * <p>
 * In case of write timeouts or unexpected errors, this policy will always return
 * [rethrowResult()]{@link module:policies/retry~RetryPolicy#rethrowResult} if the statement is deemed non-idempotent
 * (see [QueryOptions.isIdempotent]{@link QueryOptions}).
 * <p/>
 * For all other cases, this policy delegates the decision to the child policy.
 * @param {RetryPolicy} [childPolicy] The child retry policy to wrap. When not defined, it will use an instance of
 * [RetryPolicy]{@link module:policies/retry~RetryPolicy} as child policy.
 * @extends module:policies/retry~RetryPolicy
 * @constructor
 */
function IdempotenceAwareRetryPolicy(childPolicy) {
  this._childPolicy = childPolicy || new RetryPolicy();
}

util.inherits(IdempotenceAwareRetryPolicy, RetryPolicy);

IdempotenceAwareRetryPolicy.prototype.onReadTimeout = function (info, consistency, received, blockFor, isDataPresent) {
  return this._childPolicy.onReadTimeout(info, consistency, received, blockFor, isDataPresent);
};

/**
 * If the query is not idempotent, it returns a rethrow decision. Otherwise, it relies on the child policy to decide.
 */
IdempotenceAwareRetryPolicy.prototype.onRequestError = function (info, consistency, err) {
  if (info.options.isIdempotent) {
    return this._childPolicy.onRequestError(info, consistency, err);
  }
  return this.rethrowResult();
};

IdempotenceAwareRetryPolicy.prototype.onUnavailable = function (info, consistency, required, alive) {
  return this._childPolicy.onUnavailable(info, consistency, required, alive);
};

/**
 * If the query is not idempotent, it return a rethrow decision. Otherwise, it relies on the child policy to decide.
 */
IdempotenceAwareRetryPolicy.prototype.onWriteTimeout = function (info, consistency, received, blockFor, writeType) {
  if (info.options.isIdempotent) {
    return this._childPolicy.onWriteTimeout(info, consistency, received, blockFor, writeType);
  }
  return this.rethrowResult();
};

/**
 * Decision information
 * @typedef {Object} DecisionInfo
 * @property {Number} decision The decision as specified in
 * [retryDecision]{@link module:policies/retry~RetryPolicy.retryDecision}.
 * @property {Number} [consistency] The [consistency level]{@link module:types~consistencies}.
 * @property {useCurrentHost} [useCurrentHost] Determines if it should use the same host to retry the request.
 */

/**
 * Request information.
 * @typedef {Object} OperationInfo
 * @property {String} query The query that was executed.
 * @property {QueryOptions} options The options for the query that was executed.
 * @property {Number} nbRetry The number of retries already performed for this operation.
 * @property {Object} handler DEPRECATED: it will be removed in the next major version. The request handler.
 * @property {Object} request DEPRECATED: it will be removed in the next major version. Represents the request that
 * was executed.
 * @property {Boolean} retryOnTimeout. DEPRECATED: it will be removed in the next major version. The value as
 * specified in the {@link QueryOptions} for this operation. Use <code>OperationInfo.options</code> value instead.
 */

exports.RetryPolicy = RetryPolicy;
exports.IdempotenceAwareRetryPolicy = IdempotenceAwareRetryPolicy;