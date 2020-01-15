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
const util = require('util');


/** @module policies/retry */
/**
 * Base and default RetryPolicy.
 * Determines what to do when the drivers runs into an specific Cassandra exception
 * @constructor
 */
function RetryPolicy() {

}

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
 * @deprecated Since version 4.0 non-idempotent operations are never tried for write timeout or request error, use the
 * default retry policy instead.
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
  if (info.executionOptions.isIdempotent()) {
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
  if (info.executionOptions.isIdempotent()) {
    return this._childPolicy.onWriteTimeout(info, consistency, received, blockFor, writeType);
  }
  return this.rethrowResult();
};

/**
 * Creates a new instance of FallthroughRetryPolicy.
 * @classdesc
 * A retry policy that never retries nor ignores.
 * <p>
 * All of the methods of this retry policy unconditionally return
 * [rethrow]{@link module:policies/retry~Retry#rethrowResult()}. If this policy is used, retry logic will have to be
 * implemented in business code.
 * </p>
 * @alias module:policies/retry~FallthroughRetryPolicy
 * @extends RetryPolicy
 * @constructor
 */
function FallthroughRetryPolicy() {

}

util.inherits(FallthroughRetryPolicy, RetryPolicy);

/**
 * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
 */
FallthroughRetryPolicy.prototype.onReadTimeout = function () {
  return this.rethrowResult();
};

/**
 * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
 */
FallthroughRetryPolicy.prototype.onRequestError = function () {
  return this.rethrowResult();
};

/**
 * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
 */
FallthroughRetryPolicy.prototype.onUnavailable = function () {
  return this.rethrowResult();
};

/**
 * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
 */
FallthroughRetryPolicy.prototype.onWriteTimeout = function () {
  return this.rethrowResult();
};

/**
 * Decision information
 * @typedef {Object} DecisionInfo
 * @property {Number} decision The decision as specified in
 * [retryDecision]{@link module:policies/retry~RetryPolicy.retryDecision}.
 * @property {Number} [consistency] The [consistency level]{@link module:types~consistencies}.
 * @property {useCurrentHost} [useCurrentHost] Determines if it should use the same host to retry the request.
 * <p>
 *   In the case that the current host is not available anymore, it will be retried on the next host even when
 *   <code>useCurrentHost</code> is set to <code>true</code>.
 * </p>
 */

/**
 * Information of the execution to be used to determine whether the operation should be retried.
 * @typedef {Object} OperationInfo
 * @property {String} query The query that was executed.
 * @param {ExecutionOptions} executionOptions The options related to the execution of the request.
 * @property {Number} nbRetry The number of retries already performed for this operation.
 */

exports.IdempotenceAwareRetryPolicy = IdempotenceAwareRetryPolicy;
exports.FallthroughRetryPolicy = FallthroughRetryPolicy;
exports.RetryPolicy = RetryPolicy;