/** @module policies/retry */
/**
 * Base and default RetryPolicy.
 * Determines what to do when the drivers runs into an specific Cassandra exception
 * @constructor
 */
function RetryPolicy() {

}

/**
 * Determines what to do when the drivers gets an UnavailableException from a Cassandra node
 * @param {{request: object, handler: RequestHandler, nbRetry: Number}} requestInfo
 * @param consistency
 * @param {Number} required
 * @param {Number} alive
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onUnavailable = function (requestInfo, consistency, required, alive) {
  return this.rethrowResult();
};

/**
 * Determines what to do when the drivers gets an ReadTimeoutException from a Cassandra node
 * @param {{request: object, handler: RequestHandler, nbRetry: Number}} requestInfo
 * @param consistency
 * @param {Number} received
 * @param blockFor
 * @param {Boolean} isDataPresent
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onReadTimeout = function (requestInfo, consistency, received, blockFor, isDataPresent) {
  if (requestInfo.nbRetry > 0) {
    return this.rethrowResult();
  }
  return ((received >= blockFor && !this.isDataPresent) ?
    this.retryResult() :
    this.rethrowResult());
};

/**
 * Determines what to do when the drivers gets an WriteTimeoutException from a Cassandra node
 * @param {{request: object, handler: RequestHandler, nbRetry: Number}} requestInfo
 * @param consistency
 * @param {Number} received
 * @param blockFor
 * @param {String} writeType
 * @returns {DecisionInfo}
 */
RetryPolicy.prototype.onWriteTimeout = function (requestInfo, consistency, received, blockFor, writeType) {
  if (requestInfo.nbRetry > 0) {
    return this.rethrowResult();
  }
  // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidates
  return writeType == "BATCH_LOG" ? this.retryResult() : this.rethrowResult();
};

/**
 * @returns {{decision: Number}}
 */
RetryPolicy.prototype.retryResult = function () {
  return {decision: RetryPolicy.retryDecision.retry};
};

/**
 * @returns {{decision: Number}}
 */
RetryPolicy.prototype.rethrowResult = function () {
  return {decision: RetryPolicy.retryDecision.rethrow};
};

/**
 * Retry decision of the retry policy.
 * @type {{rethrow: number, retry: number}}
 */
RetryPolicy.retryDecision = {
  rethrow:  0,
  retry:    1,
  ignore:   2
};


/**
 * Decision information
 * @typedef {Object} DecisionInfo
 * @property {Number} decision
 * @property {Number} consistency
 */
exports.RetryPolicy = RetryPolicy;