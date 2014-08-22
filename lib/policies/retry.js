/**
 * Base and default RetryPolicy.
 * Determines what to do when the drivers runs into an specific Cassandra exception
 * @constructor
 */
function RetryPolicy() {

}

/**
 * Determines what to do when the drivers gets an UnavailableException from a Cassandra node
 * @param {{request: object, handler: RequestHandler}} requestInfo
 * @param consistency
 * @param {Number} required
 * @param {Number} alive
 * @returns {{decision: number, [consistency]: number}}
 */
RetryPolicy.prototype.onUnavailable = function (requestInfo, consistency, required, alive) {
  return {decision: RetryPolicy.retryDecision.rethrow};
};

/**
 * Determines what to do when the drivers gets an ReadTimeoutException from a Cassandra node
 * @param {{request: object, handler: RequestHandler}} requestInfo
 * @param consistency
 * @param {Number} received
 * @param blockFor
 * @param {Boolean} isDataPresent
 * @returns {{decision: number, [consistency]: number}}
 */
RetryPolicy.prototype.onReadTimeout = function (requestInfo, consistency, received, blockFor, isDataPresent) {
  return {decision: RetryPolicy.retryDecision.rethrow};
};

/**
 * Determines what to do when the drivers gets an WriteTimeoutException from a Cassandra node
 * @param {{request: object, handler: RequestHandler}} requestInfo
 * @param consistency
 * @param {Number} received
 * @param blockFor
 * @param {String} writeType
 * @returns {{decision: number}}
 */
RetryPolicy.prototype.onWriteTimeout = function (requestInfo, consistency, received, blockFor, writeType) {
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

exports.RetryPolicy = RetryPolicy;