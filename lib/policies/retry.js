/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var cassandraRetry = cassandra.policies.retry;
var baseRetryPolicy = cassandraRetry.RetryPolicy;

// Export parent module properties.
var parentKeys = Object.keys(cassandraRetry);
parentKeys.forEach(function (key) {
  exports[key] = cassandraRetry[key];
});

/**
 */

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

util.inherits(FallthroughRetryPolicy, baseRetryPolicy);

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

exports.FallthroughRetryPolicy = FallthroughRetryPolicy;