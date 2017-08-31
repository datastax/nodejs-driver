'use strict';

/**
 * Contains driver tuning policies to determine [load balancing]{@link module:policies/loadBalancing},
 *  [retrying]{@link module:policies/retry} queries, [reconnecting]{@link module:policies/reconnection} to a node,
 *  [address resolution]{@link module:policies/addressResolution},
 *  [timestamp generation]{@link module:policies/timestampGeneration} and
 *  [speculative execution]{@link module:policies/speculativeExecution}.
 * @module policies
 */
var addressResolution = exports.addressResolution = require('./address-resolution');
var loadBalancing = exports.loadBalancing = require('./load-balancing');
var reconnection = exports.reconnection = require('./reconnection');
var retry = exports.retry = require('./retry');
var speculativeExecution = exports.speculativeExecution = require('./speculative-execution');
var timestampGeneration = exports.timestampGeneration = require('./timestamp-generation');

/**
 * Returns a new instance of the default address translator policy used by the driver.
 * @returns {AddressTranslator}
 */
exports.defaultAddressTranslator = function () {
  return new addressResolution.AddressTranslator();
};

/**
 * Returns a new instance of the default load-balancing policy used by the driver.
 * @returns {LoadBalancingPolicy}
 */
exports.defaultLoadBalancingPolicy = function () {
  return new loadBalancing.TokenAwarePolicy(new loadBalancing.DCAwareRoundRobinPolicy());
};

/**
 * Returns a new instance of the default retry policy used by the driver.
 * @returns {RetryPolicy}
 */
exports.defaultRetryPolicy = function () {
  return new retry.RetryPolicy();
};

/**
 * Returns a new instance of the default reconnection policy used by the driver.
 * @returns {ReconnectionPolicy}
 */
exports.defaultReconnectionPolicy = function () {
  return new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false);
};


/**
 * Returns a new instance of the default speculative execution policy used by the driver.
 * @returns {SpeculativeExecutionPolicy}
 */
exports.defaultSpeculativeExecutionPolicy = function () {
  return new speculativeExecution.NoSpeculativeExecutionPolicy();
};

/**
 * Returns a new instance of the default timestamp generator used by the driver.
 * @returns {TimestampGenerator}
 */
exports.defaultTimestampGenerator = function () {
  return new timestampGeneration.MonotonicTimestampGenerator();
};