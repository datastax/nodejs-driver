/**
 * Contains driver tuning policies to determine [load balancing]{@link module:policies/loadBalancing},
 *  [retrying]{@link module:policies/retry} queries, [reconnecting]{@link module:policies/reconnection} to a node
 *  and [address resolution]{@link module:policies/addressResolution}.
 * <ul>
 *   <li>[policies/addressResolution]{@link module:policies/addressResolution}</li>
 *   <li>[policies/loadBalancing]{@link module:policies/loadBalancing}</li>
 *   <li>[policies/reconnection]{@link module:policies/reconnection}</li>
 *   <li>[policies/retry]{@link module:policies/retry}</li>
 * </ul>
 * @module policies
 */
var addressResolution = exports.addressResolution = require('./address-resolution');
var loadBalancing = exports.loadBalancing = require('./load-balancing');
var reconnection = exports.reconnection = require('./reconnection');
var retry = exports.retry = require('./retry');

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