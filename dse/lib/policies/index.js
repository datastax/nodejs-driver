/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var cassandraPolicies = require('cassandra-driver').policies;
/**
 * Contains driver tuning policies to determine load balancing, retrying queries, reconnecting to a node and address
 * resolution.
 * <p>
 *   It contains all the [policies defined in the Cassandra driver]{@link
  *   http://docs.datastax.com/en/latest-nodejs-driver-api/module-policies.html} and additional DSE-specific policies.
 * </p>
 * @module policies
 */
var addressResolution = exports.addressResolution = cassandraPolicies.addressResolution;
var loadBalancing = exports.loadBalancing = require('./load-balancing');
var reconnection = exports.reconnection = cassandraPolicies.reconnection;
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
  return new loadBalancing.DseLoadBalancingPolicy();
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