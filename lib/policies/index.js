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
  *   http://docs.datastax.com/en/drivers/nodejs/3.0/module-policies.html} and additional DSE-specific policies.
 * </p>
 * @module policies
 */
/**
 * Address resolution module, containing all the [policies defined in the Cassandra driver]{@link
  *  http://docs.datastax.com/en/drivers/nodejs/3.0/module-policies_addressResolution.html }.
 * @module policies/addressResolution
 */
exports.addressResolution = cassandraPolicies.addressResolution;
exports.loadBalancing = require('./load-balancing');
/**
 * Reconnection module, containing all the [policies defined in the Cassandra driver]{@link
  *  http://docs.datastax.com/en/drivers/nodejs/3.0/module-policies_reconnection.html }.
 * @module policies/reconnection
 */
exports.reconnection = cassandraPolicies.reconnection;
/**
 * Retry module, containing all the [policies defined in the Cassandra driver]{@link
  *  http://docs.datastax.com/en/drivers/nodejs/3.0/module-policies_retry.html }.
 * @module policies/retry
 */
exports.retry = cassandraPolicies.retry;