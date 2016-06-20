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
exports.addressResolution = cassandraPolicies.addressResolution;
exports.loadBalancing = require('./load-balancing');
exports.reconnection = cassandraPolicies.reconnection;
exports.retry = cassandraPolicies.retry;