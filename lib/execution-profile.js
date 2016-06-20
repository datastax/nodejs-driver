/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
"use strict";
var util = require('util');
var utils = require('./utils');
var cassandra = require('cassandra-driver');
var BaseExecuteProfile = cassandra.ExecutionProfile;

/**
 * Creates a new instance of {@link ExecutionProfile}.
 * @classdesc
 * Represents a set of configurations to be used in a statement execution to be used for a single {@link DseClient}
 * instance.
 * <p>
 *   A {@link ExecutionProfile} instance should not be shared across different {@link DseClient} instances.
 * </p>
 * @param {String} name Name of the execution profile.
 * <p>
 *   Use <code>'default'</code> to specify that the new instance should be the default {@link ExecutionProfile} if no
 *   profile is specified in the execution.
 * </p>
 * @param {Object} [options] Profile options, when any of the options is not specified the {@link DseClient} will the
 * default profile
 * @param {Number} [options.consistency] The consistency level to use for this profile.
 * @param {Object} [options.graphOptions]
 * @param {String} [options.graphOptions.name] The graph name to use for graph queries.
 * @param {Number} [options.graphOptions.readConsistency] The consistency level to use for graph read queries.
 * @param {String} [options.graphOptions.source] The graph traversal source name to use for graph queries.
 * @param {Number} [options.graphOptions.writeConsistency] The consistency level to use for graph write queries.
 * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
 * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
 * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
 * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
 * @example
 * const client = new dse.DseClient({
 *   contactPoints: ['host1', 'host2'],
 *   profiles: [
 *     new ExecutionProfile('metrics-oltp', {
 *       consistency: consistency.localQuorum,
 *       retry: myRetryPolicy
 *     })
 *   ]
 * });
 * 
 * client.execute(query, params, { executionProfile: 'metrics-oltp' }, callback);
 * @constructor
 */
function ExecutionProfile(name, options) {
  options = options || utils.emptyObject;
  BaseExecuteProfile.call(this, name, options);
  // jsdoc needs to be duplicated for it to appear in the DSE docs
  /**
   * Name of the execution profile.
   * @type {String}
   * @name ExecutionProfile#name
   */
  /**
   * Consistency level.
   * @type {Number}
   * @name ExecutionProfile#consistency
   */
  /**
   * Load-balancing policy.
   * @type {LoadBalancingPolicy}
   * @name ExecutionProfile#loadBalancing
   */
  /**
   * Client per host read timeout.
   * @type {Number}
   * @name ExecutionProfile#readTimeout
   */
  /**
   * Retry policy.
   * @type {RetryPolicy}
   * @name ExecutionProfile#retry
   */
  /**
   * Serial consistency level.
   * @type {Number}
   * @name ExecutionProfile#serialConsistency
   */
  options.graphOptions = options.graphOptions || utils.emptyObject;
  /**
   * The graph options for this profile.
   * @type {Object}
   * @property {String} name The graph name.
   * @property {String} readConsistency The consistency to use for graph write queries.
   * @property {String} source The graph traversal source.
   * @property {String} writeConsistency The consistency to use for graph write queries.
   */
  this.graphOptions = {
    name: options.graphOptions.name,
    readConsistency: options.graphOptions.readConsistency,
    source: options.graphOptions.source,
    writeConsistency: options.graphOptions.writeConsistency
  };
}

module.exports = ExecutionProfile;
