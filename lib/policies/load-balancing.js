/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var cassandraLoadBalancing = cassandra.policies.loadBalancing;

// Export parent module properties.
var parentKeys = Object.keys(cassandraLoadBalancing);
parentKeys.forEach(function (key) {
  exports[key] = cassandraLoadBalancing[key];
});

/**
 * Creates a new instance of {@link DseLoadBalancingPolicy}.
 * @classdesc
 * A load balancing policy designed to run against DSE cluster that provides token and datacenter awareness.
 * <p>
 *   For most executions, the query plan is determined by the token and the datacenter considered local.
 *   For graph analytics queries, it uses the preferred analytics graph server previously obtained by driver as first
 *   host in the query plan.
 * </p>
 * <p>
 *   You can use this policy to wrap a custom load-balancing policy using
 *   [DseLoadBalancingPolicy.createAsWrapper(childPolicy)]{@link
  *   module:policies/loadBalancing~DseLoadBalancingPolicy.createAsWrapper} static method.
 * </p>
 * @param {?String} [localDc] The local datacenter name.
 * @param {Number} [usedHostPerRemoteDc] The number of host per remote datacenter that the policy will yield in a
 * newQueryPlan after the local nodes.
 * @alias module:policies/loadBalancing~DseLoadBalancingPolicy
 * @constructor
 */
function DseLoadBalancingPolicy(localDc, usedHostPerRemoteDc) {
  if (!localDc || typeof localDc === 'string') {
    this._childPolicy = new cassandraLoadBalancing.TokenAwarePolicy(
      new cassandraLoadBalancing.DCAwareRoundRobinPolicy(localDc, usedHostPerRemoteDc))
  }
  else if (localDc instanceof cassandraLoadBalancing.LoadBalancingPolicy) {
    this._childPolicy = localDc;
  }
  else {
    throw new Error("localDc type is invalid");
  }
  this._preferredHost = null;
}

/**
 * Creates a new instance of <code>DseLoadBalancingPolicy</code> wrapping the provided child policy.
 * @param {LoadBalancingPolicy} childPolicy The child load balancing policy to be used.
 * @returns {DseLoadBalancingPolicy} A new <code>DseLoadBalancingPolicy</code> instance.
 */
DseLoadBalancingPolicy.createAsWrapper = function (childPolicy) {
  if (!(childPolicy instanceof cassandraLoadBalancing.LoadBalancingPolicy)) {
    throw new Error('childPolicy must be instance of LoadBalancingPolicy type');
  }
  return new DseLoadBalancingPolicy(childPolicy);
};

util.inherits(DseLoadBalancingPolicy, cassandraLoadBalancing.LoadBalancingPolicy);

/**
 * Uses the child policy to return the distance to the host.
 * @param {Host} host
 * @override
 */
DseLoadBalancingPolicy.prototype.getDistance = function (host) {
  if (host === this._preferredHost) {
    // Set the last preferred host as local.
    // It's somewhat "hacky" but ensures that the pool for the graph analytics host has the appropriate size
    return cassandra.types.distance.local;
  }
  return this._childPolicy.getDistance(host);
};

/**
 * Initializes the load balancing policy.
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 * @override
 */
DseLoadBalancingPolicy.prototype.init = function (client, hosts, callback) {
  this._childPolicy.init(client, hosts, callback);
};


/**
 * Returns the hosts to used for a query.
 * @override
 */
DseLoadBalancingPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var self = this;
  this._childPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    if (err) {
      return callback(err);
    }
    if (queryOptions && queryOptions.preferredHost) {
      self._preferredHost = queryOptions.preferredHost;
      return callback(null, self._setFirst(queryOptions.preferredHost, iterator));
    }
    callback(null, iterator);
  });
};

/**
 * Sets the given host as the first item in the iterator.
 * @param {Host|*} preferredHost
 * @param {{next: function}} iterator
 * @returns {{next: function}}
 * @private
 */
DseLoadBalancingPolicy.prototype._setFirst = function (preferredHost, iterator) {
  var first = preferredHost;
  return {
    next: function () {
      if (first) {
        var preferredHost = first;
        first = null;
        return { value: preferredHost, done: false };
      }
      return iterator.next();
    }
  }
};

exports.DseLoadBalancingPolicy = DseLoadBalancingPolicy;