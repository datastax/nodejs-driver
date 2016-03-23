'use strict';
var util = require('util');
var cassandraLoadBalancing = require('cassandra-driver').policies.loadBalancing;

// Export parent module properties.
var parentKeys = Object.keys(cassandraLoadBalancing);
parentKeys.forEach(function (key) {
  exports[key] = cassandraLoadBalancing[key];
});

/**
 * Load-balancing module, containing all the [load-balancing policies defined in the Cassandra driver]{@link
 *  http://docs.datastax.com/en/drivers/nodejs/3.0/module-policies_loadBalancing.html } and the policies defined below.
 * @module policies/loadBalancing
 */

/**
 * Creates a new instance of {@link DseLoadBalancingPolicy}.
 * @classdesc
 * A load balancing policy designed to run against DSE cluster.
 * <p>
 *   For most executions, the query plan will be determined by the child load balancing policy.
 *   Except for some cases, like graph analytics queries, for which it uses the preferred analytics graph server
 *   previously obtained by driver as first host in the query plan.
 * </p>
 * @param {LoadBalancingPolicy} childPolicy The child load balancing policy to be used.
 * @constructor
 */
function DseLoadBalancingPolicy(childPolicy) {
  if (!childPolicy) {
    throw new Error("You must specify a child load balancing policy");
  }
  this.childPolicy = childPolicy;
}

/**
 * Creates a new instance of <code>DseLoadBalancingPolicy</code> using <code>TokenAwarePolicy</code> of
 * <code>DCAwareRoundRobinPolicy</code> as child policy.
 * @param {?String} [localDc] The local datacenter name.
 * @param {Number} [usedHostPerRemoteDc] The number of host per remote datacenter that the policy will yield in a
 * newQueryPlan after the local nodes.
 * @returns {DseLoadBalancingPolicy} A new <code>DseLoadBalancingPolicy</code> instance.
 */
DseLoadBalancingPolicy.createDefault = function (localDc, usedHostPerRemoteDc) {
  return new DseLoadBalancingPolicy(
    new cassandraLoadBalancing.TokenAwarePolicy(
      new cassandraLoadBalancing.DCAwareRoundRobinPolicy(localDc, usedHostPerRemoteDc)));
};

util.inherits(DseLoadBalancingPolicy, cassandraLoadBalancing.LoadBalancingPolicy);

/**
 * Uses the child policy to return the distance to the host.
 * @param {Host} host
 * @override
 */
DseLoadBalancingPolicy.prototype.getDistance = function (host) {
  return this.childPolicy.getDistance(host);
};

/**
 * Initializes the load balancing policy.
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 * @override
 */
DseLoadBalancingPolicy.prototype.init = function (client, hosts, callback) {
  this.childPolicy.init(client, hosts, callback);
};


/**
 * Returns the hosts to used for a query.
 * @override
 */
DseLoadBalancingPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var self = this;
  this.childPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    if (err) {
      return callback(err);
    }
    if (queryOptions && queryOptions.preferredHost) {
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