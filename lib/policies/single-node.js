var LoadBalancingPolicy = require('./load-balancing.js').LoadBalancingPolicy;
var util = require('util');

/**
 * This policy yield nodes in a round-robin fashion.
 * @constructor
 */
function SingleNodePolicy() {}

util.inherits(SingleNodePolicy, LoadBalancingPolicy);


/**
 * Returns an iterator with the hosts for a new query.
 * Each new query will call this method. The first host in the result will
 * then be used to perform the query.
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions options evaluated for this execution
 * @param {Function} callback
 */
SingleNodePolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  this.single_host = this.hosts.slice(0, 1)[0];
  var self = this;

  callback(null, {
    next: function () {
      // logger.error(self.single_host.address, self.hosts)
      return {
        value: self.single_host,
        done: false
      };
    }
  });
};

module.exports = SingleNodePolicy;