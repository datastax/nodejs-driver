var util = require('util');
var async = require('async');

var types = require('../types.js');
var utils = require('../utils.js');

/**
 * Base class for Load Balancing Policies
 * @constructor
 */
function LoadBalancingPolicy() {

}

/**
 * Initializes the load balancing policy.
 * @param {Client} client
 * @param {Array} hosts
 * @param {Function} callback
 */
LoadBalancingPolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  callback();
};

/**
 * Returns the distance assigned by this policy to the provided host.
 * @param {Host} host
 */
LoadBalancingPolicy.prototype.distance = function (host) {
  return types.distance.local;
};

/**
 * Returns an iterator with the hosts for a new query.
 * Each new query will call this method. The first host in the result will
 * then be used to perform the query.
 * @param {Function} callback
 */
LoadBalancingPolicy.prototype.newQueryPlan = function (callback) {
  callback(new Error('You must implement a query plan for the LoadBalancingPolicy class'));
};

/**
 * This policy yield nodes in a round-robin fashion.
 * @constructor
 */
function RoundRobinPolicy() {
  this.index = 0;
}

util.inherits(RoundRobinPolicy, LoadBalancingPolicy);

LoadBalancingPolicy.prototype.newQueryPlan = function (callback) {
  //clone the hosts
  var hosts = this.hosts.slice(0);
  var self = this;
  callback(null, {
    next: function () {
      self.index += 1;
      //overflow protection
      if (self.index >= utils.maxInt) {
        self.index = 0;
      }
      return {value: hosts[self.index % hosts.length], done: false};
    }
  });
};

exports.LoadBalancingPolicy = LoadBalancingPolicy;
exports.RoundRobinPolicy = RoundRobinPolicy;