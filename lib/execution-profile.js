"use strict";
const utils = require('./utils');
const types = require('./types');

/**
 * Creates a new instance of {@link ExecutionProfile}.
 * @classdesc
 * Represents a set configurations to be used in a statement execution to be used for a single {@link Client} instance.
 * <p>
 *   An {@link ExecutionProfile} instance should not be shared across different {@link Client} instances.
 * </p>
 * @param {String} name Name of the execution profile.
 * <p>
 *   Use <code>'default'</code> to specify that the new instance should be the default {@link ExecutionProfile} if no
 *   profile is specified in the execution.
 * </p>
 * @param {Object} [options] Profile options, when any of the options is not specified the {@link Client} will the use
 * the ones defined in the default profile.
 * @param {Number} [options.consistency] The consistency level to use for this profile.
 * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
 * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
 * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
 * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
 * @constructor
 */
function ExecutionProfile(name, options) {
  if (typeof name !== 'string') {
    throw new TypeError('Execution profile name must be a string');
  }
  options = options || utils.emptyObject;
  /**
   * Name of the execution profile.
   * @type {String}
   */
  this.name = name;
  /**
   * Consistency level.
   * @type {Number}
   */
  this.consistency = options.consistency;
  /**
   * Load-balancing policy
   * @type {LoadBalancingPolicy}
   */
  this.loadBalancing = options.loadBalancing;
  /**
   * Client read timeout.
   * @type {Number}
   */
  this.readTimeout = options.readTimeout;
  /**
   * Retry policy.
   * @type {RetryPolicy}
   */
  this.retry = options.retry;
  /**
   * Serial consistency level.
   * @type {Number}
   */
  this.serialConsistency = options.serialConsistency;
}

/**
 * @param {ClientOptions} options
 * @constructor
 * @ignore
 */
function ProfileManager(options) {
  this._profiles = options.profiles || [];
  this._setDefault(options);
  // A array of unique load balancing policies
  this._loadBalancingPolicies = [];
  // A dictionary of name keys and profile values
  this._profilesMap = {};
  this._profiles.forEach(function (p) {
    this._profilesMap[p.name] = p;
    // Set required properties
    p.loadBalancing = p.loadBalancing || this._defaultProfile.loadBalancing;
    // Using array indexOf is not very efficient (O(n)) but the amount of profiles should be limited
    // and a handful of load-balancing policies (no hashcode for load-Balancing policies)
    if (this._loadBalancingPolicies.indexOf(p.loadBalancing) === -1) {
      this._loadBalancingPolicies.push(p.loadBalancing);
    }
    return p;
  }, this);
}

/**
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 */
ProfileManager.prototype.init = function (client, hosts, callback) {
  utils.eachSeries(this._loadBalancingPolicies, function (policy, next) {
    policy.init(client, hosts, next);
  }, callback);
};

/**
 * Uses the load-balancing policies to get the relative distance to the host and return the closest one.
 * @param {Host} host
 */
ProfileManager.prototype.getDistance = function (host) {
  let distance = types.distance.ignored;
  // this is performance critical: we can't use any other language features than for-loop :(
  for (let i = 0; i < this._loadBalancingPolicies.length; i++) {
    const d = this._loadBalancingPolicies[i].getDistance(host);
    if (d < distance) {
      distance = d;
      if (distance === types.distance.local) {
        break;
      }
    }
  }
  host.setDistance(distance);
  return distance;
};

/**
 * @param {String|ExecutionProfile} name
 * @returns {ExecutionProfile|undefined} It returns the execution profile by name or the default profile when name is
 * undefined. It returns undefined when the profile does not exist.
 */
ProfileManager.prototype.getProfile = function (name) {
  if (name instanceof ExecutionProfile) {
    return name;
  }
  return this._profilesMap[name || 'default'];
};

/** @returns {ExecutionProfile} */
ProfileManager.prototype.getDefault = function () {
  return this._defaultProfile;
};

/** @returns {LoadBalancingPolicy} */
ProfileManager.prototype.getDefaultLoadBalancing = function () {
  return this._defaultProfile.loadBalancing;
};

/**
 * @private
 * @param {ClientOptions} options
 */
ProfileManager.prototype._setDefault = function (options) {
  this._defaultProfile = this._profiles.filter(function (p) { return p.name === 'default'; })[0];
  if (!this._defaultProfile) {
    this._profiles.push(this._defaultProfile = new ExecutionProfile('default'));
  }
  // set the required properties
  this._defaultProfile.loadBalancing = this._defaultProfile.loadBalancing || options.policies.loadBalancing;
  this._defaultProfile.retry = this._defaultProfile.retry || options.policies.retry;
};

exports.ProfileManager = ProfileManager;
exports.ExecutionProfile = ExecutionProfile;
