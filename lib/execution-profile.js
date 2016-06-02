"use strict";
var utils = require('./utils');
var types = require('./types');

/**
 * Creates a new instance of {@link ExecutionProfile}.
 * @classdesc
 * Represents a set configurations to be used in a statement execution.
 * @param {Object} [options]
 * @param {Number} [options.consistency]
 * @param {LoadBalancingPolicy} [options.loadBalancing]
 * @param {Number} [options.readTimeout]
 * @param {RetryPolicy} [options.retry]
 * @param {Number} [options.serialConsistency]
 * @constructor
 */
function ExecutionProfile(options) {
  options = options || utils.emptyObject;
  /**
   * Consistency level
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
  this._profiles = options.profiles || {};
  this._setDefault(options);
  // use an array of profiles to iterate more efficiently
  this._profilesArray = Object.keys(this._profiles).map(function (name) {
    // set required properties
    var p = this._profiles[name];
    p.loadBalancing = p.loadBalancing || this._defaultProfile.loadBalancing;
    return p;
  }, this);
}

/**
 * @param {Client} client
 * @param {HostMap} hosts
 * @param {Function} callback
 */
ProfileManager.prototype.init = function (client, hosts, callback) {
  utils.eachSeries(this._profilesArray, function (profile, next) {
    profile.loadBalancing.init(client, hosts, next);
  }, callback);
};

/**
 * Uses the load-balancing policies to get the relative distance to the host and return the closest one.
 * @param {Host} host
 */
ProfileManager.prototype.getDistance = function (host) {
  var distance = types.distance.ignored;
  // this is performance critical: we can't use any other language features than for-loop :(
  for (var i = 0; i < this._profilesArray.length; i++) {
    var d = this._profilesArray[i].loadBalancing.getDistance(host);
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
 * @returns {ExecutionProfile}
 */
ProfileManager.prototype.getProfile = function (name) {
  if (name instanceof ExecutionProfile) {
    return name;
  }
  return this._profiles[name || 'default'];
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
  this._defaultProfile = this._profiles['default'];
  if (!this._defaultProfile) {
    this._defaultProfile = this._profiles['default'] = new ExecutionProfile();
  }
  // set the required properties
  this._defaultProfile.loadBalancing = this._defaultProfile.loadBalancing || options.policies.loadBalancing;
  this._defaultProfile.retry = this._defaultProfile.retry || options.policies.retry;
};

exports.ProfileManager = ProfileManager;
exports.ExecutionProfile = ExecutionProfile;
