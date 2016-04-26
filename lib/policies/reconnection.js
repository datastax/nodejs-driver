var util = require('util');

var types = require('../types');
var utils = require('../utils.js');

/** @module policies/reconnection */
/**
 * Base class for Reconnection Policies
 * @constructor
 */
function ReconnectionPolicy() {

}

/**
 * A new reconnection schedule.
 * @returns {{next: function}} An infinite iterator
 */
ReconnectionPolicy.prototype.newSchedule = function () {
  throw new Error('You must implement a new schedule for the Reconnection class');
};

/**
 * A reconnection policy that waits a constant time between each reconnection attempt.
 * @param {Number} delay Delay in ms
 * @constructor
 */
function ConstantReconnectionPolicy(delay) {
  this.delay = delay;
}

util.inherits(ConstantReconnectionPolicy, ReconnectionPolicy);

/**
 * A new reconnection schedule that returns the same next delay value
 * @returns {{next: next}} An infinite iterator
 */
ConstantReconnectionPolicy.prototype.newSchedule = function () {
  var self = this;
  return {
    next: function () {
      return {value: self.delay, done: false};
    }
  }
};

/**
 * A reconnection policy that waits exponentially longer between each
 * reconnection attempt (but keeps a constant delay once a maximum delay is reached).
 * @param {Number} baseDelay Delay in ms that
 * @param {Number} maxDelay the maximum delay in ms to wait between two reconnection attempt
 * @param {Boolean} startWithNoDelay Determines if the first attempt should be zero delay
 * @constructor
 */
function ExponentialReconnectionPolicy(baseDelay, maxDelay, startWithNoDelay) {
  this.baseDelay = baseDelay;
  this.maxDelay = maxDelay;
  this.startWithNoDelay = startWithNoDelay;
}

util.inherits(ExponentialReconnectionPolicy, ReconnectionPolicy);

/**
 * A new schedule that uses an exponentially growing delay between reconnection attempts.
 * @returns {{next: next}} An infinite iterator
 */
ExponentialReconnectionPolicy.prototype.newSchedule = function () {
  var self = this;
  var index = this.startWithNoDelay ? -1 : 0;
  return {
    next: function () {
      index++;
      var delay = 0;
      if (index > 64) {
        delay = self.maxDelay;
      }
      else if (index !== 0) {
        delay = Math.min(Math.pow(2, index) * self.baseDelay, self.maxDelay);
      }
      return {value: delay, done: false};
    }
  }
};

exports.ReconnectionPolicy = ReconnectionPolicy;
exports.ConstantReconnectionPolicy = ConstantReconnectionPolicy;
exports.ExponentialReconnectionPolicy = ExponentialReconnectionPolicy;