/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const util = require('util');

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
 * Gets an associative array containing the policy options.
 */
ReconnectionPolicy.prototype.getOptions = function () {
  return new Map();
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
 * @returns {{next: Function}} An infinite iterator
 */
ConstantReconnectionPolicy.prototype.newSchedule = function () {
  const self = this;
  return {
    next: function () {
      return {value: self.delay, done: false};
    }
  };
};

/**
 * Gets an associative array containing the policy options.
 */
ConstantReconnectionPolicy.prototype.getOptions = function () {
  return new Map([['delay', this.delay ]]);
};

/**
 * A reconnection policy that waits exponentially longer between each
 * reconnection attempt (but keeps a constant delay once a maximum delay is reached).
 * <p>
 *   A random amount of jitter (+/- 15%) will be added to the pure exponential delay value to avoid situations
 *   where many clients are in the reconnection process at exactly the same time. The jitter will never cause the
 *   delay to be less than the base delay, or more than the max delay.
 * </p>
 * @param {Number} baseDelay The base delay in milliseconds to use for the schedules created by this policy.
 * @param {Number} maxDelay The maximum delay in milliseconds to wait between two reconnection attempt.
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
 * @returns {{next: Function}} An infinite iterator.
 */
ExponentialReconnectionPolicy.prototype.newSchedule = function* () {
  let index = this.startWithNoDelay ? -1 : 0;

  while (true) {
    let delay = 0;

    if (index >= 64) {
      delay = this.maxDelay;
    } else if (index !== -1) {
      delay = Math.min(Math.pow(2, index) * this.baseDelay, this.maxDelay);
    }

    index++;

    yield this._addJitter(delay);
  }
};

/**
 * Adds a random portion of +-15% to the delay provided.
 * Initially, its adds a random value of 15% to avoid reconnection before reaching the base delay.
 * When the schedule reaches max delay, only subtracts a random portion of 15%.
 */
ExponentialReconnectionPolicy.prototype._addJitter = function (value) {
  if (value === 0) {
    // Instant reconnection without jitter
    return value;
  }

  // Use the formula: 85% + rnd() * 30% to calculate the percentage of the original delay
  let minPercentage = 0.85;
  let range = 0.30;

  if (!this.startWithNoDelay && value === this.baseDelay) {
    // Between 100% to 115% of the original value
    minPercentage = 1;
    range = 0.15;
  } else if (value === this.maxDelay) {
    // Between 85% to 100% of the original value
    range = 0.15;
  }

  return Math.floor(value * (Math.random() * range + minPercentage));
};

/**
 * Gets an associative array containing the policy options.
 */
ExponentialReconnectionPolicy.prototype.getOptions = function () {
  return new Map([
    ['baseDelay', this.baseDelay ],
    ['maxDelay', this.maxDelay ],
    ['startWithNoDelay', this.startWithNoDelay ]
  ]);
};

exports.ReconnectionPolicy = ReconnectionPolicy;
exports.ConstantReconnectionPolicy = ConstantReconnectionPolicy;
exports.ExponentialReconnectionPolicy = ExponentialReconnectionPolicy;