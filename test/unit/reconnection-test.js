'use strict';
const assert = require('assert');
//project modules
const utils = require('../../lib/utils');
const reconnection = require('../../lib/policies/reconnection');

describe('ConstantReconnectionPolicy', function () {
  it('should yield the same wait time', function (done) {
    var delay = 2000;
    const policy = new reconnection.ConstantReconnectionPolicy(delay);
    utils.times(100, function (n, next) {
      var schedule = policy.newSchedule();
      for (let i = 0; i < 5; i++) {
        assert.strictEqual(schedule.next().value, delay);
      }
      next();
    }, done);

  });
});

describe('ExponentialReconnectionPolicy', function () {
  it('should exponentially grow the wait time', function (done) {
    var baseDelay = 1000;
    var maxDelay = 256000;
    const policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, false);
    utils.times(1, function (n, next) {
      var schedule = policy.newSchedule();
      for (let i = 0; i < 8; i++) {
        assert.strictEqual(schedule.next().value, Math.pow(2, i + 1) * baseDelay);
      }
      for (var j = 8; j < 30; j++) {
        assert.strictEqual(schedule.next().value, maxDelay);
      }
      next();
    }, done);

  });
});