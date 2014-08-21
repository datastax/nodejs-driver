var assert = require('assert');
var async = require('async');
var util = require('util');
//project modules
var reconnection = require('../../lib/policies/reconnection.js');

describe('ConstantReconnectionPolicy', function () {
  it('should yield the same wait time', function (done) {
    var delay = 2000;
    var policy = new reconnection.ConstantReconnectionPolicy(delay);
    async.times(100, function (n, next) {
      var schedule = policy.newSchedule();
      for (var i = 0; i < 5; i++) {
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
    var policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, false);
    async.times(1, function (n, next) {
      var schedule = policy.newSchedule();
      for (var i = 0; i < 8; i++) {
        assert.strictEqual(schedule.next().value, Math.pow(2, i + 1) * baseDelay);
      }
      for (var j = 8; j < 30; j++) {
        assert.strictEqual(schedule.next().value, maxDelay);
      }
      next();
    }, done);

  });
});