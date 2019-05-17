/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
//project modules
const utils = require('../../lib/utils');
const helper = require('../test-helper');
const reconnection = require('../../lib/policies/reconnection');

describe('ConstantReconnectionPolicy', function () {
  it('should yield the same wait time', function (done) {
    const delay = 2000;
    const policy = new reconnection.ConstantReconnectionPolicy(delay);
    utils.times(100, function (n, next) {
      const schedule = policy.newSchedule();
      for (let i = 0; i < 5; i++) {
        assert.strictEqual(schedule.next().value, delay);
      }
      next();
    }, done);

  });

  describe('#getOptions()', () => {
    it('should return a Map with the delay', () => {
      helper.assertMapEqual(new reconnection.ConstantReconnectionPolicy(123).getOptions(), new Map([['delay', 123]]));
    });
  });
});

describe('ExponentialReconnectionPolicy', function () {
  it('should exponentially grow the wait time', function () {
    const baseDelay = 1000;
    const maxDelay = 256000;
    const policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, false);

    const schedule = policy.newSchedule();

    let value = schedule.next().value;
    assert.ok(value >= baseDelay && value < baseDelay * 1.15, `Value is incorrect: ${value}`);

    for (let i = 1; i < 8; i++) {
      value = schedule.next().value;
      const expectedDelay = Math.pow(2, i) * baseDelay;
      assert.ok(value >= expectedDelay * 0.85 && value < expectedDelay * 1.15, `Value is incorrect: ${value}`);
    }

    for (let j = 8; j < 30; j++) {
      value = schedule.next().value;
      assert.ok(value >= maxDelay * 0.85 && value < maxDelay, `Value is incorrect: ${value}`);
    }
  });

  it('should return independent schedules', () => {
    const baseDelay = 1000;
    const maxDelay = 32000;
    const policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, false);

    const schedule1 = policy.newSchedule();
    let value = schedule1.next().value;
    assert.ok(value >= baseDelay && value < baseDelay * 1.15, `Value is unexpected: ${value}`);
    value = schedule1.next().value;
    assert.ok(value >= 2 * 0.85 * baseDelay, `Value is unexpected: ${value}`);

    // Validate that the other iterator starts from the beginning
    const schedule2 = policy.newSchedule();
    value = schedule2.next().value;
    assert.ok(value >= baseDelay && value < baseDelay * 1.15, `Value is unexpected: ${value}`);
  });

  it('should support starting with no delay', () => {
    const baseDelay = 1000;
    const maxDelay = 32000;
    const policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, true);

    const schedule1 = policy.newSchedule();
    let value = schedule1.next().value;
    // Initially is zero
    assert.strictEqual(value, 0);

    // The following times follows exponential growth
    value = schedule1.next().value;
    assert.ok(value >= 0.85 * baseDelay && value < baseDelay * 1.15, `Value is unexpected: ${value}`);
    value = schedule1.next().value;
    assert.ok(value >= 2 * 0.85 * baseDelay, `Value is unexpected: ${value}`);
  });

  describe('#getOptions()', () => {
    it('should return a Map with the policy options', () => {
      helper.assertMapEqual(new reconnection.ExponentialReconnectionPolicy(2000, 100000, false).getOptions(),
        new Map([['baseDelay', 2000], ['maxDelay', 100000], ['startWithNoDelay', false]]));
    });
  });
});