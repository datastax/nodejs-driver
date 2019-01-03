/**
 * Copyright (C) 2016-2017 DataStax, Inc.
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
  it('should exponentially grow the wait time', function (done) {
    const baseDelay = 1000;
    const maxDelay = 256000;
    const policy = new reconnection.ExponentialReconnectionPolicy(baseDelay, maxDelay, false);
    utils.times(1, function (n, next) {
      const schedule = policy.newSchedule();
      for (let i = 0; i < 8; i++) {
        assert.strictEqual(schedule.next().value, Math.pow(2, i + 1) * baseDelay);
      }
      for (let j = 8; j < 30; j++) {
        assert.strictEqual(schedule.next().value, maxDelay);
      }
      next();
    }, done);

  });

  describe('#getOptions()', () => {
    it('should return a Map with the policy options', () => {
      helper.assertMapEqual(new reconnection.ExponentialReconnectionPolicy(2000, 100000, false).getOptions(),
        new Map([['baseDelay', 2000], ['maxDelay', 100000], ['startWithNoDelay', false]]));
    });
  });
});