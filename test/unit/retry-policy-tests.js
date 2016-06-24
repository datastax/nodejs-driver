/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');
var cassandra = require('cassandra-driver');
var retry = require('../../lib/policies/retry');
var FallthroughRetryPolicy = retry.FallthroughRetryPolicy;
var helper = require('../helper');


describe('FallthroughRetryPolicy', function () {
  describe('constructor', function () {
    it('should create instance of RetryPolicy', function () {
      var policy = new FallthroughRetryPolicy();
      helper.assertInstanceOf(policy, retry.RetryPolicy);
    });
  });
  describe('#onReadTimeout()', function () {
    it('should return  rethrow decision', function () {
      var policy = new FallthroughRetryPolicy();
      var decisionInfo = policy.onReadTimeout();
      assert.ok(decisionInfo);
      assert.strictEqual(decisionInfo.decision, retry.RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onRequestError()', function () {
    it('should return  rethrow decision', function () {
      var policy = new FallthroughRetryPolicy();
      var decisionInfo = policy.onRequestError();
      assert.ok(decisionInfo);
      assert.strictEqual(decisionInfo.decision, retry.RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onUnavailable()', function () {
    it('should return  rethrow decision', function () {
      var policy = new FallthroughRetryPolicy();
      var decisionInfo = policy.onUnavailable();
      assert.ok(decisionInfo);
      assert.strictEqual(decisionInfo.decision, retry.RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onWriteTimeout()', function () {
    it('should return  rethrow decision', function () {
      var policy = new FallthroughRetryPolicy();
      var decisionInfo = policy.onWriteTimeout();
      assert.ok(decisionInfo);
      assert.strictEqual(decisionInfo.decision, retry.RetryPolicy.retryDecision.rethrow);
    });
  });
});