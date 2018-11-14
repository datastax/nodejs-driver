/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');
const types = require('../../lib/types');
const loadBalancing = require('../../lib/policies/load-balancing');
const DseLoadBalancingPolicy = loadBalancing.DseLoadBalancingPolicy;
const helper = require('../test-helper');
const ExecutionOptions = require('../../lib/execution-options').ExecutionOptions;

describe('DseLoadBalancingPolicy', function () {
  describe('constructor', function () {
    it('should set token aware as child policy', function () {
      const lbp = new DseLoadBalancingPolicy('us-east', 2);
      helper.assertInstanceOf(lbp._childPolicy, loadBalancing.TokenAwarePolicy);
    });
  });
  describe('#newQueryPlan()', function () {
    it('should return the preferred host first', function (done) {
      const hosts = [ 'h1', 'h2', 'h3'];
      const lbp = DseLoadBalancingPolicy.createAsWrapper(new TestLoadBalancingPolicy(hosts));
      lbp.newQueryPlan('ks1', getExecOptions({ preferredHost: 'h0' }), function (err, iterator) {
        assert.ifError(err);
        assert.ok(iterator);
        const hostArray = iteratorToArray(iterator);
        assert.deepEqual(hostArray, ['h0', 'h1', 'h2', 'h3']);
        done();
      });
    });
    it('should return the child policy plan when preferred is not defined', function (done) {
      const hosts = [ 'h1', 'h2', 'h3'];
      const lbp = DseLoadBalancingPolicy.createAsWrapper(new TestLoadBalancingPolicy(hosts));
      lbp.newQueryPlan('ks1', getExecOptions({ }), function (err, iterator) {
        assert.ifError(err);
        assert.ok(iterator);
        const hostArray = iteratorToArray(iterator);
        assert.deepEqual(hostArray, hosts);
        done();
      });
    });
    it('should mark the preferred host as local', function (done) {
      const hosts = [ 'h1', 'h2', 'h3'];
      const childPolicy = new TestLoadBalancingPolicy(hosts);
      childPolicy.getDistance = function () {
        return types.distance.ignored;
      };
      const lbp = DseLoadBalancingPolicy.createAsWrapper(childPolicy);
      lbp.newQueryPlan('ks1', getExecOptions({ preferredHost: 'h0' }), function (err, iterator) {
        assert.ifError(err);
        assert.ok(iterator);
        iteratorToArray(iterator);
        assert.strictEqual(lbp.getDistance('h0'), types.distance.local);
        assert.strictEqual(lbp.getDistance('h_not_exist'), types.distance.ignored);
        done();
      });
    });
  });
  describe('#getDistance()', function () {
    it('should use the distance from the child policy', function () {
      const childPolicy = new TestLoadBalancingPolicy([]);
      let childPolicyCalled = 0;
      childPolicy.getDistance = function () {
        return (++childPolicyCalled);
      };
      const lbp = DseLoadBalancingPolicy.createAsWrapper(childPolicy);
      //noinspection JSCheckFunctionSignatures
      assert.strictEqual(1, lbp.getDistance('h1'));
      //noinspection JSCheckFunctionSignatures
      assert.strictEqual(2, lbp.getDistance('h1'));
      //noinspection JSCheckFunctionSignatures
      assert.strictEqual(3, lbp.getDistance('h1'));
    });
  });
});

/**
 * A load balancing policy that uses a fixed list of hosts suitable for testing.
 * @extends {LoadBalancingPolicy}
 */
function TestLoadBalancingPolicy(arr) {
  this.arr = arr;
}

util.inherits(TestLoadBalancingPolicy, loadBalancing.LoadBalancingPolicy);

TestLoadBalancingPolicy.prototype.newQueryPlan = function (q, o, callback) {
  return callback(null, arrayIterator(this.arr));
};

function getExecOptions(options) {
  options = options || {};

  const execOptions = ExecutionOptions.empty();
  execOptions.getPreferredHost = () => options.preferredHost;
  return execOptions;
}

function arrayIterator (arr) {
  let index = 0;
  return { next : function () {
    if (index >= arr.length) {
      return { done: true };
    }
    return { value: arr[index++], done: false };
  }};
}

function iteratorToArray(iterator) {
  const values = [];
  let item = iterator.next();
  while (!item.done) {
    values.push(item.value);
    item = iterator.next();
  }
  return values;
}