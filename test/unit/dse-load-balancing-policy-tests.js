'use strict';
var assert = require('assert');
var util = require('util');
var loadBalancing = require('../../lib/policies/load-balancing');
var DseLoadBalancingPolicy = loadBalancing.DseLoadBalancingPolicy;
var LoadBalancingPolicy = loadBalancing.LoadBalancingPolicy;

describe('DseLoadBalancingPolicy', function () {
  describe('#newQueryPlan()', function () {
    it('should return the preferred host first', function (done) {
      var hosts = [ 'h1', 'h2', 'h3'];
      var lbp = new DseLoadBalancingPolicy(new TestLoadBalancingPolicy(hosts));
      lbp.newQueryPlan('ks1', { preferredHost: 'h0' }, function (err, iterator) {
        assert.ifError(err);
        assert.ok(iterator);
        var hostArray = iteratorToArray(iterator);
        assert.deepEqual(hostArray, ['h0', 'h1', 'h2', 'h3']);
        done();
      });
    });
    it('should return the child policy plan when preferred is not defined', function (done) {
      var hosts = [ 'h1', 'h2', 'h3'];
      var lbp = new DseLoadBalancingPolicy(new TestLoadBalancingPolicy(hosts));
      lbp.newQueryPlan('ks1', { }, function (err, iterator) {
        assert.ifError(err);
        assert.ok(iterator);
        var hostArray = iteratorToArray(iterator);
        assert.deepEqual(hostArray, hosts);
        done();
      });
    });
  });
  describe('#getDistance()', function () {
    it('should use the distance from the child policy', function () {
      var childPolicy = new TestLoadBalancingPolicy([]);
      var childPolicyCalled = 0;
      childPolicy.getDistance = function () {
        return (++childPolicyCalled);
      };
      var lbp = new DseLoadBalancingPolicy(childPolicy);
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
 */
function TestLoadBalancingPolicy(arr) {
  this.arr = arr;
}

util.inherits(TestLoadBalancingPolicy, loadBalancing.DseLoadBalancingPolicy);

TestLoadBalancingPolicy.prototype.newQueryPlan = function (q, o, callback) {
  return callback(null, arrayIterator(this.arr));
};

function arrayIterator (arr) {
  var index = 0;
  return { next : function () {
    if (index >= arr.length) {
      return { done: true };
    }
    return { value: arr[index++], done: false}
  }};
}

function iteratorToArray(iterator) {
  var values = [];
  var item = iterator.next();
  while (!item.done) {
    values.push(item.value);
    item = iterator.next();
  }
  return values;
}