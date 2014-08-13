var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');
//project modules
var loadBalancing = require('../lib/policies/load-balancing.js');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;

//mocha test fixture
describe('RoundRobinPolicy', function () {
  it('should yield nodes in a round robin manner even in parallel', function (done) {
    var policy = new RoundRobinPolicy();
    var hosts = [];
    var originalHosts = ['A', 'B', 'C', 'E', 'F'];
    var times = 5;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(function (err, iterator) {
          assert.equal(err, null);
          var item = iterator.next();
          assert.strictEqual(item.done, false);
          hosts.push(item.value);
          next();
        });
      }, function (err) {
        assert.equal(err, null);
        assert.strictEqual(hosts.length, times);
        //Count the number of times of each element
        originalHosts.forEach(function (item) {
          var length = 0;
          hosts.forEach(function (host) {
            length += (host === item ? 1 : 0);
          });
          assert.strictEqual(length, times / originalHosts.length);
        });
        done();
      });
    });
  });
  //TODO: Check with hosts changing
});