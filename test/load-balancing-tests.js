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
    var originalHosts = ['A', 'B', 'C', 'E'];
    var times = 100;
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
          var lastHost = null;
          hosts.forEach(function (host) {
            length += (host === item ? 1 : 0);
            assert.notEqual(lastHost, host);
            lastHost = host;
          });
          assert.strictEqual(length, times / originalHosts.length);
        });
        done();
      });
    });
  });
  it('should yield host in a round robin manner when consuming', function (done) {
    var policy = new RoundRobinPolicy();
    var hosts = [];
    var originalHosts = ['A', 'B', 'C', 'E', 'F'];
    var times = 15;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(function (err, iterator) {
          assert.equal(err, null);
          for (var i = 0; i < originalHosts.length; i++) {
            var item = iterator.next();
            assert.strictEqual(item.done, false);
            hosts.push(item.value);
          }
          next();
        });
      }, function (err) {
        assert.equal(err, null);
        assert.strictEqual(hosts.length, times * originalHosts.length);
        //Count the number of times of each element
        originalHosts.forEach(function (item) {
          var length = 0;
          var lastHost = null;
          hosts.forEach(function (host) {
            length += (host === item ? 1 : 0);
            assert.notEqual(lastHost, host);
            lastHost = host;
          });
          assert.strictEqual(length, times);
        });
        done();
      });
    });
  });
  it('should yield no more than N host', function (done) {
    var policy = new RoundRobinPolicy();
    var originalHosts = ['A', 'B', 'C'];
    var times = 10;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(function (err, iterator) {
          var item;
          for (var i = 0; i < originalHosts.length; i++) {
            item = iterator.next();
            assert.strictEqual(item.done, false);
            assert.notEqual(item.value, null);
          }
          //one more time
          item = iterator.next();
          //it should be finished
          assert.strictEqual(item.done, true);
          assert.equal(item.value, null);
          //call once again just for fun
          iterator.next();
          next();
        });
      }, done);
    });
  });
  //TODO: Check with hosts changing
});