var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
var Host = require('../../lib/host.js').Host;
var types= require('../../lib/types.js');
var loadBalancing = require('../../lib/policies/load-balancing.js');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;

//mocha test fixture
describe('RoundRobinPolicy', function () {
  it('should yield nodes in a round robin manner even in parallel', function (done) {
    var policy = new RoundRobinPolicy();
    var hosts = [];
    var originalHosts = ['A', 'B', 'C', 'E'];
    var times = 100;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, function (err, iterator) {
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
        policy.newQueryPlan(null, function (err, iterator) {
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
        policy.newQueryPlan(null, function (err, iterator) {
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
  describe('DCAwareRoundRobinPolicy', function () {
    it('should yield local nodes in a round robin manner in parallel', function (done) {
      var policy = new DCAwareRoundRobinPolicy('dc1');
      var hosts = [];
      var originalHosts = [];
      for (var i = 0; i < 50; i++) {
        var h = new Host(i, 2, helper.baseOptions);
        h.datacenter = (i % 2 === 0) ? 'dc1' : 'dc2';
        originalHosts.push(h);
      }
      var localLength = originalHosts.length / 2;
      var times = 70;
      policy.init(new Client(helper.baseOptions), originalHosts, function (err) {
        assert.ifError(err);
        async.times(times, function (n, next) {
          policy.newQueryPlan(null, function (err, iterator) {
            assert.equal(err, null);
            for (var i = 0; i < localLength; i++) {
              var item = iterator.next();
              assert.strictEqual(item.done, false, 'It shouldn\'t be done at index ' + i);
              hosts.push(item.value);
            }
            next();
          });
        }, function (err) {
          assert.equal(err, null);
          assert.strictEqual(hosts.length, times * localLength);
          //Count the number of times of each element
          originalHosts.forEach(function (item) {
            var length = 0;
            var lastHost = null;
            hosts.forEach(function (host) {
              length += (host === item ? 1 : 0);
              assert.notEqual(lastHost, host);
              lastHost = host;
            });
            if (item.datacenter === 'dc1') {
              //check that appears the same times it was iterated.
              assert.strictEqual(length, times);
            }
            else {
              //check that it never hit the remote dc
              assert.strictEqual(length, 0);
            }
          });
          done();
        });
      });
    });
    it('should yield the correct amount of remote nodes at the end', function (done) {
      //local dc + 2 per each datacenter
      var policy = new DCAwareRoundRobinPolicy(null, 2);
      var hosts = [];
      var originalHosts = [];
      for (var i = 0; i < 60; i++) {
        var h = new Host(i, 2, helper.baseOptions);
        switch (i % 3) {
          case 0:
            h.datacenter = 'dc1';
            break;
          case 1:
            h.datacenter = 'dc2';
            break;
          case 2:
            h.datacenter = 'dc3';
            break;
        }
        originalHosts.push(h);
      }
      var localLength = originalHosts.length / 3;
      //2 nodes per each remote dc
      var expectedLength = localLength + 2 * 2;
      var times = 1;
      policy.init(new Client(helper.baseOptions), originalHosts, function (err) {
        assert.ifError(err);
        assert.strictEqual(policy.localDc, 'dc1');
        async.times(times, function (n, next) {
          policy.newQueryPlan(null, function (err, iterator) {
            assert.equal(err, null);
            for (var i = 0; i < originalHosts.length; i++) {
              var item = iterator.next();
              if (i >= expectedLength) {
                assert.strictEqual(item.done, true);
                continue;
              }
              if (i < localLength) {
                assert.strictEqual(item.value.datacenter, 'dc1');
              }
              else {
                assert.strictEqual(policy.getDistance(item.value), types.distance.remote);
              }
              hosts.push(item.value);
            }
            next();
          });
        }, function (err) {
          assert.equal(err, null);
          assert.strictEqual(hosts.length, times * expectedLength);
          //Count the number of times of each element
          originalHosts.forEach(function (item) {
            if (item.datacenter === 'dc1') {
              var length = 0;
              var lastHost = null;
              hosts.forEach(function (host) {
                length += (host === item ? 1 : 0);
                assert.notEqual(lastHost, host);
                lastHost = host;
              });
              //check that appears the same times it was iterated.
              assert.strictEqual(length, times);
            }
          });
          done();
        });
      });
    });
  });
  //TODO: Check with hosts changing, check if they are considered.
});