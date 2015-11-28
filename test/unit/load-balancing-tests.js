var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
var clientOptions = require('../../lib/client-options.js');
var Host = require('../../lib/host.js').Host;
var HostMap = require('../../lib/host.js').HostMap;
var types = require('../../lib/types');
var utils = require('../../lib/utils.js');
var loadBalancing = require('../../lib/policies/load-balancing.js');
var LoadBalancingPolicy = loadBalancing.LoadBalancingPolicy;
var TokenAwarePolicy = loadBalancing.TokenAwarePolicy;
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
var WhiteListPolicy = loadBalancing.WhiteListPolicy;

describe('RoundRobinPolicy', function () {
  it('should yield an error when the hosts are not set', function(done) {
    var policy = new RoundRobinPolicy();
    policy.hosts = null;
    policy.newQueryPlan(null, null, function(err, iterator) {
      assert(err instanceof Error);
      done();
    });
  });
  it('should yield nodes in a round robin manner even in parallel', function (done) {
    var policy = new RoundRobinPolicy();
    var hosts = [];
    var originalHosts = createHostMap(['A', 'B', 'C', 'E']);
    var times = 100;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
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
    var hostList = ['A', 'B', 'C', 'E', 'F'];
    var permutations = [];
    // Capture the various permutations of plans.
    for (var i = 0; i < hostList.length; i++) {
      var permutation = [];
      for(var j = i; j < hostList.length + i; j++) {
        permutation.push(hostList[j % hostList.length]);
      }
      permutations.push(permutation);
    }
    var originalHosts = createHostMap(hostList);
    var times = 30;

    testRoundRobinPlan(times, policy, null, originalHosts, originalHosts, permutations, done);
  });
  it('should yield no more than N host', function (done) {
    var policy = new RoundRobinPolicy();
    var originalHosts = createHostMap(['A', 'B', 'C']);
    var times = 10;
    policy.init(null, originalHosts, function () {
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
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
});
describe('DCAwareRoundRobinPolicy', function () {
  it('should yield an error when the hosts are not set', function(done) {
    var policy = new DCAwareRoundRobinPolicy('dc1');
    policy.hosts = null;
    policy.newQueryPlan(null, null, function(err, iterator) {
      assert(err instanceof Error);
      done();
    });
  });
  it('should yield local nodes in a round robin manner in parallel', function (done) {
    //local datacenter: dc1
    //0 host per remote datacenter
    var policy = new DCAwareRoundRobinPolicy('dc1');
    var options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    var hosts = [];
    var originalHosts = new HostMap();
    for (var i = 0; i < 50; i++) {
      var h = new Host(i, 2, options);
      h.datacenter = (i % 2 === 0) ? 'dc1' : 'dc2';
      originalHosts.set(i.toString(), h);
    }
    var localLength = originalHosts.length / 2;
    var times = 1;
    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.equal(err, null);
          for (var i = 0; i < originalHosts.length; i++) {
            var item = iterator.next();
            if (i >= localLength) {
              //once the local have ended, it should be "done"
              assert.strictEqual(item.done, true, 'Not done for item ' + i);
              assert.equal(item.value, null);
              continue;
            }
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
  it('should yield local hosts in a round robin manner when consuming.', function (done) {
    var policy = new DCAwareRoundRobinPolicy('dc1');
    var options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    var originalHosts = new HostMap();
    for (var i = 0; i < 50; i++) {
      var h = new Host(i, 2, options);
      h.datacenter = (i % 2 === 0) ? 'dc1' : 'dc2';
      originalHosts.set(i.toString(), h);
    }
    var localHosts = originalHosts.values().filter(function(element) {
      return element.datacenter == 'dc1';
    });
    var times = 50;

    var localPermutations = [];
    // Capture the various permutations of plans.
    for (var i = 0; i < localHosts.length; i++) {
      var permutation = [];
      for(var j = i; j < localHosts.length + i; j++) {
        permutation.push(localHosts[j % localHosts.length]);
      }
      localPermutations.push(permutation);
    }

    testRoundRobinPlan(times, policy, options, originalHosts, localHosts, localPermutations, done);
  });
  it('should yield the correct amount of remote nodes at the end', function (done) {
    //local datacenter: null (first host's datacenter will be used)
    //2 host per remote datacenter
    var policy = new DCAwareRoundRobinPolicy(null, 2);
    var options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    var hosts = [];
    var originalHosts = new HostMap();
    for (var i = 0; i < 60; i++) {
      var h = new Host(i, 2, options);
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
      originalHosts.set(i.toString(), h);
    }
    var localLength = originalHosts.length / 3;
    //2 nodes per each remote dc
    var expectedLength = localLength + 2 * 2;
    var times = 1;
    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      assert.strictEqual(policy.localDc, 'dc1');
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
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
  it('should yield local + remote hosts in a round robin manner when' +
    ' consuming', function (done) {
    var policy = new DCAwareRoundRobinPolicy(null, 3);
    var options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    var originalHosts = new HostMap();
    for (var i = 0; i < 60; i++) {
      var h = new Host(i, 2, options);
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
      originalHosts.set(i.toString(), h);
    }

    var localHosts = originalHosts.values().filter(function(element) {
      return element.datacenter == 'dc1';
    });

    var dc2Hosts = originalHosts.values().filter(function(element) {
      return element.datacenter == 'dc2';
    });

    var dc3Hosts = originalHosts.values().filter(function(element) {
      return element.datacenter == 'dc3';
    });

    var times = 60;

    var localPermutations = [];
    // Capture the various permutations of plans.
    for (var i = 0; i < localHosts.length; i++) {
      var permutation = [];
      for(var j = i; j < localHosts.length + i; j++) {
        permutation.push(localHosts[j % localHosts.length]);
      }
      localPermutations.push(permutation);
    }

    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      async.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function(err, iterator) {
          assert.ifError(err);
          // Iterate through plan local hosts + (remoteHosts * remoteDcs) + 1.
          async.timesSeries(localHosts.length + (3 * 2) + 1, function (planN, iteratorNext) {
            var item = iterator.next();
            assert.strictEqual(item.done, (planN >= localHosts.length + (3 * 2)));
            // Wait a random amount of time between executions to ensure
            // sequence of query plan iteration does not impact other
            // query plans.
            var randomWait = Math.floor((Math.random() * 5) + 1);
            setTimeout(function () {
              iteratorNext(null, item.value);
            }, randomWait);
          }, function (err, planHosts) {
            assert.ifError(err);

            // Ensure each host appears only once and at the beginning of the
            // plan.
            localHosts.forEach(function (host) {
              var length = 0;
              planHosts.slice(0, localHosts.length).forEach(function (planHost) {
                length += (host === planHost ? 1 : 0);
              });
              assert.strictEqual(1, length,
                host + " appears " + length + " times in "
                + planHosts + ".  Expected only once.");
            });

            var foundDc2Hosts = [];
            var foundDc3Hosts = [];
            // Ensure that planHosts returned 3 remote hosts from each dc and
            // that they were unique.
            planHosts.slice(localHosts.length, localHosts.length + (3 * 2)).forEach(function (host) {
              var length = 0;
              dc2Hosts.forEach(function (dc2Host) {
                length += (host == dc2Host ? 1: 0);
              });

              assert.ok(length <= 1, host + " found more than once in plan.");
              if(length == 1) {
                foundDc2Hosts.push(host);
              } else {
                // If host is not in dc2, it should be in dc3.
                length = 0;
                dc3Hosts.forEach(function (dc3Host) {
                  length += (host == dc3Host ? 1 : 0);
                });

                assert.ok(length <= 1, host + " found more than once in plan.");
                assert.equal(1, length, host + " is a non-remote host found" +
                  " in plan advanced past local hosts.");
                if (length == 1) {
                  foundDc3Hosts.push(host);
                }
              }
            });

            assert.strictEqual(foundDc2Hosts.length, 3, "Expected 3 hosts" +
              " from dc2 in plan.");
            assert.strictEqual(foundDc3Hosts.length, 3, "Expected 3 hosts" +
              " from dc3 in plan.");
            next(err, {number: n, plan: planHosts});
          });
        });
      }, function (err, plans) {
        assert.equal(err, null);
        // Sort plans in order of creation (they are emitted by completion
        // which is random).
        plans.sort(function(a, b) {
          return a.number - b.number;
        });
        assert.strictEqual(times, plans.length);

        // Ensure each permutation happened the expected number of times
        // (times / permutations) and never consecutively.
        localPermutations.forEach(function(permutation) {
          var length = 0;
          var lastPlan = null;
          plans.forEach(function(item) {
            var localOnlyPlan = item.plan.slice(0, localHosts.length);
            var localOnlyPlanDesc = JSON.stringify(localOnlyPlan);
            var permutationDesc = JSON.stringify(permutation);
            length += (localOnlyPlanDesc === permutationDesc ? 1 : 0);
            assert.notEqual(lastPlan, localOnlyPlanDesc, "last encountered" +
              " plan is the same as the previous one.\n" + lastPlan + "\n===\n" + localOnlyPlanDesc);
            lastPlan = localOnlyPlanDesc;
          });
          assert.strictEqual(length, times / localPermutations.length);
        });

        // Ensure remote part of query plans is non-repeating among plans.
        var lastPlan = null;
        plans.forEach(function (item){
          var remoteOnlyPlan = item.plan.slice(localHosts.length);
          var remoteOnlyPlanDesc = JSON.stringify(remoteOnlyPlan);
          assert.notEqual(lastPlan, remoteOnlyPlanDesc, "last encountered" +
            " remote plan is the same as the previous one.\n" + lastPlan + "\n==\n" + remoteOnlyPlanDesc);
          lastPlan = remoteOnlyPlanDesc;
        });
        done();
      });
    });
  });
});
describe('TokenAwarePolicy', function () {
  it('should use the childPolicy when no routingKey provided', function (done) {
    var options = clientOptions.extend({}, helper.baseOptions);
    var childPolicy = createDummyPolicy(options);
    var policy = new TokenAwarePolicy(childPolicy);
    async.series([
      function (next) {
        policy.init(new Client(options), new HostMap(), next);
      },
      function (next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          var hosts = helper.iteratorToArray(iterator);
          assert.ok(hosts);
          assert.strictEqual(hosts.length, 3);
          assert.strictEqual(childPolicy.initCalled, 1);
          assert.strictEqual(childPolicy.newQueryPlanCalled, 1);
          next();
        });
      }
    ], done);
  });
  it('should retrieve local replicas plus childPolicy hosts plus remote replicas', function (done) {
    var options = clientOptions.extend({}, helper.baseOptions);
    var childPolicy = createDummyPolicy(options);
    var policy = new TokenAwarePolicy(childPolicy);
    var client = new Client(options);
    client.getReplicas = function () {
      return [new Host('repl1', 2, options), new Host('repl2', 2, options), new Host('repl3', 2, options), new Host('repl4', 2, options)];
    };
    async.series([
      function (next) {
        policy.init(client, new HostMap(), next);
      },
      function (next) {
        policy.newQueryPlan(null, {routingKey: new Buffer(16)}, function (err, iterator) {
          var hosts = helper.iteratorToArray(iterator);
          assert.ok(hosts);
          assert.strictEqual(hosts.length, 6);
          assert.strictEqual(childPolicy.initCalled, 1);
          assert.strictEqual(childPolicy.newQueryPlanCalled, 1);
          assert.strictEqual(hosts[0].address, 'repl2');
          assert.strictEqual(hosts[1].address, 'repl4');
          //Child load balancing policy nodes, do not repeat repl2
          assert.strictEqual(hosts[2].address, 'child1');
          assert.strictEqual(hosts[3].address, 'child2');
          //Remote replicas
          assert.strictEqual(hosts[4].address, 'repl1');
          assert.strictEqual(hosts[5].address, 'repl3');
          next();
        });
      }
    ], done);
  });
});
describe('WhiteListPolicy', function () {
  it('should use the childPolicy to determine the distance', function () {
    var getDistanceCalled = 0;
    var childPolicy = {
      getDistance: function () {
        getDistanceCalled++;
        return types.distance.local;
      }
    };
    var policy = new WhiteListPolicy(childPolicy, ['h1:9042', 'h2:9042']);
    assert.strictEqual(policy.getDistance({ address: 'h1:9042'}), types.distance.local);
    assert.strictEqual(getDistanceCalled, 1);
    assert.strictEqual(policy.getDistance({ address: 'h2:9042'}), types.distance.local);
    assert.strictEqual(getDistanceCalled, 2);
    assert.strictEqual(policy.getDistance({ address: 'h_not_exists:9042'}), types.distance.ignored);
    //child policy should not be called
    assert.strictEqual(getDistanceCalled, 2);
  });
  it('should filter the child policy hosts', function (done) {
    var childPolicy = {
      newQueryPlan: function (ks, o, cb) {
        cb(null, utils.arrayIterator([{ address: '1.1.1.1:9042'}, { address: '1.1.1.2:9042'}, { address: '1.1.1.3:9042'}]));
      }
    };
    var policy = new WhiteListPolicy(childPolicy, ['1.1.1.3:9042', '1.1.1.1:9042']);
    policy.newQueryPlan('ks1', {}, function (err, iterator) {
      assert.ifError(err);
      var hosts = helper.iteratorToArray(iterator);
      assert.strictEqual(hosts.length, 2);
      assert.strictEqual(helper.lastOctetOf(hosts[0]), '1');
      assert.strictEqual(helper.lastOctetOf(hosts[1]), '3');
      done()
    });
  });
});

function testRoundRobinPlan(times, policy, options, allHosts, expectedHosts, permutations, done) {
  var client = options ? new Client(options) : null;

  policy.init(client, allHosts, function (err) {
    assert.ifError(err);
    async.times(times, function (n, next) {
      policy.newQueryPlan(null, null, function(err, iterator) {
        assert.ifError(err);
        async.timesSeries(expectedHosts.length, function (planN, iteratorNext) {
          var item = iterator.next();
          assert.strictEqual(item.done, false);
          // Wait a random amount of time between executions to ensure
          // sequence of query plan iteration does not impact other
          // query plans.
          var randomWait = Math.floor((Math.random() * 5) + 1);
          setTimeout(function () {
            iteratorNext(null, item.value);
          }, randomWait);
        }, function(err, planHosts) {
          assert.ifError(err);

          // Ensure each host appears only once.
          expectedHosts.forEach(function(host) {
            var length = 0;
            planHosts.forEach(function(planHost) {
              length += (host === planHost ? 1 : 0);
            });
            assert.strictEqual(1, length,
              host + " appears " + length + " times in "
              + planHosts + ".  Expected only once.");
          });
          next(err, {number: n, plan: planHosts});
        });
      });
    }, function (err, plans) {
      assert.equal(err, null);
      // Sort plans in order of creation (they are emitted by completion
      // which is random).
      plans.sort(function(a, b) {
        return a.number - b.number;
      });

      assert.strictEqual(times, plans.length);
      // Ensure each permutation happened the expected number of times
      // (times / permutations) and never consecutively.
      permutations.forEach(function(permutation) {
        var length = 0;
        var lastPlan = null;
        plans.forEach(function(item) {
          var planDesc = JSON.stringify(item.plan);
          var permutationDesc = JSON.stringify(permutation);
          length += (planDesc === permutationDesc ? 1 : 0);
          assert.notEqual(lastPlan, planDesc, "last encountered plan is the" +
            " same as the previous one.\n" + lastPlan + "\n===\n" + planDesc);
          lastPlan = planDesc;
        });
        assert.strictEqual(length, times / permutations.length);
      });
      done();
    });
  });
}

/**
 * @param {Object} options
 * @returns {LoadBalancingPolicy}
 */
function createDummyPolicy(options) {
  var childPolicy = new LoadBalancingPolicy();
  childPolicy.initCalled = 0;
  childPolicy.newQueryPlanCalled = 0;
  childPolicy.remoteCounter = 0;
  childPolicy.init = function (c, hs, cb) {
    childPolicy.initCalled++;
    cb();
  };
  childPolicy.getDistance = function () {
    return childPolicy.remoteCounter++ % 2 === 0 ? types.distance.remote : types.distance.local;
  };
  childPolicy.newQueryPlan = function (k, o, cb) {
    childPolicy.newQueryPlanCalled++;

    var hosts = [new Host('repl2', 2, options), new Host('child1', 2, options), new Host('child2', 2, options)];
    cb(null, utils.arrayIterator(hosts));
  };
  return childPolicy;
}
/**
 * Creates a dummy host map containing the key as key and value
 * @param {Array} hosts
 * @returns {HostMap}
 */
function createHostMap(hosts) {
  var map = new HostMap();
  for (var i = 0; i < hosts.length; i++) {
    map.set(hosts[i], hosts[i]);
  }
  return map;
}