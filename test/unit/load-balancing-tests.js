/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');

const helper = require('../test-helper.js');
const Client = require('../../lib/client.js');
const clientOptions = require('../../lib/client-options.js');
const Host = require('../../lib/host.js').Host;
const HostMap = require('../../lib/host.js').HostMap;
const types = require('../../lib/types');
const utils = require('../../lib/utils.js');
const loadBalancing = require('../../lib/policies/load-balancing.js');
const ExecutionOptions = require('../../lib/execution-options').ExecutionOptions;
const LoadBalancingPolicy = loadBalancing.LoadBalancingPolicy;
const TokenAwarePolicy = loadBalancing.TokenAwarePolicy;
const RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
const DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
const WhiteListPolicy = loadBalancing.WhiteListPolicy;

describe('RoundRobinPolicy', function () {
  it('should yield an error when the hosts are not set', function(done) {
    const policy = new RoundRobinPolicy();
    policy.hosts = null;
    policy.newQueryPlan(null, null, function(err) {
      assert(err instanceof Error);
      done();
    });
  });
  it('should yield nodes in a round robin manner even in parallel', function (done) {
    const policy = new RoundRobinPolicy();
    const hosts = [];
    const originalHosts = createHostMap(['A', 'B', 'C', 'E']);
    const times = 100;
    policy.init(null, originalHosts, function () {
      utils.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.equal(err, null);
          const item = iterator.next();
          assert.strictEqual(item.done, false);
          hosts.push(item.value);
          next();
        });
      }, function (err) {
        assert.equal(err, null);
        assert.strictEqual(hosts.length, times);
        //Count the number of times of each element
        originalHosts.forEach(function (item) {
          let length = 0;
          let lastHost = null;
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
    const policy = new RoundRobinPolicy();
    const hostList = ['A', 'B', 'C', 'E', 'F'];
    const permutations = [];
    // Capture the various permutations of plans.
    for (let i = 0; i < hostList.length; i++) {
      const permutation = [];
      for (let j = i; j < hostList.length + i; j++) {
        permutation.push(hostList[j % hostList.length]);
      }
      permutations.push(permutation);
    }
    const originalHosts = createHostMap(hostList);
    const times = 30;

    testRoundRobinPlan(times, policy, null, originalHosts, originalHosts, permutations, done);
  });
  it('should yield no more than N host', function (done) {
    const policy = new RoundRobinPolicy();
    const originalHosts = createHostMap(['A', 'B', 'C']);
    const times = 10;
    policy.init(null, originalHosts, function () {
      utils.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          let item;
          for (let i = 0; i < originalHosts.length; i++) {
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
    const policy = new DCAwareRoundRobinPolicy('dc1');
    policy.hosts = null;
    policy.newQueryPlan(null, null, function(err) {
      assert(err instanceof Error);
      done();
    });
  });
  it('should yield local nodes in a round robin manner in parallel', function (done) {
    //local datacenter: dc1
    //0 host per remote datacenter
    const policy = new DCAwareRoundRobinPolicy('dc1');
    const options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    const hosts = [];
    const originalHosts = new HostMap();
    for (let i = 0; i < 50; i++) {
      const h = new Host(i, 2, options);
      h.datacenter = (i % 2 === 0) ? 'dc1' : 'dc2';
      originalHosts.set(i.toString(), h);
    }
    const localLength = originalHosts.length / 2;
    const times = 1;
    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      utils.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.equal(err, null);
          for (let i = 0; i < originalHosts.length; i++) {
            const item = iterator.next();
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
          let length = 0;
          let lastHost = null;
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
    const policy = new DCAwareRoundRobinPolicy('dc1');
    const options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    const originalHosts = new HostMap();
    let i;
    for (i = 0; i < 50; i++) {
      const h = new Host(i, 2, options);
      h.datacenter = (i % 2 === 0) ? 'dc1' : 'dc2';
      originalHosts.set(i.toString(), h);
    }
    const localHosts = originalHosts.values().filter(function(element) {
      return element.datacenter === 'dc1';
    });
    const times = 50;

    const localPermutations = [];
    // Capture the various permutations of plans.
    for (i = 0; i < localHosts.length; i++) {
      const permutation = [];
      for(let j = i; j < localHosts.length + i; j++) {
        permutation.push(localHosts[j % localHosts.length]);
      }
      localPermutations.push(permutation);
    }

    testRoundRobinPlan(times, policy, options, originalHosts, localHosts, localPermutations, done);
  });
  it('should yield the correct amount of remote nodes at the end', function (done) {
    //local datacenter: null (first host's datacenter will be used)
    //2 host per remote datacenter
    const policy = new DCAwareRoundRobinPolicy(null, 2);
    const options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    const hosts = [];
    const originalHosts = new HostMap();
    for (let i = 0; i < 60; i++) {
      const h = new Host(i, 2, options);
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
    const localLength = originalHosts.length / 3;
    //2 nodes per each remote dc
    const expectedLength = localLength + 2 * 2;
    const times = 1;
    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      assert.strictEqual(policy.localDc, 'dc1');
      utils.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.equal(err, null);
          for (let i = 0; i < originalHosts.length; i++) {
            const item = iterator.next();
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
            let length = 0;
            let lastHost = null;
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
  it('should yield local + remote hosts in a round robin manner when consuming', function (done) {
    const policy = new DCAwareRoundRobinPolicy(null, 3);
    const options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    const originalHosts = new HostMap();
    let i;
    for (i = 0; i < 60; i++) {
      const h = new Host(i, 2, options);
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

    const localHosts = originalHosts.values().filter(function(element) {
      return element.datacenter === 'dc1';
    });

    const dc2Hosts = originalHosts.values().filter(function(element) {
      return element.datacenter === 'dc2';
    });

    const dc3Hosts = originalHosts.values().filter(function(element) {
      return element.datacenter === 'dc3';
    });

    const times = 60;

    const localPermutations = [];
    // Capture the various permutations of plans.
    for (i = 0; i < localHosts.length; i++) {
      const permutation = [];
      for(let j = i; j < localHosts.length + i; j++) {
        permutation.push(localHosts[j % localHosts.length]);
      }
      localPermutations.push(permutation);
    }

    policy.init(new Client(options), originalHosts, function (err) {
      assert.ifError(err);
      const plans = [];
      utils.times(times, function (n, next) {
        policy.newQueryPlan(null, null, function(err, iterator) {
          assert.ifError(err);
          const planHosts = [];
          // Iterate through plan local hosts + (remoteHosts * remoteDcs) + 1.
          utils.timesSeries(localHosts.length + (3 * 2) + 1, function (planN, iteratorNext) {
            const item = iterator.next();
            assert.strictEqual(item.done, (planN >= localHosts.length + (3 * 2)));
            // Wait a random amount of time between executions to ensure
            // sequence of query plan iteration does not impact other
            // query plans.
            const randomWait = Math.floor((Math.random() * 5) + 1);
            setTimeout(function () {
              planHosts.push(item.value);
              iteratorNext();
            }, randomWait);
          }, function (err) {
            assert.ifError(err);

            // Ensure each host appears only once and at the beginning of the
            // plan.
            localHosts.forEach(function (host) {
              let length = 0;
              planHosts.slice(0, localHosts.length).forEach(function (planHost) {
                length += (host === planHost ? 1 : 0);
              });
              assert.strictEqual(1, length,
                host + " appears " + length + " times in "
                + planHosts + ".  Expected only once.");
            });

            const foundDc2Hosts = [];
            const foundDc3Hosts = [];
            // Ensure that planHosts returned 3 remote hosts from each dc and
            // that they were unique.
            planHosts.slice(localHosts.length, localHosts.length + (3 * 2)).forEach(function (host) {
              let length = 0;
              dc2Hosts.forEach(function (dc2Host) {
                length += (host === dc2Host ? 1: 0);
              });

              assert.ok(length <= 1, host + " found more than once in plan.");
              if(length === 1) {
                foundDc2Hosts.push(host);
              } else {
                // If host is not in dc2, it should be in dc3.
                length = 0;
                dc3Hosts.forEach(function (dc3Host) {
                  length += (host === dc3Host ? 1 : 0);
                });

                assert.ok(length <= 1, host + " found more than once in plan.");
                assert.equal(1, length, host + " is a non-remote host found" +
                  " in plan advanced past local hosts.");
                if (length === 1) {
                  foundDc3Hosts.push(host);
                }
              }
            });

            assert.strictEqual(foundDc2Hosts.length, 3, "Expected 3 hosts" +
              " from dc2 in plan.");
            assert.strictEqual(foundDc3Hosts.length, 3, "Expected 3 hosts" +
              " from dc3 in plan.");
            plans.push({number: n, plan: planHosts});
            next(err);
          });
        });
      }, function (err) {
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
          let length = 0;
          let lastPlan = null;
          plans.forEach(function(item) {
            const localOnlyPlan = item.plan.slice(0, localHosts.length);
            const localOnlyPlanDesc = JSON.stringify(localOnlyPlan);
            const permutationDesc = JSON.stringify(permutation);
            length += (localOnlyPlanDesc === permutationDesc ? 1 : 0);
            assert.notEqual(lastPlan, localOnlyPlanDesc, "last encountered" +
              " plan is the same as the previous one.\n" + lastPlan + "\n===\n" + localOnlyPlanDesc);
            lastPlan = localOnlyPlanDesc;
          });
          assert.strictEqual(length, times / localPermutations.length);
        });

        // Ensure remote part of query plans is non-repeating among plans.
        let lastPlan = null;
        plans.forEach(function (item){
          const remoteOnlyPlan = item.plan.slice(localHosts.length);
          const remoteOnlyPlanDesc = JSON.stringify(remoteOnlyPlan);
          assert.notEqual(lastPlan, remoteOnlyPlanDesc, "last encountered" +
            " remote plan is the same as the previous one.\n" + lastPlan + "\n==\n" + remoteOnlyPlanDesc);
          lastPlan = remoteOnlyPlanDesc;
        });
        done();
      });
    });
  });
  it('should handle cache being cleared and next iterations', function (done) {
    const policy = new DCAwareRoundRobinPolicy('dc1');
    const options = clientOptions.extend({}, helper.baseOptions, {policies: {loadBalancing: policy}});
    const hosts = new HostMap();
    hosts.set('1', createHost('1', options));
    hosts.set('2', createHost('2', options));
    utils.series([
      function initPolicy(next) {
        policy.init(null, hosts, next);
      },
      function checkQueryPlanWithNewNodesBeingAdded(next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.ifError(err);
          const item = iterator.next();
          assert.ok(!item.done);
          // Add an item to clear the LBP cache
          hosts.set('3', createHost('2', options));
          assert.ok(!iterator.next().done);
          // It should be done now, as the LBP had a reference to the previous array of hosts.
          assert.ok(iterator.next().done);
          next();
        });
      },
      function checkNewQueryPlan(next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          assert.ifError(err);
          assert.strictEqual(utils.iteratorToArray(iterator).length, 3);
          next();
        });
      }
    ], done);

  });
  it('should warn on init when no local DC was configured', function (done) {
    const policy = new DCAwareRoundRobinPolicy();
    const client = new Client(helper.baseOptions);
    const logEvents = [];
    client.on('log', function(level, className, message, furtherInfo) {
      logEvents.push({level: level, className: className, message: message, furtherInfo: furtherInfo});
    });
    const hosts = new HostMap();
    hosts.set('1', createHost('1', client.options));
    utils.series([
      function initPolicy(next) {
        policy.init(client, hosts, next);
      },
      function checkLogs(next) {
        assert.strictEqual(logEvents.length, 1);
        const event = logEvents[0];
        assert.strictEqual(event.level, 'warning');
        assert.strictEqual(event.message, 'No local Data Center was provided with DCAwareRoundRobinPolicy.' +
          '  Using discovered DC \'dc1\' from host 1.  Future releases will require local DC to be specified.');
        next();
      }
    ], done);
  });
  it('should not warn on init when local DC was configured', function (done) {
    const policy = new DCAwareRoundRobinPolicy('dc1');
    const client = new Client(helper.baseOptions);
    const logEvents = [];
    client.on('log', function(level, className, message, furtherInfo) {
      logEvents.push({level: level, className: className, message: message, furtherInfo: furtherInfo});
    });
    const hosts = new HostMap();
    hosts.set('1', createHost('1', client.options));
    utils.series([
      function initPolicy(next) {
        policy.init(client, hosts, next);
      },
      function checkLogs(next) {
        assert.strictEqual(logEvents.length, 0);
        next();
      }
    ], done);
  });
});
describe('TokenAwarePolicy', function () {
  it('should use the childPolicy when no routingKey provided', function (done) {
    const options = clientOptions.extend({}, helper.baseOptions);
    const childPolicy = createDummyPolicy(options);
    const policy = new TokenAwarePolicy(childPolicy);
    utils.series([
      function (next) {
        policy.init(new Client(options), new HostMap(), next);
      },
      function (next) {
        policy.newQueryPlan(null, null, function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
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
    const options = clientOptions.extend({}, helper.baseOptions);
    const childPolicy = createDummyPolicy(options);
    const policy = new TokenAwarePolicy(childPolicy);
    const client = new Client(options);
    client.getReplicas = toFunc([ 'repl1_remote', 'repl2_local', 'repl3_remote', 'repl4_local' ].map(toHost));
    utils.series([
      helper.toTask(policy.init, policy, client, new HostMap()),
      function (next) {
        policy.newQueryPlan(null, getExecOptions({ routingKey: utils.allocBufferUnsafe(16)}), function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
          assert.ok(hosts);
          assert.strictEqual(hosts.length, 4);
          assert.strictEqual(childPolicy.initCalled, 1);
          assert.strictEqual(childPolicy.newQueryPlanCalled, 1);
          // local replicas in the first 2 positions (unordered)
          assert.deepEqual(hosts.map(toAddress).slice(0, 2).sort(), ['repl2_local', 'repl4_local']);
          // Child load balancing policy nodes, do not repeat repl2
          assert.deepEqual(hosts.map(toAddress).slice(2, 4), [ 'child1', 'child2' ]);
          next();
        });
      }
    ], done);
  });
  it('should retrieve local and remote replicas in a pseudo random order', function (done) {
    const options = clientOptions.extend({}, helper.baseOptions);
    const childPolicy = createDummyPolicy(options);
    const policy = new TokenAwarePolicy(childPolicy);
    const client = new Client(options);
    client.getReplicas = toFunc(
      [ 'repl1_remote', 'repl2_local', 'repl3_remote', 'repl4_local', 'repl5_local'].map(toHost));
    const localReplicas = {};
    utils.series([
      helper.toTask(policy.init, policy, client, new HostMap()),
      function (next) {
        utils.timesLimit(100, 32, function (n, timesNext) {
          policy.newQueryPlan(null, getExecOptions({ routingKey: utils.allocBufferUnsafe(16) }), function (err, iterator) {
            const hosts = helper.iteratorToArray(iterator);
            assert.strictEqual(hosts.length, 5);
            assert.deepEqual(hosts.map(toAddress).slice(0, 3).sort(), ['repl2_local', 'repl4_local', 'repl5_local']);
            localReplicas[hosts[0].address] = true;
            // Child load balancing policy nodes, do not repeat repl2
            assert.deepEqual(hosts.map(toAddress).slice(3, 5), [ 'child1', 'child2' ]);
            timesNext();
          });
        }, next);
      },
      function checkFirstReplicas(next) {
        assert.strictEqual(Object.keys(localReplicas).length, 3);
        next();
      }
    ], done);
  });
  it('should fairly distribute between replicas', function (done) {
    this.timeout(20000);
    const options = clientOptions.extend({}, helper.baseOptions);
    const childPolicy = createDummyPolicy(options);
    const policy = new TokenAwarePolicy(childPolicy);
    const client = new Client(options);
    client.getReplicas = toFunc([
      'repl1_remote', 'repl2_local', 'repl3_remote', 'repl4_local', 'repl5_remote', 'repl6_local', 'repl7_local'
    ].map(toHost));
    // An array containing the amount of times it host appeared at a determined position
    const replicaPositions = [ {}, {}, {}, {} ];
    const routingKey = types.Uuid.random().buffer;
    const iterations = 100000;
    utils.series([
      helper.toTask(policy.init, policy, client, new HostMap()),
      function (next) {
        utils.timesLimit(iterations, 128, function (n, timesNext) {
          policy.newQueryPlan(null, getExecOptions({ routingKey }), function (err, iterator) {
            const hosts = helper.iteratorToArray(iterator);
            assert.strictEqual(hosts.length, 6);
            hosts.map(toAddress).slice(0, 4).forEach(function (address, i) {
              replicaPositions[i][address] = (replicaPositions[i][address] || 0) + 1;
            });
            process.nextTick(timesNext);
          });
        }, next);
      },
      function checkReplicas(next) {
        const totalHosts = replicaPositions.length;
        const expected = iterations / totalHosts;
        for (let i = 0; i < totalHosts; i++) {
          const hostsAtPosition = replicaPositions[i];
          // eslint-disable-next-line no-loop-func
          Object.keys(hostsAtPosition).forEach(function (address) {
            const timesSelected = hostsAtPosition[address];
            // Check that the times that the value is selected is close to the expected
            assert.ok(timesSelected > expected * 0.97);
            assert.ok(timesSelected < expected * 1.03);
          });
        }
        next();
      }
    ], done);
  });
});
describe('WhiteListPolicy', function () {
  it('should use the childPolicy to determine the distance', function () {
    let getDistanceCalled = 0;
    const childPolicy = {
      getDistance: function () {
        getDistanceCalled++;
        return types.distance.local;
      }
    };
    const policy = new WhiteListPolicy(childPolicy, ['h1:9042', 'h2:9042']);
    assert.strictEqual(policy.getDistance({ address: 'h1:9042'}), types.distance.local);
    assert.strictEqual(getDistanceCalled, 1);
    assert.strictEqual(policy.getDistance({ address: 'h2:9042'}), types.distance.local);
    assert.strictEqual(getDistanceCalled, 2);
    assert.strictEqual(policy.getDistance({ address: 'h_not_exists:9042'}), types.distance.ignored);
    //child policy should not be called
    assert.strictEqual(getDistanceCalled, 2);
  });
  it('should filter the child policy hosts', function (done) {
    const childPolicy = {
      newQueryPlan: function (ks, o, cb) {
        cb(null, utils.arrayIterator([{ address: '1.1.1.1:9042'}, { address: '1.1.1.2:9042'}, { address: '1.1.1.3:9042'}]));
      }
    };
    const policy = new WhiteListPolicy(childPolicy, ['1.1.1.3:9042', '1.1.1.1:9042']);
    policy.newQueryPlan('ks1', {}, function (err, iterator) {
      assert.ifError(err);
      const hosts = helper.iteratorToArray(iterator);
      assert.strictEqual(hosts.length, 2);
      assert.strictEqual(helper.lastOctetOf(hosts[0]), '1');
      assert.strictEqual(helper.lastOctetOf(hosts[1]), '3');
      done();
    });
  });
});

function testRoundRobinPlan(times, policy, options, allHosts, expectedHosts, permutations, done) {
  const client = options ? new Client(options) : null;

  policy.init(client, allHosts, function (err) {
    assert.ifError(err);
    let i = 0;
    utils.map(new Array(times), function (n, next) {
      n = i++;
      policy.newQueryPlan(null, null, function(err, iterator) {
        assert.ifError(err);
        if (expectedHosts instanceof HostMap) {
          expectedHosts = expectedHosts.values();
        }
        utils.mapSeries(expectedHosts, function (planN, iteratorNext) {
          const item = iterator.next();
          assert.strictEqual(item.done, false);
          // Wait a random amount of time between executions to ensure
          // sequence of query plan iteration does not impact other
          // query plans.
          const randomWait = Math.floor((Math.random() * 5) + 1);
          setTimeout(function () {
            iteratorNext(null, item.value);
          }, randomWait);
        }, function(err, planHosts) {
          assert.ifError(err);

          // Ensure each host appears only once.
          expectedHosts.forEach(function(host) {
            let length = 0;
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
        let length = 0;
        let lastPlan = null;
        plans.forEach(function(item) {
          const planDesc = JSON.stringify(item.plan);
          const permutationDesc = JSON.stringify(permutation);
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
  const childPolicy = new LoadBalancingPolicy();
  childPolicy.initCalled = 0;
  childPolicy.newQueryPlanCalled = 0;
  childPolicy.init = function (c, hs, cb) {
    childPolicy.initCalled++;
    cb();
  };
  childPolicy.getDistance = function (h) {
    if (h.address.lastIndexOf('_remote') > 0) {
      return types.distance.remote;
    }
    return types.distance.local;
  };
  childPolicy.newQueryPlan = function (k, o, cb) {
    childPolicy.newQueryPlanCalled++;

    const hosts = [ new Host('repl2_local', 2, options), new Host('child1', 2, options), new Host('child2', 2, options) ];
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
  const map = new HostMap();
  for (let i = 0; i < hosts.length; i++) {
    map.set(hosts[i], hosts[i]);
  }
  return map;
}

/**
 *
 * @param {String} address
 * @param {Object} options
 * @param {String} [dc]
 */
function createHost(address, options, dc) {
  const h = new Host(address, 4, options);
  h.datacenter = dc || 'dc1';
  return h;
}

/**
 * @param {Host} h
 * @returns {String}
 */
function toAddress(h) {
  return h.address;
}

/**
 * @param {String} address
 * @returns {Host}
 */
function toHost(address) {
  const options = clientOptions.extend({}, helper.baseOptions);
  return new Host(address, 4, options);
}

function toFunc(val) {
  return (() => val);
}

function getExecOptions(options) {
  const result = ExecutionOptions.empty();
  result.getRoutingKey = () => options.routingKey;
  return result;
}