/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { assert } from "chai";
import util from "util";
import helper from "../test-helper";
import policies from "../../lib/policies/index";
import clientOptions from "../../lib/client-options";
import { Host, HostMap } from "../../lib/host";
import types from "../../lib/types/index";
import utils from "../../lib/utils";
import { ExecutionOptions } from "../../lib/execution-options";
import errors from "../../lib/errors";
import Client from "../../lib/client";

'use strict';
const { loadBalancing } = policies;
const { DefaultLoadBalancingPolicy } = loadBalancing;
const { lastOctetOf } = helper;

const localDc = 'dc1';
const remoteDc = 'dc2';
const ipPrefixLocal = '10.0.0.';
const ipPrefixRemote = '10.0.1.';
const routingKey = utils.allocBufferUnsafe(0);

describe('DefaultLoadBalancingPolicy', () => {

  describe('constructor', () => {
    it('should support providing the localDc as a string instead of the options', () => {
      assert.strictEqual(new DefaultLoadBalancingPolicy('my-local-dc').localDc, 'my-local-dc');
    });

    it('should support providing the localDc in the options', () => {
      assert.strictEqual(new DefaultLoadBalancingPolicy({ localDc: 'my-local-dc2' }).localDc, 'my-local-dc2');
    });
  });

  describe('#newQueryPlan()', () => {

    context('when keyspace or routing key is not defined', () => {

      it('should return only local nodes and balance the load', () => {
        const localDcLength = 5;
        const occurrences = 2;

        const policy = getNewInstance({ local: localDcLength, remote: 3 });
        return getQueryPlan(policy, localDcLength * occurrences).then(result => {
          assert.strictEqual(result.length, localDcLength * occurrences);

          result.forEach(hosts => {
            // only local nodes
            hosts.forEach(assertLocalDcHost);
            assert.strictEqual(hosts.length, localDcLength);
          });

          // balanced between the local nodes
          result[0].forEach(host => {
            // assert that its on each place of the array twice
            for (let i = 0; i < localDcLength; i++) {
              assert.strictEqual(result.reduce((total, plan) => total + (plan[i] === host ? 1 : 0), 0), occurrences);
            }
          });
        });
      });

      it('should use the provided filter', () => {
        const localDcLength = 6;
        const expectedNodes = localDcLength / 2;
        // Filter: only nodes with even last byte
        const filter = h => lastOctetOf(h) % 2 === 0;
        const policy = getNewInstance({ local: localDcLength, remote: 3, filter });
        return getQueryPlan(policy, localDcLength).then(result => {
          assert.strictEqual(result.length, localDcLength);
          result.forEach(hosts => {
            hosts.forEach(assertLocalDcHost);
            assert.strictEqual(hosts.length, expectedNodes);
          });

          result[0].forEach(host => {
            for (let i = 0; i < expectedNodes; i++) {
              // Assert that its on each place of the array twice
              assert.strictEqual(
                result.reduce((total, plan) => total + (plan[i] === host ? 1 : 0), 0), localDcLength / expectedNodes);
            }
          });
        });
      });

      it('should yield preferredHost first when defined', () => testPreferredHost(false));
    });

    it('should return local replicas first and balance the load based on comparer', () => {
      const localDcLength = 7;
      const expectedReplicas = 5;
      const replicaCondition = h => lastOctetOf(h) > 1;

      let comparerCalled = 0;
      const compareFn = (h1, h2) => (lastOctetOf(h1) > lastOctetOf(h2) ? 1 : -1);

      const policy = getNewInstance({
        local: localDcLength, remote: 3,
        getReplicas: hostMap => hostMap.values().filter(replicaCondition),
        compare: (h1, h2) => compareFn(h1, h2, comparerCalled++)
      });

      const repeat = 500;
      return getQueryPlan(policy, repeat, 'ks1', routingKey).then(result => {

        assert.strictEqual(result.length, repeat);
        assert.strictEqual(comparerCalled, repeat);

        result.forEach(hosts => {
          hosts.forEach(assertLocalDcHost);
          assert.strictEqual(hosts.length, localDcLength);

          const replicas = hosts.slice(0, expectedReplicas);
          // The local replicas followed by the rest of the nodes
          assert.strictEqual(replicas.filter(replicaCondition).length, expectedReplicas);
          assert.strictEqual(hosts.slice(expectedReplicas).filter(replicaCondition).length, 0);

          // the first 2 have been ordered
          assert.strictEqual(compareFn(hosts[0], hosts[1]), 1);
        });
      });
    });

    it('should yield preferredHost first when defined', () => testPreferredHost(true));

    it('should yield preferredHost first when host is remote', () => testPreferredHost(true, true));

    it('should send unhealthy replicas to the back of the list', () => {
      const localDcLength = 7;
      // 5 replicas, 2 unhealthy
      const expectedReplicas = 5;
      // Use last byte to identify replicas
      const unhealthyReplicas = [ 3, 5 ];
      const repeat = 500;
      const replicaCondition = h => lastOctetOf(h) > 1;

      let comparerCalled = 0;
      const compareFn = (h1, h2) => (lastOctetOf(h1) > lastOctetOf(h2) ? 1 : -1);

      const policy = getNewInstance({
        local: localDcLength, remote: 3,
        getReplicas: hostMap => hostMap.values().filter(replicaCondition),
        compare: (h1, h2) => compareFn(h1, h2, comparerCalled++),
        healthCheck: h => unhealthyReplicas.indexOf(~~lastOctetOf(h)) === -1
      });

      return getQueryPlan(policy, repeat, 'ks1', routingKey).then(result => {

        assert.strictEqual(result.length, repeat);
        assert.strictEqual(comparerCalled, repeat);

        result.forEach(hosts => {
          hosts.forEach(assertLocalDcHost);
          assert.strictEqual(hosts.length, localDcLength);

          const replicas = hosts.slice(0, expectedReplicas);
          // The local replicas followed by the rest of the nodes
          assert.strictEqual(replicas.filter(replicaCondition).length, expectedReplicas);
          assert.strictEqual(hosts.slice(expectedReplicas).filter(replicaCondition).length, 0);

          // The last replicas are the unhealthy ones
          assert.deepStrictEqual(replicas
            .slice(expectedReplicas - unhealthyReplicas.length, expectedReplicas)
            .map(h => ~~lastOctetOf(h))
            .filter(a => unhealthyReplicas.indexOf(a) >= 0)
            .sort(), unhealthyReplicas);

          // the first 2 have been ordered
          assert.strictEqual(compareFn(hosts[0], hosts[1]), 1);
        });
      });
    });

    it('should not reorder unhealthy replicas when there is a majority of unhealthy replicas', () => {
      const localDcLength = 7;
      // 5 replicas, 3 unhealthy
      const expectedReplicas = 5;
      // Use last byte to identify replicas
      const unhealthyReplicas = [ 4, 5, 6 ];
      const repeat = 1000;
      const replicaCondition = h => lastOctetOf(h) > 1;

      let comparerCalled = 0;
      const compareFn = (h1, h2) => (lastOctetOf(h1) > lastOctetOf(h2) ? 1 : -1);

      const policy = getNewInstance({
        local: localDcLength, remote: 3,
        getReplicas: hostMap => hostMap.values().filter(replicaCondition),
        compare: (h1, h2) => compareFn(h1, h2, comparerCalled++),
        healthCheck: h => unhealthyReplicas.indexOf(~~lastOctetOf(h)) === -1
      });

      return getQueryPlan(policy, repeat, 'ks1', routingKey).then(result => {

        assert.strictEqual(result.length, repeat);
        assert.strictEqual(comparerCalled, repeat);

        result.forEach(hosts => {
          hosts.forEach(assertLocalDcHost);
          assert.strictEqual(hosts.length, localDcLength);

          const replicas = hosts.slice(0, expectedReplicas);
          // The local replicas followed by the rest of the nodes
          assert.strictEqual(replicas.filter(replicaCondition).length, expectedReplicas);
          assert.strictEqual(hosts.slice(expectedReplicas).filter(replicaCondition).length, 0);
          // the first 2 have been ordered
          assert.strictEqual(compareFn(hosts[0], hosts[1]), 1);
        });

        // At least once, the item in first position is an unhealthy replica
        assert(result.filter(hosts => unhealthyReplicas.indexOf(~~lastOctetOf(hosts[0])) > 0).length > 0);
      });
    });

    it('should use the provided filter and should not use comparer when there are only two replicas', () => {
      const localDcLength = 5;
      const expectedReplicas = 2;
      // 3 replicas but 1 is filtered out
      const replicaCondition = h => lastOctetOf(h) < 3;
      const repeat = 200;

      let comparerCalled = 0;
      const compareFn = (h1, h2) => (lastOctetOf(h1) > lastOctetOf(h2) ? 1 : -1);
      const filter = h => ~~lastOctetOf(h) !== 0;

      const policy = getNewInstance({
        local: localDcLength, remote: 3,
        getReplicas: hostMap => hostMap.values().filter(replicaCondition),
        compare: (h1, h2) => compareFn(h1, h2, comparerCalled++),
        filter
      });

      return getQueryPlan(policy, repeat, 'ks1', routingKey).then(result => {

        assert.strictEqual(result.length, repeat);
        assert.strictEqual(comparerCalled, 0);

        result.forEach(hosts => {
          hosts.forEach(assertLocalDcHost);
          // Filter was applied
          assert.strictEqual(hosts.length, localDcLength - 1);
          assert.strictEqual(hosts.filter(h => filter(h)).length, localDcLength - 1);

          const replicas = hosts.slice(0, expectedReplicas);
          // The local replicas followed by the rest of the nodes
          assert.strictEqual(replicas.filter(replicaCondition).length, expectedReplicas);
          assert.strictEqual(hosts.slice(expectedReplicas).filter(replicaCondition).length, 0);
        });
      });
    });

    it('should target newly up replicas fewer times than other replicas', () => {
      const localDcLength = 7;
      // 3 replicas, 1 newly UP
      const expectedReplicas = 5;
      // Use last byte to identify replicas
      const newlyUpReplica = 4;
      const repeat = 1500;
      const replicaCondition = h => lastOctetOf(h) > 1;

      const policy = getNewInstance({
        local: localDcLength, remote: 3,
        getReplicas: hostMap => hostMap.values().filter(replicaCondition),
        isHostNewlyUp: h => ~~lastOctetOf(h) === newlyUpReplica
      });

      return getQueryPlan(policy, repeat, 'ks1', routingKey).then(result => {

        assert.strictEqual(result.length, repeat);

        result.forEach(hosts => {
          hosts.forEach(assertLocalDcHost);
          assert.strictEqual(hosts.length, localDcLength);

          const replicas = hosts.slice(0, expectedReplicas);
          // The local replicas followed by the rest of the nodes
          assert.strictEqual(replicas.filter(replicaCondition).length, expectedReplicas);
        });

        const newlyUpAsCoordinator = result.filter(hosts => ~~lastOctetOf(hosts[0]) === newlyUpReplica).length;
        assert.ok(newlyUpAsCoordinator * 2 < repeat / expectedReplicas,
          util.format('%d expected to be less than half than %d', newlyUpAsCoordinator, repeat / expectedReplicas));
      });
    });
  });

  describe('#init()', () => {
    it('should throw an error when localDataCenter is not configured on Client options', function (done) {
      const policy = new DefaultLoadBalancingPolicy();
      const options = utils.extend({}, helper.baseOptions);
      delete options.localDataCenter;
      const hosts = new HostMap();
      hosts.set('1', createHost('1', 'dc1'));
      hosts.set('2', createHost('2', 'dc2'));
      const client = new Client(options);
      policy.init(client, hosts, (err) => {
        helper.assertInstanceOf(err, errors.ArgumentError);
        assert.strictEqual(err.message,
          `'localDataCenter' is not defined in Client options and also was not specified in constructor.` +
          ` At least one is required. Available DCs are: [dc1,dc2]`);
        done();
      });
    });

    it('should log on init when localDc was provided to constructor but localDataCenter was not set on Client options', function (done) {
      const policy = new DefaultLoadBalancingPolicy({ localDc: 'dc1' });
      const options = utils.extend({}, helper.baseOptions);
      delete options.localDataCenter;
      const client = new Client(options);
      const logEvents = [];
      client.on('log', function(level, className, message, furtherInfo) {
        logEvents.push({level: level, className: className, message: message, furtherInfo: furtherInfo});
      });
      const hosts = new HostMap();
      hosts.set('1', createHost('1'));
      utils.series([
        function initPolicy(next) {
          policy.init(client, hosts, next);
        },
        function checkLogs(next) {
          assert.strictEqual(logEvents.length, 1);
          const event = logEvents[0];
          assert.strictEqual(event.level, 'info');
          assert.strictEqual(event.message,
            `Local data center 'dc1' was provided as an argument to the load-balancing policy. It is preferable` +
            ` to specify the local data center using 'localDataCenter' in Client options instead when your` +
            ` application is targeting a single data center.`);
          next();
        }
      ], done);
    });
  });

  describe('#getDistance()', () => {
    it('should only mark nodes in local dc as local', () => {
      const localDcLength = 8;
      const policy = getNewInstance({ local: localDcLength, remote: 10 });
      const hosts = policy.getTestHostMap().values();

      assert.strictEqual(hosts.length, localDcLength + 10);

      hosts.forEach((h, i) => {
        assert.strictEqual(policy.getDistance(h), i < localDcLength ? types.distance.local : types.distance.ignored);
      });
    });

    it('should mark preferredHost as local', () => {
      const localDcLength = 8;
      const policy = getNewInstance({ local: localDcLength, remote: 10 });
      const hosts = policy.getTestHostMap().values();
      const preferredHost = hosts[hosts.length - 1];

      // Should be set the first time
      getQueryPlan(policy, 2, null, null, preferredHost);

      // Should not be unset when preferredHost is not provided
      getQueryPlan(policy, 2, null, null, null);

      assert.strictEqual(hosts.length, localDcLength + 10);

      // Preferred should be local
      assert.strictEqual(policy.getDistance(preferredHost), types.distance.local);

      // The distance to the rest of the nodes should be determined by the data center
      hosts.slice(0, hosts.length - 1).forEach((h, i) => {
        assert.strictEqual(policy.getDistance(h), i < localDcLength ? types.distance.local : types.distance.ignored);
      });
    });
  });
});

describe('policies.defaultLoadBalancingPolicy()', () => {
  [
    { title: 'without the local dc', localDc: undefined },
    { title: 'with a local dc', localDc: 'my_dc' }
  ].forEach(({ title, localDc }) => {
    it(`should support creating a new instance ${title}`, () => {
      const lbp = policies.defaultLoadBalancingPolicy(localDc);
      assert.instanceOf(lbp, DefaultLoadBalancingPolicy);
      assert.strictEqual(lbp.localDc, localDc);
    });
  });
});

function testPreferredHost(useReplicas, isPreferredRemote) {
  const localDcLength = 7;
  const replicaCondition = h => lastOctetOf(h) > 1;
  const repeat = localDcLength * 2;

  const policy = getNewInstance({
    local: localDcLength,
    remote: 3,
    getReplicas: hostMap => hostMap.values().filter(replicaCondition)
  });

  const preferredHost = policy.getTestHostMap().values()[isPreferredRemote ? localDcLength : 2];

  return getQueryPlan(policy, repeat, useReplicas && 'ks1', useReplicas && routingKey, preferredHost).then(result => {

    assert.strictEqual(result.length, repeat);

    result.forEach(hosts => {
      // Preferred host in the first position
      assert.strictEqual(hosts[0], preferredHost);

      // Check that preferred host appears once
      hosts.slice(1).forEach(h => assert.notEqual(h, preferredHost));

      if (!isPreferredRemote) {
        // only local nodes
        hosts.forEach(assertLocalDcHost);
        assert.strictEqual(hosts.length, localDcLength);
      } else {
        hosts.slice(1).forEach(assertLocalDcHost);
        assert.strictEqual(hosts.length, localDcLength + 1);
      }
    });
  });
}

/**
 * Gets an initialized policy containing the provided local and remote hosts.
 */
function getNewInstance(options) {
  const hosts = [];
  for (let i = 0; i < options.local; i++) {
    hosts.push(createHost(ipPrefixLocal + i, localDc));
  }

  for (let i = 0; i < options.remote; i++) {
    hosts.push(createHost(ipPrefixRemote + i, remoteDc));
  }

  const hostMap = createHostMap(hosts);

  let getReplicas;
  if (options.getReplicas) {
    getReplicas = () => options.getReplicas(hostMap);
  }

  const policy = new DefaultLoadBalancingPolicy({
    localDc, filter: options.filter, getReplicas, compare: options.compare, healthCheck: options.healthCheck,
    isHostNewlyUp: options.isHostNewlyUp
  });

  policy.init(null, hostMap, utils.noop);
  policy.getTestHostMap = () => hostMap;

  return policy;
}

function createHostMap(hosts) {
  const map = new HostMap();
  hosts.forEach(h => map.set(h.address, h));
  return map;
}

function getQueryPlan(policy, repeat, keyspace, routingKey, preferredHost) {

  const execOptions = ExecutionOptions.empty();
  execOptions.getRoutingKey = () => routingKey;
  execOptions.getPreferredHost = () => preferredHost;

  return new Promise((resolve, reject) => {
    const result = [];
    utils.times(repeat, (n, next) => {
      policy.newQueryPlan(keyspace, execOptions, (err, queryPlan) => {
        if (err) {
          return next(err);
        }
        result.push(utils.iteratorToArray(queryPlan));
        next();
      });
    }, err => {
      if (err) {
        return reject(err);
      }
      resolve(result);
    });
  });
}

function createHost(address, dc) {
  const options = clientOptions.extend({}, helper.baseOptions);
  const h = new Host(address, types.protocolVersion.maxSupported, options);
  h.datacenter = dc || 'dc1';
  return h;
}

function assertLocalDcHost(h) {
  assert.strictEqual(h.datacenter, localDc);
  helper.assertContains(h.address, ipPrefixLocal);
}