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
import assert from "assert";
import util from "util";
import simulacron from "../simulacron";
import utils from "../../../lib/utils";
import helper from "../../test-helper";
import policies from "../../../lib/policies/index";
import errors from "../../../lib/errors";
import promiseUtils from "../../../lib/promise-utils";
import { ExecutionProfile } from "../../../lib/execution-profile";
import Client from "../../../lib/client";

'use strict';
const { loadBalancing } = policies;

const queryOptions = { prepare: true, routingKey: utils.allocBuffer(16), keyspace: 16 };

const localDc = 'dc1';

describe('LoadBalancingPolicy implementations', function() {
  this.timeout(20000);

  let cluster;
  let client;
  const localDcLength = 7;
  const remoteDcLength = 3;

  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  beforeEach(done => {
    cluster = new simulacron.SimulacronCluster();
    cluster.register([localDcLength, remoteDcLength], null, done);
  });

  beforeEach(done => cluster.prime({
    when: { query: 'SELECT * FROM table1' },
    then: { result: 'success' }
  }, done));

  beforeEach(done => cluster.prime({
    when: { query: 'SELECT * FROM delayed_1' },
    then: { result: 'success' }
  }, done));

  beforeEach(done => cluster.node(1).prime({
    when: { query: 'SELECT * FROM delayed_1' },
    then: { result: 'success', delay_in_ms: 50 }
  }, done));

  beforeEach(done => cluster.prime({
    when: { query: 'SELECT * FROM paused_2' },
    then: { result: 'success' }
  }, done));

  beforeEach(done => cluster.node(2).prime({
    when: { query: 'SELECT * FROM paused_2' },
    then: { result: 'success', delay_in_ms: 1200 }
  }, done));

  afterEach(() => {
    if (client) {
      client.shutdown();
    }
  });

  afterEach(done => cluster.unregister(done));

  describe('DefaultLoadBalancingPolicy', function() {

    context('when getReplicas() returns null', () => {
      it('should yield local hosts', () => {
        client = new Client({
          contactPoints: cluster.getContactPoints(),
          policies: {
            loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({localDc, getReplicas: () => null})
          }
        });

        const query = 'SELECT * FROM table1';

        return client.connect()
          .then(() => Promise.all(new Array(16).fill(null).map(_ => client.execute(query, [], queryOptions))))
          .then(results => results.map(r => client.hosts.get(r.info.queriedHost)).forEach(h =>
            assert.strictEqual(h.datacenter, localDc)));
      });
    });

    context('when no routing key is specified', () => {
      it('should balance between local hosts', () => {
        client = new Client({
          contactPoints: cluster.getContactPoints(),
          policies: {
            loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({localDc})
          }
        });

        const query = 'SELECT * FROM table1';
        const repeat = 5;
        const hostCounts = {};
        const incrementCount = result => {
          const address = result.info.queriedHost;
          hostCounts[address] = (hostCounts[address] || 0) + 1;
        };

        return client.connect()
          .then(() => promiseRepeat(localDcLength * repeat, 200, () => client.execute(query).then(incrementCount)))
          .then(() => {
            assert.strictEqual(Object.keys(hostCounts).length, localDcLength);
            Object.keys(hostCounts).forEach(address => {
              assert.strictEqual(client.hosts.get(address).datacenter, localDc);
              assert.strictEqual(hostCounts[address], repeat);
            });
          });
      });
    });

    it('should balance the load fairly between replicas', function () {
      if (helper.isWin()) {
        return this.skip();
      }

      let replicas = null;
      let localReplicas;

      client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({localDc, getReplicas: () => replicas})
        }
      });

      const query = 'SELECT * FROM table1';
      const length = 5000;
      const hostCounts = {};
      let totalCount = 0;
      const incrementCount = result => {
        const address = result.info.queriedHost;
        hostCounts[address] = (hostCounts[address] || 0) + 1;
        totalCount++;
      };

      return client.connect()
        .then(() => {
          // replicas needs to be faked to avoid getting token metadata with Simulacron
          let counter = 0;
          // 3 nodes from the local dc and all remote nodes are replicas
          replicas = client.hosts.values().filter(h => h.datacenter !== localDc || counter++ < 3);
          localReplicas = replicas.filter(h => h.datacenter === localDc);
        })
        .then(() => promiseRepeat(length, 200, () => client.execute(query, [], queryOptions).then(incrementCount)))
        .then(() => {
          assert.strictEqual(totalCount, length);

          // It should only contain local replicas
          assert.deepEqual(Object.keys(hostCounts).sort(), localReplicas.map(h => h.address).sort());

          // Look that it was "fairly" balanced
          const deviation = 0.1;
          utils.objectValues(hostCounts).forEach(count => {
            assert.ok(count > length / localReplicas.length - length * deviation);
            assert.ok(count < length / localReplicas.length + length * deviation);
          });
        });
    });

    it('should balance the load fairly between replicas when 1 replica takes more time to complete', function () {
      if (helper.isWin()) {
        return this.skip();
      }

      let replicas = null;
      let localReplicas;

      client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({localDc, getReplicas: () => replicas})
        }
      });

      const query = 'SELECT * FROM delayed_1';
      const length = 2000;
      const hostCounts = {};
      let totalCount = 0;
      const incrementCount = result => {
        const address = result.info.queriedHost;
        hostCounts[address] = (hostCounts[address] || 0) + 1;
        totalCount++;
      };

      return client.connect()
        .then(() => {
          // replicas needs to be faked to avoid getting token metadata with Simulacron
          let counter = 0;
          // 5 nodes from the local dc and all remote nodes are replicas
          replicas = client.hosts.values().filter(h => h.datacenter !== localDc || counter++ < 5);
          localReplicas = replicas.filter(h => h.datacenter === localDc);
        })
        .then(() => promiseRepeat(length, 200, () => client.execute(query, [], queryOptions).then(incrementCount)))
        .then(() => {
          assert.strictEqual(totalCount, length);

          // It should only contain local replicas
          assert.deepEqual(Object.keys(hostCounts).sort(), localReplicas.map(h => h.address).sort());

          const delayedAddress = cluster.node(1).address;
          const delayedCount = hostCounts[delayedAddress];
          delete hostCounts[delayedAddress];

          assert.ok(delayedCount > 0);
          // Assert less than half the rest of the healthy nodes
          utils.objectValues(hostCounts).forEach(healthyCount => assert.ok(delayedCount * 2 < healthyCount,
            util.format('Delayed vs healthy: %d was not less than half of %d', delayedCount, healthyCount)));

          // Look that it was "fairly" balanced between healthy nodes
          const deviation = 0.1;
          const healthyReplicasLength = localReplicas.length - 1;
          const healthyReplicasLoad = length - delayedCount;
          utils.objectValues(hostCounts).forEach(count => {
            const message = util.format('count %d expected to be within range of %d', count,
              healthyReplicasLoad / healthyReplicasLength);
            assert.ok(count > healthyReplicasLoad / healthyReplicasLength - length * deviation, message);
            assert.ok(count < healthyReplicasLoad / healthyReplicasLength + length * deviation, message);
          });
        });
    });

    it('should not send additional traffic when one node is paused', () => {
      let replicas = null;
      let localReplicas;

      client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({localDc, getReplicas: () => replicas})
        },
        pooling: {heartBeatInterval: 0}
      });

      const query = 'SELECT * FROM paused_2';
      const queriedHosts = new Set();
      const pausedAddress = cluster.node(2).address;
      const execPromises = [];

      return client.connect()
        .then(() => {
          // replicas needs to be faked to avoid getting token metadata with Simulacron
          let counter = 0;

          // 3 nodes from the local dc and all remote nodes are replicas
          replicas = client.hosts.values()
            .slice().sort(utils.propCompare('address'))
            .filter(h => h.datacenter !== localDc || counter++ < 3);

          localReplicas = replicas.filter(h => h.datacenter === localDc);
          // Local replicas Array contains the paused node
          assert.strictEqual(localReplicas.filter(h => h.address === pausedAddress).length, 1);
        })
        .then(() => {
          // send a bunch of queries without wait to finish
          for (let i = 0; i < 20 * localReplicas.length; i++) {
            execPromises.push(client.execute(query, [], queryOptions));
          }
        })
        .then(() => new Promise(r => setTimeout(r, 600)))
        .then(() => {
          // 20 items must be in-flight on the paused node
          const pausedHost = client.hosts.get(pausedAddress);
          assert.ok(pausedHost.getInFlight() > 5);
          client.hosts.forEach(h => {
            if (h !== pausedHost) {
              assert.strictEqual(h.getInFlight(), 0);
            }
          });
        })
        .then(() => promiseRepeat(10, 5, () => client.execute(query, [], queryOptions)
          .then(result => queriedHosts.add(result.info.queriedHost))))
        .then(() => {
          assert.deepStrictEqual(
            Array.from(queriedHosts).sort(),
            localReplicas.map(h => h.address).sort().filter(address => address !== pausedAddress));
        })
        .then(() => Promise.all(execPromises))
        .then(() => {
          // Make other queries and see that paused node is back to normal
          client.hosts.forEach(h => assert.strictEqual(h.getInFlight(), 0));
          return Promise.all(new Array(100).fill(null).map(_ =>
            client.execute('SELECT * FROM table1', [], queryOptions)));
        })
        .then(results => assert.ok(results.filter(r => r.info.queriedHost === pausedAddress).length > 0));
    });

    it('should validate localDc parameter and include available dcs in the error', async () => {
      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        profiles: [
          new ExecutionProfile('default', { loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({ localDc }) }),
          // Use a different LBP instance without setting the local DC
          new ExecutionProfile('test', { loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy() })
        ]
      });

      helper.shutdownAfterThisTest(client);

      await helper.assertThrowsAsync(client.connect(), errors.ArgumentError,
        /'localDataCenter' is not defined in Client options .* Available DCs are: \[dc1,dc2]/);

      await client.shutdown();
    });

    it('should validate that the local dc matches the topology and include available dcs in the error', async () => {
      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        profiles: [
          new ExecutionProfile('default', { loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({ localDc }) }),
          // Use a different LBP instance setting the local DC to an invalid one
          new ExecutionProfile('test', {
            loadBalancing: new loadBalancing.DefaultLoadBalancingPolicy({ localDc: 'dc_invalid' })
          })
        ]
      });

      helper.shutdownAfterThisTest(client);

      await helper.assertThrowsAsync(client.connect(), errors.ArgumentError,
        /Datacenter dc_invalid was not found\. Available DCs are: \[dc1,dc2]/);

      await client.shutdown();
    });
  });

  describe('policies.defaultLoadBalancingPolicy()', () => {
    it('should use the local dc provided', async () => {
      const dc = 'dc2';
      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: policies.defaultLoadBalancingPolicy(dc)
        }
      });

      helper.shutdownAfterThisTest(client);

      await client.connect();
      const coordinators = new Set();

      await promiseUtils.times(100, 32, async () => {
        const rs = await client.execute(helper.queries.basic);
        coordinators.add(rs.info.queriedHost);
      });

      assert.strictEqual(coordinators.size, remoteDcLength);
      coordinators.forEach(address => assert.strictEqual(client.hosts.get(address).datacenter, dc));

      await client.shutdown();
    });
  });
});

/** Start n actions that returns promises */
function promiseRepeat(times, limit, fn){
  if (times < limit) {
    limit = times;
  }

  let counter = 0;

  const promises = new Array(limit);

  function sendNext() {
    if (counter >= times) {
      return null;
    }
    return fn(counter++).then(sendNext);
  }

  for (let i = 0; i < limit; i++) {
    promises[i] = sendNext();
  }

  return Promise.all(promises).then(() => null);
}