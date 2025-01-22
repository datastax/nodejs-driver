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

'use strict';

const { assert } = require('chai');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const errors = require('../../../lib/errors');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const policies = require('../../../lib/policies');
const promiseUtils = require('../../../lib/promise-utils');
const { version } = require('../../../index');
const { distance } = types;

const Client = require('../../../lib/client');

const healthResponseCountInterval = 200;

describe('pool', function () {

  this.timeout(40000);
  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  context('with a 3-node simulated cluster', function () {
    let cluster;
    let client;

    beforeEach(done => {
      cluster = new simulacron.SimulacronCluster();
      cluster.register([3], null, done);
    });
    beforeEach(() => client = new Client({
      contactPoints: cluster.getContactPoints(),
      // Use a LBP with a fixed order to have predictable behaviour
      // Warning: OrderedLoadBalancingPolicy is only suitable for testing!
      policies: { loadBalancing: new helper.OrderedLoadBalancingPolicy(cluster) },
      pooling: {
        coreConnectionsPerHost: {
          [types.distance.local]: 2
        },
        maxRequestsPerConnection: 50,
        heartBeatInterval: 0
      }
    }));
    beforeEach(done => cluster.prime({
      when: { query: 'SELECT * FROM paused' },
      then: { result: 'success', delay_in_ms: 2000 }
    }, done));
    beforeEach(done => cluster.node(0).prime({
      when: { query: 'SELECT * FROM paused_on_first' },
      then: { result: 'success', delay_in_ms: 2000 }
    }, done));
    afterEach(() => client.shutdown());
    afterEach(done => cluster.unregister(done));

    it('should use next host when a host is busy', function () {
      return client.connect()
        .then(() => Promise.all(new Array(150).fill(null).map(() => client.execute('SELECT * FROM paused'))))
        .then(results => {
          const hosts = cluster.dc(0).nodes.map(n => n.address);
          // First 100 items should be the first host (50 * 2 connections)
          assertArray(results.slice(0, 100).map(rs => rs.info.queriedHost), hosts[0]);
          // The next 50 items should be the second host
          assertArray(results.slice(100).map(rs => rs.info.queriedHost), hosts[1]);
        })
        .then(() => validateStateOfPool(client, cluster));
    });

    it('should fail when all hosts are busy', function () {
      return client.connect()
        .then(() => Promise.all(new Array(350).fill(null).map(() => {
          const resultTuple = {
            err: null,
            coordinator: null
          };
          return client.execute('SELECT * FROM paused')
            .then(rs => resultTuple.coordinator = rs.info.queriedHost)
            .catch(err => resultTuple.err = err)
            .then(() => resultTuple);
        })))
        .then(results => {
          const hosts = cluster.dc(0).nodes.map(x => x.address);

          // The first 300 requests should have succeed
          assertArray(results.slice(0, 100).map(r => r.coordinator), hosts[0]);
          assertArray(results.slice(100, 200).map(r => r.coordinator), hosts[1]);
          assertArray(results.slice(200, 300).map(r => r.coordinator), hosts[2]);

          // The last 50 items should have failed (rejected)
          results.slice(300).forEach(r => {
            assert.strictEqual(r.coordinator, null);
            helper.assertInstanceOf(r.err, errors.NoHostAvailableError);
            assert.deepEqual(Object.keys(r.err.innerErrors).sort(), hosts.sort());
            Object.values(r.err.innerErrors).forEach(err => {
              helper.assertInstanceOf(err, errors.BusyConnectionError);
            });
          });
        })
        .then(() => validateStateOfPool(client, cluster));
    });

    it('should increase and decrease response dequeued counter', function() {
      return client.connect()
        .then(() => Promise.all(new Array(150).fill(null).map(() => client.execute('SELECT * FROM system.local'))))
        .then(() => {
          assert.ok(client.hosts.values().filter(h => h.getResponseCount() > 1).length > 0);
        })
        .then(() => new Promise(resolve => setTimeout(resolve, healthResponseCountInterval * 2 + 20)))
        .then(() => {
          // After some time passed without any activity, the counter values should be 0.
          assert.strictEqual(client.hosts.values().filter(h => h.getResponseCount() !== 0).length, 0);
        });
    });

    it('should retry on the same host when defined by the retry policy', () => {
      let retryCount = 0;
      let firstHost;
      const retryPolicy = policies.defaultRetryPolicy();

      retryPolicy.onRequestError = (info, c, err) => {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        assert.notEqual(firstHost, undefined);
        assert.strictEqual(err.host, firstHost);
        retryCount = info.nbRetry;
        if (info.nbRetry > 1) {
          // After a couple of attempts, retry on the next host
          return retryPolicy.retryResult(undefined, false);
        }
        return retryPolicy.retryResult(undefined, true);
      };

      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: new helper.OrderedLoadBalancingPolicy(cluster),
          retry: retryPolicy
        },
        pooling: {
          coreConnectionsPerHost: {
            [types.distance.local]: 2
          },
          maxRequestsPerConnection: 50
        },
        socketOptions: { defunctReadTimeoutThreshold: Number.MAX_SAFE_INTEGER }
      });

      return client.connect()
        .then(() => firstHost = cluster.node(0).address)
        .then(() => client.execute('SELECT * FROM paused_on_first', null, { readTimeout: 200, isIdempotent: true }))
        .then(() => assert.strictEqual(retryCount, 2))
        .then(() => client.shutdown());
    });

    it('should retry on the next host when useCurrentHost is true and connections are not available', () => {
      let firstHost;
      const retryPolicy = policies.defaultRetryPolicy();
      let retryPolicyCalled = 0;

      retryPolicy.onRequestError = (info, c, err) => {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        assert.notEqual(firstHost, undefined);
        assert.strictEqual(err.host, firstHost.address);
        retryPolicyCalled++;

        // Use a safety mechanism to avoid retrying forever, in case the assertions fail
        const useCurrentHost = info.nbRetry < 5;
        return retryPolicy.retryResult(undefined, useCurrentHost);
      };

      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        policies: {
          loadBalancing: new helper.OrderedLoadBalancingPolicy(cluster),
          retry: retryPolicy
        },
        pooling: {
          coreConnectionsPerHost: {
            [types.distance.local]: 2
          },
          maxRequestsPerConnection: 50
        },
        socketOptions: { defunctReadTimeoutThreshold: Number.MAX_SAFE_INTEGER }
      });

      const promises = [];

      return client.connect()
        .then(() => {
          firstHost = client.hosts.get(cluster.node(0).address);

          // Create 99 in-flight requests
          promises.push.apply(promises, new Array(99).fill(0).map(() =>
            client.execute('SELECT * FROM paused_on_first', null, { readTimeout: 10000 })));

          // The pool will be busy after this request, so the retry will occur on the next host
          const p2 = client.execute('SELECT * FROM paused_on_first', null, { readTimeout: 50, isIdempotent: true });
          promises.push(p2);
          return p2;
        })
        // The query will be retried on the next host
        .then(rs => assert.strictEqual(rs.info.queriedHost, cluster.node(1).address))
        .then(() => {
          assert.strictEqual(firstHost.getInFlight(), 100);
          assert.strictEqual(client.getState().getInFlightQueries(firstHost), 100);
        })
        .then(() => Promise.all(promises))
        .then(() => new Promise(resolve => setImmediate(resolve)))
        .then(() => helper.setIntervalUntilPromise(() => firstHost.getInFlight() === 0, 10, 100))
        .then(() => {
          assert.strictEqual(retryPolicyCalled, 1);
          assert.strictEqual(firstHost.getInFlight(), 0);
          assert.strictEqual(client.getState().getInFlightQueries(firstHost), 0);
        })
        .then(() => client.shutdown());
    });

    it('should reconnect in the background when hosts are back online', async () => {
      const client = newInstance(cluster, {
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(200) },
        pooling: { heartBeatInterval: 50 }
      });

      await client.connect();

      const hosts = client.hosts.values();

      hosts.forEach(h => assert.strictEqual(1, h.pool.connections.length));

      await Promise.all([0, 1, 2].map(n => promiseUtils.fromCallback(cb => cluster.dc(0).node(n).stop(cb))));

      await helper.wait.forAllNodesDown(client);

      assert.deepStrictEqual(client.hosts.values().map(h => h.isUp()), [ false, false, false ]);

      assert.isNull(client.controlConnection.connection);

      // Restart nodes
      await Promise.all([0, 1, 2].map(n => promiseUtils.fromCallback(cb => cluster.dc(0).node(n).start(cb))));

      await helper.wait.forAllNodesUp(client);

      assert.deepStrictEqual(client.hosts.values().map(h => h.isUp()), [ true, true, true ]);
      client.hosts.values().forEach(h => assert.strictEqual(1, h.pool.connections.length));

      // ControlConnection should be refreshed
      await helper.wait.until(() => client.controlConnection.connection);
      assert.isTrue(client.controlConnection.connection.connected);

      await client.shutdown();
    });

    it('should connect when first contact point is down', () => {
      const startIpOctets = simulacron.startingIp.split('.');
      const secondNodeLastOctet = parseInt(startIpOctets[3], 10) + 1;
      const client = new Client({
        contactPoints: [
          startIpOctets.join('.'),
          `${startIpOctets.slice(0, 3).join('.')}.${secondNodeLastOctet}`],
        localDataCenter: 'dc1',
      });

      return promiseUtils.fromCallback(cb => cluster.dc(0).node(0).stop(cb))
        .then(() => client.connect())
        .then(() => assert.strictEqual(
          client.controlConnection.host.address.split(':')[0].split('.')[3],
          secondNodeLastOctet.toString()))
        .then(() => client.shutdown());
    });

    it('should have a single connection when warmup is false', async () => {
      // Use a single contact point to make sure control connection uses the first host
      const contactPoints = [ cluster.getContactPoints()[0] ];

      const client = new Client({
        contactPoints,
        localDataCenter: 'dc1',
        pooling: { warmup: false, coreConnectionsPerHost: { [types.distance.local]: 3 } }
      });

      await client.connect();

      try {
        assert.strictEqual(client.hosts.length, 3);
        const state = client.getState();
        const hosts = state.getConnectedHosts();
        assert.deepStrictEqual(hosts.map(h => h.address), contactPoints);
        assert.strictEqual(state.getOpenConnections(hosts[0]), 1);
      } finally {
        client.shutdown();
      }
    });

    it('should maintain the same ControlConnection host after nodes going UP and DOWN and back UP', () => {
      const firstAddress = cluster.getContactPoints()[0];

      const client = new Client({
        contactPoints: [ firstAddress ],
        localDataCenter: 'dc1',
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(100) },
        pooling: { heartBeatInterval: 50 }
      });

      after(() => client.shutdown());

      let secondAddress;

      return client.connect()
        .then(() => {
          assert.strictEqual(client.getState().getConnectedHosts().length, 3);
          assert.strictEqual(client.controlConnection.getEndpoint(), firstAddress);
        })
        .then(() => promiseUtils.fromCallback(cb => cluster.node(firstAddress).stop(cb)))
        .then(() => helper.setIntervalUntilPromise(
          () => (client.controlConnection.getEndpoint() || firstAddress) !== firstAddress, 20, 5000
        ))
        .then(() => {
          // Validate CC is now connected to another host
          assert.notEqual(client.controlConnection.getEndpoint(), undefined);
          assert.notStrictEqual(client.controlConnection.getEndpoint(), firstAddress);
          secondAddress = client.controlConnection.getEndpoint();
        })
        .then(() => promiseUtils.fromCallback(cb => cluster.node(firstAddress).start(cb)))
        .then(() => helper.setIntervalUntilPromise(
          () => client.hosts.values().find(h => !h.isUp()) === undefined, 20, 5000
        ))
        .then(() => {
          assert.strictEqual(client.getState().getConnectedHosts().length, 3);
          // Validate that events of original node going back UP don't affect the ControlConnection
          assert.strictEqual(client.controlConnection.getEndpoint(), secondAddress);
        })
        .then(() => client.shutdown());
    });

    it('should stop attempting to reconnect to down after shutdown', () => {
      const firstAddress = cluster.getContactPoints()[0];
      const reconnectionDelay = 20;

      const client = new Client({
        contactPoints: [ firstAddress ],
        localDataCenter: 'dc1',
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(reconnectionDelay) },
        pooling: { heartBeatInterval: 50 }
      });

      after(() => client.shutdown());

      // Validate via log messages
      const logMessages = [];

      return client.connect()
        .then(() => {
          assert.strictEqual(client.getState().getConnectedHosts().length, 3);
          assert.strictEqual(client.controlConnection.getEndpoint(), firstAddress);
        })
        .then(() => Promise.all(
          client.hosts.values()
            .map(h => h.address).slice(0, 2)
            .map(address => promiseUtils.fromCallback(cb => cluster.node(address).stop(cb)))
        ))
        .then(() => helper.setIntervalUntilPromise(
          () => (client.controlConnection.getEndpoint() || firstAddress) !== firstAddress, 20, 5000
        ))
        .then(() => {
          assert.notEqual(client.controlConnection.getEndpoint(), undefined);
          assert.notStrictEqual(client.controlConnection.getEndpoint(), firstAddress);
        })
        .then(() => client.shutdown())
        .then(() => client.on('log', (level, message) => logMessages.push([ level, message ])))
        .then(() => new Promise(r => setTimeout(r, reconnectionDelay * 2)))
        .then(() => assert.deepStrictEqual(logMessages, []));
    });

    it('should log the module versions on first connect only', function(done) {
      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        localDataCenter: 'dc1'
      });
      const versionLogRE = /^Connecting to cluster using .+ version (.+)$/;
      let versionMessage = undefined;

      client.on('log', function(level, className, message) {
        const match = message.match(versionLogRE);
        if (match) {
          versionMessage = { level: level, match: match };
        }
      });

      utils.series([
        client.connect.bind(client),
        function ensureLogged(next) {
          assert.ok(versionMessage);
          assert.strictEqual(versionMessage.level, 'info');
          // versions should match those from the modules.
          assert.strictEqual(versionMessage.match[1], version);
          versionMessage = undefined;
          next();
        },
        client.connect.bind(client),
        function ensureNotLogged(next) {
          assert.strictEqual(versionMessage, undefined);
          next();
        },
        client.shutdown.bind(client)
      ],done);
    });

    it('should support providing client id, application name and version', () => {
      const client = new Client(helper.getOptions({
        contactPoints: cluster.getContactPoints(),
        localDataCenter: 'dc1',
        applicationName: 'My App',
        applicationVersion: 'v3.2.1',
        clientId: types.Uuid.random()
      }));

      return client.connect()
        .then(() => client.shutdown());
    });

    it('should create all connections eventually when warmup is false', async () => {
      const connectionsLength = 4;

      const client = newInstance(cluster, {
        pooling: {
          warmup: false,
          coreConnectionsPerHost: { [distance.local]: connectionsLength }
        }
      });

      await client.connect();

      const hosts = client.hosts.values();
      assert.lengthOf(hosts, 3);

      for (const h of hosts) {
        assert.strictEqual(h._distance, distance.local);
      }

      await helper.wait.until(() =>
        hosts.reduce((count, h) => count + h.pool.connections.length, 0) === connectionsLength * hosts.length);

      await client.shutdown();
    });

    [
      {
        name: 'using the control connection',
        fn: (client) => client.controlConnection.query(helper.queries.basic, false)
      }, {
        name: 'using client execute',
        fn: (client) => client.execute(helper.queries.basic)
      }
    ].forEach(item => {
      it(`should reject requests immediately after shutdown ${item.name}`, async () => {
        const client = new Client({
          contactPoints: cluster.getContactPoints(),
          localDataCenter: 'dc1',
          pooling: { heartBeatInterval: 10000 }
        });

        after(() => client.shutdown());

        let stop = false;
        let requestCounter = 0;
        let responseCounter = 0;
        const results = [];
        const maxResults = 1000;
        let failureError = null;

        await client.connect();

        // Execute queries in the background
        process.nextTick(async () => {
          while (!stop && responseCounter < maxResults) {
            // eslint-disable-next-line no-loop-func
            await promiseUtils.times(100, 32, async () => {
              const checkReject = client.isShuttingDown;
              let timePassed = false;

              if (checkReject) {
                setTimeout(() => timePassed = true, 20);
              }

              requestCounter++;
              let err = null;
              try {
                await item.fn(client);
              } catch (e) {
                err = e;
              }

              responseCounter++;
              if (checkReject && results.length < maxResults) {
                results.push({ timePassed, err });
              }

              if (err && !client.isShuttingDown) {
                failureError = err;
              }
            });
          }
        });

        await promiseUtils.delay(20);

        // Shutdown while there are queries in progress
        await client.shutdown();

        await promiseUtils.delay(200);
        stop = true;
        await promiseUtils.delay(200);

        assert.ok(results.length > 0);
        results.forEach(r => {
          helper.assertInstanceOf(r.err, Error);
          assert.strictEqual(r.timePassed, false);
        });
        assert.strictEqual(responseCounter, requestCounter);
        assert.isNull(failureError);
      });
    });
  });

  context('with a simulated cluster containing three nodes on each of the two datacenters', () => {
    let cluster;

    beforeEach(done => {
      cluster = new simulacron.SimulacronCluster();
      cluster.register([3, 3], null, done);
    });

    afterEach(done => cluster.unregister(done));

    it('should not attempt to make a connection on a new node that is on an ignored/remote DC', () => {
      // For the purpose of this test, we will use an address translator that will create the impression
      // that on the second (ignored) DC, there are 2 nodes instead of 3
      // Then, the address translator will show 3 nodes, giving the impression that the third node is "new".

      const node1Address = cluster.dc(1).node(1).address.split(':')[0];
      const node2Address = cluster.dc(1).node(2).address.split(':')[0];
      const addressTranslator = new CustomTestAddressTranslator()
        .withTranslations(new Map([ [ node2Address, node1Address]]));

      const client = new Client({
        contactPoints: [cluster.getContactPoints()[0]],
        localDataCenter: 'dc1',
        policies: {
          addressResolution: addressTranslator,
          reconnection: new policies.reconnection.ConstantReconnectionPolicy(400)
        },
        pooling: {
          coreConnectionsPerHost: {
            [types.distance.local]: 1,
            [types.distance.remote]: 0,
          }
        }
      });

      let initialControlConnectionHost;

      return client.connect()
        .then(() => {
          // The driver is seeing only 5 hosts, instead of 6
          assert.strictEqual(client.hosts.length, 5);
          const lbp = client.options.policies.loadBalancing;

          // 1 connection on local nodes, 0 on remote/ignored hosts
          client.hosts.forEach(host => {
            const expectedConnections = lbp.getDistance(host) === types.distance.local ? 1 : 0;
            assert.strictEqual(host.pool.connections.length, expectedConnections);
          });

          initialControlConnectionHost = client.controlConnection.host;

          // Clear the address translator "test translations"
          addressTranslator.withTranslations(new Map());

          // Stop the node with the control connection to force a refresh
          return promiseUtils.fromCallback(cb => cluster.dc(0).node(0).stop(cb));
        })
        .then(() =>
          // Force the driver to acknowledge that the node is stopped (avoid half open)
          client.execute(helper.queries.basic, null, { host: initialControlConnectionHost }).catch(() => {}))
        .then(() =>
          // Wait until the control connection reconnects to the next node
          helper.setIntervalUntilPromise(() => client.controlConnection.host !== initialControlConnectionHost, 500, 20))
        .then(() =>
          // Allow reconnection
          promiseUtils.fromCallback(cb => cluster.dc(0).node(0).start(cb)))
        .then(() =>
          // Wait for connections to node0 are reestablished
          helper.setIntervalUntilPromise(() => initialControlConnectionHost.pool.connections.length > 0, 500, 40))
        .then(() =>
          // Wait some more just to be sure no connection is being created in the background
          new Promise(r => setTimeout(r, 500)))
        .then(() => {
          // The driver now sees 6 nodes
          assert.strictEqual(client.hosts.length, 6);
          const newNodeAddress = cluster.dc(1).node(2).address;

          // There shouldn't be any connection to the new node
          assert.strictEqual(client.hosts.get(newNodeAddress).pool.connections.length, 0);
        })
        .then(() => client.shutdown());
    });

    it('should use different contactPoints as initial control connection', async () => {

      const addresses = new Set();

      for (let i = 0; i < 20; i++) {
        const client = new Client({
          contactPoints: cluster.getContactPoints(),
          localDataCenter: 'dc1',
          pooling: { warmup: false }
        });

        try {
          await client.connect();
          addresses.add(client.controlConnection.host.address);
        } finally {
          await client.shutdown();
        }
      }

      assert.isAbove(addresses.size, 1);
    });

    it('should not create connections to nodes not selected by the load balancing policy on init', async () => {
      const addresses = cluster
        .dc(0).nodes.map(n => n.address)
        .concat(cluster.dc(1).nodes.slice(0, 1).map(n => n.address));

      const client = newInstance(cluster, {
        policies: { loadBalancing: new helper.OrderedLoadBalancingPolicy(addresses) }
      });

      await client.connect();

      // Wait for some time, as remote connections are created in the background
      await helper.delayAsync(500);

      assert.deepStrictEqual(client.getState().getConnectedHosts().map(h => h.address).sort(), addresses.sort());

      await client.shutdown();
    });

    it('should not create connections to remote nodes when core connections per host is zero', async () => {
      const localAddresses = cluster.dc(0).nodes.map(n => n.address);
      const remoteAddresses = cluster.dc(1).nodes.slice(0, 2).map(n => n.address);

      const client = newInstance(cluster, {
        policies: { loadBalancing: new CustomLoadBalancingPolicy(localAddresses, remoteAddresses) },
        pooling: { coreConnectionsPerHost: { [types.distance.local]: 2, [types.distance.remote]: 0 }}
      });

      await client.connect();

      // Wait for some time, as remote connections are created in the background
      await helper.delayAsync(500);

      // Use private property to validate that two nodes are considered remote
      assert.lengthOf(client.hosts.values().filter(h => h._distance === types.distance.remote), 2);

      // Only connections to local hosts
      assert.deepStrictEqual(
        client.getState().getConnectedHosts().map(h => h.address).sort(),
        localAddresses.sort());

      await client.shutdown();
    });

    [true, false].forEach(warmup => {
      it(`should create connections to remote nodes in the background when warmup is ${warmup}`, async () => {
        const localAddresses = cluster.dc(0).nodes.map(n => n.address);
        const remoteAddress = cluster.dc(1).node(0).address;

        const client = newInstance(cluster, {
          policies: { loadBalancing: new CustomLoadBalancingPolicy(localAddresses, [ remoteAddress ]) },
          pooling: {
            coreConnectionsPerHost: { [types.distance.local]: 2, [types.distance.remote]: 1 },
            warmup
          }
        });

        await client.connect();

        // Wait for some time, as remote connections are created in the background
        await helper.delayAsync(500);

        const remoteHost = client.hosts.get(remoteAddress);
        const localHostsHosts = localAddresses.map(a => client.hosts.get(a));

        // Two connections to local hosts
        localHostsHosts.forEach(h => assert.deepStrictEqual(client.getState().getOpenConnections(h), 2));

        await helper.wait.until(() => client.getState().getOpenConnections(remoteHost) === 1);

        await client.shutdown();
      });
    });
  });
});

function assertArray(arr, value) {
  assert.deepStrictEqual(arr, new Array(arr.length).fill(null).map(() => value));
}

/**
 * Asserts that all nodes are UP and that the LBP is selecting the first healthy node.
 */
function validateStateOfPool(client, cluster) {
  return client.execute('SELECT * FROM system.local')
    .then(rs => {
      // Assert it didn't affect the state of the pool
      assertArray(client.hosts.values().map(h => h.isUp()), true);
      return assert.strictEqual(rs.info.queriedHost, cluster.node(0).address);
    });
}

/**
 * An address translator that returns the same address, except for the ones provided, suitable for testing.
 */
class CustomTestAddressTranslator extends policies.addressResolution.AddressTranslator {
  constructor() {
    super();
    this._addressesMap = new Map();
  }

  /** @param {Map} addressesMap */
  withTranslations(addressesMap) {
    this._addressesMap = addressesMap;
    return this;
  }

  translate(address, port, callback) {
    const translatedAddress = this._addressesMap.get(address);

    if (translatedAddress !== undefined) {
      address = translatedAddress;
    }

    callback(`${address}:${port}`);
  }
}

/**
 * A load balancing policy suitable for testing.
 */
class CustomLoadBalancingPolicy extends policies.loadBalancing.LoadBalancingPolicy {
  constructor(localAddresses, remoteAddresses) {
    super();

    this.localAddresses = localAddresses;
    this.remoteAddresses = remoteAddresses || [];
  }

  getDistance(host) {
    if (this.localAddresses.indexOf(host.address) >= 0) {
      return types.distance.local;
    }

    if (this.remoteAddresses.indexOf(host.address) >= 0) {
      return types.distance.remote;
    }

    return types.distance.ignored;
  }

  newQueryPlan(keyspace, executionOptions, callback) {
    callback(null, this._queryPlan());
  }

  *_queryPlan() {
    yield* utils.shuffleArray(this.localAddresses.map(a => this.hosts.get(a)));
    yield* utils.shuffleArray(this.remoteAddresses.map(a => this.hosts.get(a)));
  }
}

function newInstance(simulacronCluster, options) {
  const client = new Client(
    Object.assign({
      contactPoints: simulacronCluster.getContactPoints(),
      localDataCenter: 'dc1'
    },
    options));

  helper.shutdownAfterThisTest(client);

  return client;
}