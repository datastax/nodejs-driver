/**
 * Copyright (C) DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const assert = require('assert');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const errors = require('../../../lib/errors');
const types = require('../../../lib/types');
const policies = require('../../../lib/policies');

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
      policies: { loadBalancing: new helper.OrderedLoadBalancingPolicy() },
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
      then: { result: 'success', delay_in_ms: 1000 }
    }, done));
    beforeEach(done => cluster.node(0).prime({
      when: { query: 'SELECT * FROM paused_on_first' },
      then: { result: 'success', delay_in_ms: 1000 }
    }, done));
    afterEach(() => client.shutdown());
    afterEach(done => cluster.unregister(done));

    it('should use next host when a host is busy', function () {
      return client.connect()
        .then(() => Promise.all(new Array(150).fill(null).map(() => client.execute('SELECT * FROM paused'))))
        .then(results => {
          const hosts = client.hosts.keys();
          // First 100 items should be the first host (50 * 2 connections)
          assertArray(results.slice(0, 100).map(rs => rs.info.queriedHost), hosts[0]);
          // The next 50 items should be the second host
          assertArray(results.slice(100).map(rs => rs.info.queriedHost), hosts[1]);
        })
        .then(() => validateStateOfPool(client));
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
          const hosts = client.hosts.keys();

          // The first 300 requests should have succeed
          assertArray(results.slice(0, 100).map(r => r.coordinator), hosts[0]);
          assertArray(results.slice(100, 200).map(r => r.coordinator), hosts[1]);
          assertArray(results.slice(200, 300).map(r => r.coordinator), hosts[2]);

          // The last 50 items should have failed (rejected)
          results.slice(300).forEach(r => {
            assert.strictEqual(r.coordinator, null);
            helper.assertInstanceOf(r.err, errors.NoHostAvailableError);
            assert.deepEqual(Object.keys(r.err.innerErrors).sort(), hosts.sort());
            helper.values(r.err.innerErrors).forEach(err => {
              helper.assertInstanceOf(err, errors.BusyConnectionError);
            });
          });
        })
        .then(() => validateStateOfPool(client));
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
          loadBalancing: new helper.OrderedLoadBalancingPolicy(),
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
        .then(() => firstHost = client.hosts.values()[0].address)
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
          loadBalancing: new helper.OrderedLoadBalancingPolicy(),
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
          firstHost = client.hosts.values()[0];

          // Create 99 in-flight requests
          promises.push.apply(promises, new Array(99).fill(0).map(() =>
            client.execute('SELECT * FROM paused_on_first', null, { readTimeout: 2000 })));

          // The pool will be busy after this request, so the retry will occur on the next host
          const p2 = client.execute('SELECT * FROM paused_on_first', null, { readTimeout: 50, isIdempotent: true });
          promises.push(p2);
          return p2;
        })
        // The query will be retried on the next host
        .then(rs => assert.strictEqual(rs.info.queriedHost, client.hosts.values()[1].address))
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

    it('should reconnect in the background when hosts are back online', () => {
      const client = new Client({
        contactPoints: cluster.getContactPoints(),
        localDataCenter: 'dc1',
        policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(200) },
        pooling: { heartBeatInterval: 50 }
      });

      return client.connect()
        .then(() => client.hosts.values().forEach(h => assert.strictEqual(1, h.pool.connections.length)))
        .then(() => Promise.all([0, 1, 2].map(n => promiseFromCallback(cb => cluster.dc(0).node(n).stop(cb)))))
        .then(() => helper.setIntervalUntilPromise(
          // Validate all nodes are down
          () => client.hosts.values().reduce((acc, h) => acc && !h.isUp(), true), 20, 5000
        ))
        .then(() => assert.deepStrictEqual(client.hosts.values().map(h => h.isUp()), [ false, false, false ]))
        .then(() => Promise.all([0, 1, 2].map(n => promiseFromCallback(cb => cluster.dc(0).node(n).start(cb)))))
        .then(() => helper.setIntervalUntilPromise(
          // Validate all nodes are UP
          () => client.hosts.values().reduce((acc, h) => acc && h.isUp(), true), 20, 5000
        ))
        .then(() => assert.deepStrictEqual(client.hosts.values().map(h => h.isUp()), [ true, true, true ]))
        .then(() => client.hosts.values().forEach(h => assert.strictEqual(1, h.pool.connections.length)))
        .then(() => new Promise(r => setTimeout(r, 400)))
        .then(() => client.shutdown());
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

      return promiseFromCallback(cb => cluster.dc(0).node(0).stop(cb))
        .then(() => client.connect())
        .then(() => assert.strictEqual(
          client.controlConnection.host.address.split(':')[0].split('.')[3],
          secondNodeLastOctet.toString()))
        .then(() => client.shutdown());
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
          return promiseFromCallback(cb => cluster.dc(0).node(0).stop(cb));
        })
        .then(() =>
          // Force the driver to acknowledge that the node is stopped (avoid half open)
          client.execute(helper.queries.basic, null, { host: initialControlConnectionHost }).catch(() => {}))
        .then(() =>
          // Wait until the control connection reconnects to the next node
          helper.setIntervalUntilPromise(() => client.controlConnection.host !== initialControlConnectionHost, 500, 20))
        .then(() =>
          // Allow reconnection
          promiseFromCallback(cb => cluster.dc(0).node(0).start(cb)))
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

    it('should use different contactPoints as initial control connection', () =>
      helper.repeat(20, () => {
        const client = new Client({
          contactPoints: cluster.getContactPoints(),
          localDataCenter: 'dc1'
        });

        let address;

        return client.connect()
          .then(() => address = client.controlConnection.host.address)
          .then(() => client.shutdown())
          .then(() => address);

      }).then(addresses => assert.ok(new Set(addresses).size > 0))
    );
  });
});

function assertArray(arr, value) {
  assert.deepStrictEqual(arr, new Array(arr.length).fill(null).map(() => value));
}

/**
 * Asserts that all nodes are UP and that the LBP is selecting the first healthy node.
 */
function validateStateOfPool(client) {
  return client.execute('SELECT * FROM system.local')
    .then(rs => {
      // Assert it didn't affect the state of the pool
      assertArray(client.hosts.values().map(h => h.isUp()), true);
      return assert.strictEqual(rs.info.queriedHost, client.hosts.keys()[0]);
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

function promiseFromCallback(handler) {
  return new Promise((resolve, reject) => {
    handler((err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}