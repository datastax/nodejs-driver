'use strict';

const assert = require('assert');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const errors = require('../../../lib/errors');
const types = require('../../../lib/types');
const policies = require('../../../lib/policies');

const Client = require('../../../lib/client');

describe('pool', function () {

  this.timeout(20000);
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
        maxRequestsPerConnection: 50
      }
    }));
    beforeEach(done => cluster.prime({
      when: { query: 'SELECT * FROM paused' },
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
  });

  context('with a cluster containing three nodes on each of the two datacenters', () => {
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
        contactPoints: cluster.getContactPoints(),
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
        .then(() => {
          // The driver now sees 6 nodes
          assert.strictEqual(client.hosts.length, 6);
          const newNodeAddress = cluster.dc(1).node(2).address;

          // There shouldn't be any connection to the new node
          assert.strictEqual(client.hosts.get(newNodeAddress).pool.connections.length, 0);
        })
        .then(() => client.shutdown());
    });
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