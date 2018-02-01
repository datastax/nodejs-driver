'use strict';

const assert = require('assert');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const errors = require('../../../lib/errors');
const types = require('../../../lib/types');

const Client = require('../../../lib/client');

describe('pool', function () {

  this.timeout(5000);
  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  describe('with a 3-node simulated cluster', function () {
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