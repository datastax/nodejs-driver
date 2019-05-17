/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const utils = require('../../../lib/utils.js');
const loadBalancing = require('../../../lib/policies/load-balancing.js');
const DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
const TokenAwarePolicy = loadBalancing.TokenAwarePolicy;

describe('DCAwareRoundRobinPolicy', function () {
  this.timeout(180000);
  it('should never hit remote dc if not set', function (done) {
    const countByHost = {};
    utils.series([
      //1 cluster with 3 dcs with 2 nodes each
      helper.ccmHelper.start('2:2:2') ,
      function testCase(next) {
        const options = utils.deepExtend({}, helper.baseOptions, {policies: {loadBalancing: new DCAwareRoundRobinPolicy()}});
        const client = new Client(options);
        utils.times(120, function (n, timesNext) {
          client.execute(helper.queries.basic, function (err, result) {
            assert.ifError(err);
            assert.ok(result && result.rows);
            const hostId = result.info.queriedHost;
            assert.ok(hostId);
            const h = client.hosts.get(hostId);
            assert.ok(h);
            assert.strictEqual(h.datacenter, 'dc1');
            countByHost[hostId] = (countByHost[hostId] || 0) + 1;
            timesNext();
          });
        }, next);
      },
      function assertHosts(next) {
        const hostsQueried = Object.keys(countByHost);
        assert.strictEqual(hostsQueried.length, 2);
        assert.strictEqual(countByHost[hostsQueried[0]], countByHost[hostsQueried[1]]);
        next();
      },
      helper.ccmHelper.remove
    ], done);
  });
});
describe('TokenAwarePolicy', function () {
  this.timeout(120000);
  describe('with a 3:3 node topology', function() {
    const keyspace = 'ks1';
    const table = 'table1';
    const client = new Client({
      policies: { loadBalancing: new TokenAwarePolicy(new DCAwareRoundRobinPolicy())},
      keyspace: keyspace,
      contactPoints: helper.baseOptions.contactPoints
    });

    before(function (done) {
      const localClient = new Client(helper.baseOptions);
      utils.series([
        helper.ccmHelper.start('3:3'),
        localClient.connect.bind(localClient),
        function createKs(next) {
          const createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
          localClient.execute(util.format(createQuery, keyspace, 1, 1), helper.waitSchema(localClient, next));
        },
        function createTable(next) {
          const query = util.format('CREATE TABLE %s.%s (id int primary key, name int)', keyspace, table);
          localClient.execute(query, helper.waitSchema(localClient, next));
        },
        localClient.shutdown.bind(localClient)
      ], done);
    });
    after(function (done) {
      utils.series([
        helper.ccmHelper.remove,
        client.shutdown.bind(client)
      ], done);
    });
    it('should use primary replica according to murmur multiple dc', function (done) {
      //Pre-calculated based on Murmur
      //This test can be improved using query tracing, consistency all and checking hops
      const expectedPartition = {
        '1': '2',
        '2': '2',
        '3': '1',
        '4': '3',
        '5': '2',
        '6': '3',
        '7': '3',
        '8': '2',
        '9': '1',
        '10': '2'
      };
      utils.times(100, function (n, timesNext) {
        const id = (n % 10) + 1;
        const query = util.format('INSERT INTO %s (id, name) VALUES (%s, %s)', table, id, id);
        client.execute(query, null, {routingKey: utils.allocBufferFromArray([0, 0, 0, id])}, function (err, result) {
          assert.ifError(err);
          //for murmur id = 1, it go to replica 2
          const address = result.info.queriedHost;
          assert.strictEqual(helper.lastOctetOf(address), expectedPartition[id.toString()]);
          timesNext();
        });
      }, done);
    });
  });
  describe('with a 4:4 node topology', function() {
    const keyspace1 = 'ks1';
    const keyspace2 = 'ks2';
    // Resolves to token -4069959284402364209 which should have primary replica of 3 and 7 with 3 being the closest replica.
    const routingKey = utils.allocBufferFromArray([0, 0, 0, 1]);

    const client_dc2 = new Client({
      policies: { loadBalancing: new TokenAwarePolicy(new DCAwareRoundRobinPolicy())},
      contactPoints: ['127.0.0.5'] // choose a host in dc2, for closest replica local selection validation.
    });
    const policy_dc2 = client_dc2.options.policies.loadBalancing;

    const client_dc1 = new Client({
      policies: { loadBalancing: new TokenAwarePolicy(new DCAwareRoundRobinPolicy())},
      contactPoints: ['127.0.0.1']
    });
    const policy_dc1 = client_dc1.options.policies.loadBalancing;
    const localDc = 'dc2';

    before(function (done) {
      const createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
      /** @type {LoadBalancingPolicy} */
      utils.series([
        helper.ccmHelper.start('4:4'),
        function createKs1(next) {
          client_dc1.execute(util.format(createQuery, keyspace1, 2, 2), helper.waitSchema(client_dc1, next));
        },
        function createKs2(next) {
          client_dc1.execute(util.format(createQuery, keyspace2, 1, 1), helper.waitSchema(client_dc1, next));
        },
        client_dc2.connect.bind(client_dc2)
      ], done);
    });
    after(function (done) {
      utils.series([
        helper.ccmHelper.remove,
        client_dc1.shutdown.bind(client_dc1),
        client_dc2.shutdown.bind(client_dc2)
      ], done);
    });

    it('should yield 2 local replicas first, then 2 remaining local nodes when RF is 2', function (done) {
      utils.times(20, function (n, timesNext) {
        //keyspace 1
        policy_dc2.newQueryPlan(keyspace1, {routingKey: routingKey}, function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
          // 2 local replicas first, 2 remaining local nodes.
          assert.ok(hosts.length, 4);
          hosts.forEach(function(host) {
            assert.strictEqual(host.datacenter, localDc);
          });
          // the local replicas should be 7 (primary) and 8 in dc2.
          const replicas = hosts.slice(0,2).map(helper.lastOctetOf).sort();
          assert.deepEqual(replicas, ['7', '8']);
          timesNext();
        });
      }, done);
    });
    it('should yield 1 local replica first, then 3 remaining local nodes when RF is 1', function (done) {
      utils.times(20, function (n, timesNext) {
        //keyspace 2
        policy_dc2.newQueryPlan(keyspace2, {routingKey: routingKey}, function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
          // 1 local replica, 3 remaining local nodes.
          assert.ok(hosts.length, 4);
          hosts.forEach(function(host) {
            assert.strictEqual(host.datacenter, localDc);
          });
          // the local replicas should be 3 (primary).
          const replicas = hosts.slice(0,1).map(helper.lastOctetOf).sort();
          assert.deepEqual(replicas, ['7']);
          timesNext();
        });
      }, done);
    });
    it('should yield closest replica first (when same DC), then 3 remaining local nodes when RF is 1', function (done) {
      utils.times(20, function (n, timesNext) {
        //no keyspace
        policy_dc1.newQueryPlan(null, {routingKey: routingKey}, function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
          //1 (closest) replica irrespective of keyspace topology, plus 3 additional local nodes
          assert.ok(hosts.length, 4);
          hosts.forEach(function(host) {
            assert.strictEqual(host.datacenter, 'dc1');
          });
          // the local replicas should be 3 (primary).
          const replicas = hosts.slice(0,1).map(helper.lastOctetOf).sort();
          assert.deepEqual(replicas, ['3']);
          timesNext();
        });
      }, done);
    });
    it('should not yield closest replica when not in same DC as local', function (done) {
      utils.times(20, function (n, timesNext) {
        //no keyspace
        policy_dc2.newQueryPlan(null, {routingKey: routingKey}, function (err, iterator) {
          const hosts = helper.iteratorToArray(iterator);
          // Should simply get all local nodes, since no keyspace was provided and the closest
          // replica is in dc1, no token aware ordering is provided.
          assert.ok(hosts.length, 4);
          hosts.forEach(function(host) {
            assert.strictEqual(host.datacenter, localDc);
          });
          timesNext();
        });
      }, done);
    });
  });
});