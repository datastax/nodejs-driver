"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var loadBalancing = require('../../../lib/policies/load-balancing.js');
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
var TokenAwarePolicy = loadBalancing.TokenAwarePolicy;

describe('DCAwareRoundRobinPolicy', function () {
  this.timeout(180000);
  it('should never hit remote dc if not set', function (done) {
    var countByHost = {};
    utils.series([
      //1 cluster with 3 dcs with 2 nodes each
      helper.ccmHelper.start('2:2:2') ,
      function testCase(next) {
        var options = utils.deepExtend({}, helper.baseOptions, {policies: {loadBalancing: new DCAwareRoundRobinPolicy()}});
        var client = new Client(options);
        var prevHost = null;
        utils.times(120, function (n, timesNext) {
          client.execute(helper.queries.basic, function (err, result) {
            assert.ifError(err);
            assert.ok(result && result.rows);
            var hostId = result.info.queriedHost;
            assert.ok(hostId);
            var h = client.hosts.get(hostId);
            assert.ok(h);
            assert.strictEqual(h.datacenter, 'dc1');
            prevHost = h;
            countByHost[hostId] = (countByHost[hostId] || 0) + 1;
            timesNext();
          });
        }, next);
      },
      function assertHosts(next) {
        var hostsQueried = Object.keys(countByHost);
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
  it('should use primary replica according to Murmur multiple dc', function (done) {
    var keyspace = 'ks1';
    var table = 'table1';
    utils.series([
      helper.ccmHelper.start('3:3'),
      function createKs(next) {
        var client = new Client(helper.baseOptions);
        var createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
        client.execute(util.format(createQuery, keyspace, 3, 3), helper.waitSchema(client, next));
      },
      function createTable(next) {
        var client = new Client(helper.baseOptions);
        var query = util.format('CREATE TABLE %s.%s (id int primary key, name int)', keyspace, table);
        client.execute(query, helper.waitSchema(client, next));
      },
      function testCase(next) {
        //Pre-calculated based on Murmur
        //This test can be improved using query tracing, consistency all and checking hops
        var expectedPartition = {
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
        var client = new Client({
          policies: { loadBalancing: new TokenAwarePolicy(new DCAwareRoundRobinPolicy())},
          keyspace: keyspace,
          contactPoints: helper.baseOptions.contactPoints
        });
        utils.times(100, function (n, timesNext) {
          var id = (n % 10) + 1;
          var query = util.format('INSERT INTO %s (id, name) VALUES (%s, %s)', table, id, id);
          client.execute(query, null, {routingKey: new Buffer([0, 0, 0, id])}, function (err, result) {
            assert.ifError(err);
            //for murmur id = 1, it go to replica 2
            var address = result.info.queriedHost;
            assert.strictEqual(helper.lastOctetOf(address), expectedPartition[id.toString()]);
            timesNext();
          });
        }, next);
      },
      helper.ccmHelper.remove
    ], done);
  });
  it('should yield local replicas plus childPolicy plus remote replicas', function (done) {
    var keyspace1 = 'ks1';
    var keyspace2 = 'ks2';
    var createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";var client = new Client({
      policies: { loadBalancing: new TokenAwarePolicy(new DCAwareRoundRobinPolicy())},
      contactPoints: helper.baseOptions.contactPoints
    });
    /** @type {LoadBalancingPolicy} */
    var policy = client.options.policies.loadBalancing;
    var localDc = 'dc1';
    utils.series([
      helper.ccmHelper.start('4:4'),
      function createKs1(next) {
        var localClient = new Client(helper.baseOptions);
        localClient.execute(util.format(createQuery, keyspace1, 3, 3), helper.waitSchema(localClient, next));
      },
      function createKs1(next) {
        var localClient = new Client(helper.baseOptions);
        localClient.execute(util.format(createQuery, keyspace2, 1, 1), helper.waitSchema(localClient, next));
      },
      client.connect.bind(client),
      function testCase1(next) {
        utils.times(20, function (n, timesNext) {
          //keyspace 1
          policy.newQueryPlan(keyspace1, {routingKey: new Buffer([0, 0, 0, 1])}, function (err, iterator) {
            var hosts = helper.iteratorToArray(iterator);
            //6 replicas plus an additional local node
            assert.ok(hosts.length, 7);
            assert.strictEqual(hosts[0].datacenter, localDc);
            assert.strictEqual(hosts[1].datacenter, localDc);
            assert.strictEqual(hosts[2].datacenter, localDc);
            assert.strictEqual(hosts[3].datacenter, localDc);
            timesNext();
          });
        }, next);
      },
      function testCase2(next) {
        utils.times(20, function (n, timesNext) {
          //keyspace 2
          policy.newQueryPlan(keyspace2, {routingKey: new Buffer([0, 0, 0, 1])}, function (err, iterator) {
            var hosts = helper.iteratorToArray(iterator);
            //2 replicas plus 3 additional local nodes
            assert.ok(hosts.length, 5);
            assert.strictEqual(hosts[0].datacenter, localDc);
            assert.strictEqual(hosts[1].datacenter, localDc);
            assert.strictEqual(hosts[2].datacenter, localDc);
            assert.strictEqual(hosts[3].datacenter, localDc);
            timesNext();
          });
        }, next);
      },
      function testCase3(next) {
        utils.times(20, function (n, timesNext) {
          //no keyspace
          policy.newQueryPlan(null, {routingKey: new Buffer([0, 0, 0, 1])}, function (err, iterator) {
            var hosts = helper.iteratorToArray(iterator);
            //1 (closest) replica plus 3 additional local nodes
            assert.ok(hosts.length, 4);
            assert.strictEqual(hosts[0].datacenter, localDc);
            assert.strictEqual(hosts[1].datacenter, localDc);
            assert.strictEqual(hosts[2].datacenter, localDc);
            assert.strictEqual(hosts[3].datacenter, localDc);
            timesNext();
          });
        }, next);
      },
      helper.ccmHelper.remove
    ], done);
  });
});