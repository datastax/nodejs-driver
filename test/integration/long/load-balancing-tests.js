var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types.js');
var utils = require('../../../lib/utils.js');
var loadBalancing = require('../../../lib/policies/load-balancing.js');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var DCAwareRoundRobinPolicy = loadBalancing.DCAwareRoundRobinPolicy;
var TokenAwarePolicy = loadBalancing.TokenAwarePolicy;


describe('DCAwareRoundRobinPolicy', function () {
  this.timeout(120000);
  it('should never hit remote dc if not set', function (done) {
    var countByHost = {};
    async.series([
      //1 cluster with 3 dcs with 2 nodes each
      helper.ccmHelper.start('2:2:2') ,
      function testCase(next) {
        var options = utils.deepExtend({}, helper.baseOptions, {policies: {loadBalancing: new DCAwareRoundRobinPolicy()}});
        var client = new Client(options);
        var prevHost = null;
        async.times(120, function (n, timesNext) {
          client.execute('SELECT * FROM system.schema_columnfamilies', function (err, result) {
            assert.ifError(err);
            assert.ok(result && result.rows);
            var hostId = result._queriedHost;
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
  it('should use primary replica according to Murmur single dc', function (done) {
    var keyspace = 'ks1';
    var table = 'table1';
    async.series([
      helper.ccmHelper.start('3'),
      function createKs(next) {
        var client = new Client(helper.baseOptions);
        client.execute(helper.createKeyspaceCql(keyspace, 1), helper.waitSchema(client, next));
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
          policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy())},
          keyspace: keyspace,
          contactPoints: helper.baseOptions.contactPoints
        });
        async.times(100, function (n, timesNext) {
          var id = (n % 10) + 1;
          var query = util.format('INSERT INTO %s (id, name) VALUES (%s, %s)', table, id, id);
          client.execute(query, null, {routingKey: new Buffer([0, 0, 0, id])}, function (err, result) {
            assert.ifError(err);
            //for murmur id = 1, it go to replica 2
            var address = result._queriedHost;
            assert.strictEqual(address.charAt(address.length-1), expectedPartition[id.toString()]);
            timesNext();
          });
        }, next);
      },
      helper.ccmHelper.remove
    ], done);
  });
});