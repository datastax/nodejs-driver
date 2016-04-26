"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var loadBalancing = require('../../../lib/policies/load-balancing.js');
var RoundRobinPolicy = loadBalancing.RoundRobinPolicy;
var TokenAwarePolicy = loadBalancing.TokenAwarePolicy;
var WhiteListPolicy = loadBalancing.WhiteListPolicy;


describe('TokenAwarePolicy', function () {
  this.timeout(180000);
  it('should use primary replica according to Murmur single dc', function (done) {
    var keyspace = 'ks1';
    var table = 'table1';
    var testClient = new Client(helper.baseOptions);
    utils.series([
      helper.ccmHelper.start('3'),
      testClient.connect.bind(testClient),
      function createKs(next) {
        testClient.execute(helper.createKeyspaceCql(keyspace, 1), next);
      },
      function createTable(next) {
        var query = util.format('CREATE TABLE %s.%s (id int primary key, name int)', keyspace, table);
        testClient.execute(query, next);
      },
      testClient.shutdown.bind(testClient),
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
        utils.times(100, function (n, timesNext) {
          var id = (n % 10) + 1;
          var query = util.format('INSERT INTO %s (id, name) VALUES (%s, %s)', table, id, id);
          client.execute(query, null, { routingKey: new Buffer([0, 0, 0, id])}, function (err, result) {
            assert.ifError(err);
            //for murmur id = 1, it go to replica 2
            var address = result.info.queriedHost;
            assert.strictEqual(helper.lastOctetOf(address), expectedPartition[id.toString()]);
            timesNext();
          });
        }, helper.finish(client, next));
      },
      helper.ccmHelper.remove
    ], done);
  });
  it('should target the correct partition', function (done) {
    var keyspace1 = 'ks1';
    var keyspace2 = 'ks2';
    var table = 'table1';

    function testCase(ks, next) {
      var client = new Client({
        policies: { loadBalancing: new TokenAwarePolicy(new RoundRobinPolicy())},
        keyspace: ks,
        contactPoints: helper.baseOptions.contactPoints
      });
      utils.timesSeries(10, function (n, timesNext) {
        var id = (n % 10) + 1;
        var query = util.format('INSERT INTO %s (id, name) VALUES (?, ?)', table);
        client.execute(query, [id, id], { traceQuery: true, prepare: true}, function (err, result) {
          assert.ifError(err);
          var coordinator = result.info.queriedHost;
          var traceId = result.info.traceId;
          client.metadata.getTrace(traceId, function (err, trace) {
            assert.ifError(err);
            trace.events.forEach(function (event) {
              //no network hops in the server
              assert.strictEqual(
                helper.lastOctetOf(event['source'].toString()),
                helper.lastOctetOf(coordinator.toString()));
            });
            timesNext();
          });
        });
      }, helper.finish(client, next));
    }

    var testClient = new Client(helper.baseOptions);
    utils.series([
      helper.ccmHelper.start('3'),
      testClient.connect.bind(testClient),
      function createKs1(next) {
        testClient.execute(helper.createKeyspaceCql(keyspace1, 1), next);
      },
      function createKs2(next) {
        var query = util.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', " +
          "'%s' : %d}", keyspace2, testClient.hosts.values()[0].datacenter, 1);
        testClient.execute(query, next);
      },
      function createTable1(next) {
        var query = util.format('CREATE TABLE %s.%s (id int primary key, name int)', keyspace1, table);
        testClient.execute(query, next);
      },
      function createTable2(next) {
        var query = util.format('CREATE TABLE %s.%s (id int primary key, name int)', keyspace2, table);
        testClient.execute(query, next);
      },
      function testCaseWithKs1(next) {
        testCase(keyspace1, next);
      },
      function testCaseWithKs2(next) {
        testCase(keyspace2, next);
      },
      testClient.shutdown.bind(testClient),
      helper.ccmHelper.remove
    ], done);
  });
});

describe('WhiteListPolicy', function () {
  this.timeout(180000);
  before(helper.ccmHelper.start(3));
  after(helper.ccmHelper.remove);
  it('should use the hosts in the white list only', function (done) {
    var policy = new WhiteListPolicy(new RoundRobinPolicy(), ['127.0.0.1:9042', '127.0.0.2:9042']);
    var client = newInstance(policy);
    utils.timesLimit(100, 20, function (n, next) {
      client.execute('SELECT * FROM system.local', function (err, result) {
        assert.ifError(err);
        var lastOctet = helper.lastOctetOf(result.info.queriedHost);
        assert.ok(lastOctet === '1' || lastOctet === '2');
        next();
      });
    }, function (err) {
      assert.ifError(err);
      client.shutdown(done);
    });
  });
});

function newInstance(policy) {
  var options = utils.extend({}, helper.baseOptions, { policies: { loadBalancing: policy}});
  return new Client(options);
}