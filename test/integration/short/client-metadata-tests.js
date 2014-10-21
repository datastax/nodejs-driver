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

describe('Client', function () {
  this.timeout(240000);
  describe('#getReplicas() with Murmur', function () {
    before(function (done) {
      var client = new Client(helper.baseOptions);
      var createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
      async.series([
        helper.ccmHelper.start('4:4', {sleep: 1000}),
        function (next) {
          client.execute(util.format(createQuery, 'sampleks1', 2, 2), next);
        },
        function (next) {
          client.execute(util.format(createQuery, 'sampleks2', 3, 1), next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should get the local and remote replicas for a given keyspace', function (done) {
      var client = new Client(helper.baseOptions);
      client.connect(function (err) {
        assert.ifError(err);
        var replicas = client.getReplicas('sampleks1', new Buffer([0, 0, 0, 1]));
        assert.ok(replicas);
        //2 replicas per each dc
        assert.strictEqual(replicas.length, 4);
        assert.strictEqual(replicas.reduce(function (val, h) { return val += (h.datacenter === 'dc1' ? 1 : 0)}, 0), 2);
        //pre-calculated based on murmur3
        assert.strictEqual(replicas[0].address.charAt(replicas[0].address.length-1), '3');
        assert.strictEqual(replicas[1].address.charAt(replicas[1].address.length-1), '7');
        assert.strictEqual(replicas[2].address.charAt(replicas[2].address.length-1), '4');
        assert.strictEqual(replicas[3].address.charAt(replicas[3].address.length-1), '8');

        replicas = client.getReplicas('sampleks1', new Buffer([0, 0, 0, 3]));
        assert.ok(replicas);
        //2 replicas per each dc
        assert.strictEqual(replicas.length, 4);
        assert.strictEqual(replicas.reduce(function (val, h) { return val += (h.datacenter === 'dc1' ? 1 : 0)}, 0), 2);
        //pre-calculated based on murmur3
        assert.strictEqual(replicas[0].address.charAt(replicas[0].address.length-1), '1');
        assert.strictEqual(replicas[1].address.charAt(replicas[1].address.length-1), '5');
        assert.strictEqual(replicas[2].address.charAt(replicas[2].address.length-1), '2');
        assert.strictEqual(replicas[3].address.charAt(replicas[3].address.length-1), '6');
        done();
      });
    });
    it('should get the closest replica if no keyspace specified', function (done) {
      var client = new Client(helper.baseOptions);
      client.connect(function (err) {
        assert.ifError(err);
        var replicas = client.getReplicas(null, new Buffer([0, 0, 0, 1]));
        assert.ok(replicas);
        assert.strictEqual(replicas.length, 1);
        //pre-calculated based on murmur3
        assert.strictEqual(replicas[0].address.charAt(replicas[0].address.length-1), '3');
        done();
      });
    });
  });
  describe('#getReplicas() with ByteOrder', function () {
    var client = new Client(helper.baseOptions);
    var ccmOptions = {
      vnodes: true,
      yaml: ['partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner']
    };
    before(helper.ccmHelper.start('2', ccmOptions));
    after(helper.ccmHelper.remove);
    it('should get the replica', function (done) {
      function compareReplicas(val, expectedReplica) {
        var replicas = client.getReplicas(null, val);
        assert.ok(replicas);
        //2 replicas per each dc
        assert.strictEqual(replicas.length, 1);
        //pre-calculated based on Byte ordered partitioner
        assert.strictEqual(replicas[0].address, expectedReplica.address);
      }
      var client = new Client(helper.baseOptions);
      client.connect(function (err) {
        assert.ifError(err);
        for (var i = 0; i < client.metadata.ring.length; i++) {
          var token = client.metadata.ring[i];
          var replica = client.metadata.primaryReplicas[client.metadata.tokenizer.stringify(token)];
          compareReplicas(token, replica);
        }
        done();
      });
    });
  });
});