/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var tokenizer = require('../../../lib/tokenizer');

describe('Client', function () {
  this.timeout(240000);
  describe('#getReplicas() with MurmurPartitioner', function () {
    before(function (done) {
      var client = newInstance();
      var createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
      utils.series([
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
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        validateMurmurReplicas(client);
        done();
      });
    });
    it('should get the local and remote replicas for a given keyspace if isMetadataSyncEnabled is false but keyspace metadata is present', function (done) {
      var client = newInstance({ isMetadataSyncEnabled: false });
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.refreshKeyspace('sampleks1', function(err, keyspace) {
          assert.ifError(err);
          assert.strictEqual('sampleks1', keyspace.name);
          validateMurmurReplicas(client);
          done();
        });
      });
    });
    it('should return null if keyspace metadata is not present', function (done) {
      var client = newInstance({ isMetadataSyncEnabled: false });
      client.connect(function (err) {
        assert.ifError(err);
        var replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 1]));
        assert.strictEqual(null, replicas);
        done();
      });
    });
    it('should get the closest replica if no keyspace specified', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var replicas = client.getReplicas(null, utils.allocBufferFromArray([0, 0, 0, 1]));
        assert.ok(replicas);
        assert.strictEqual(replicas.length, 1);
        //pre-calculated based on murmur3
        var lastOctets = replicas.map(helper.lastOctetOf);
        assert.strictEqual(lastOctets[0], '3');
        done();
      });
    });
  });
  describe('#getReplicas() with ByteOrderPartitioner', function () {
    var ccmOptions = {
      vnodes: true,
      yaml: ['partitioner:org.apache.cassandra.dht.ByteOrderedPartitioner']
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
      var client = newInstance();
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
  describe('#getReplicas() with RandomPartitioner', function () {
    var ccmOptions = {
      vnodes: true,
      yaml: ['partitioner:org.apache.cassandra.dht.RandomPartitioner']
    };
    before(helper.ccmHelper.start('2', ccmOptions));
    after(helper.ccmHelper.remove);
    it('should get the replica', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        helper.assertInstanceOf(client.metadata.tokenizer, tokenizer.RandomTokenizer);
        assert.ifError(err);
        for (var i = 0; i < client.metadata.ring.length; i++) {
          var token = client.metadata.ring[i];
          var position = utils.binarySearch(client.metadata.ring, token, client.metadata.tokenizer.compare);
          assert.ok(position >= 0);
        }
        client.execute('select key from system.local', [], { routingKey: utils.allocBufferUnsafe(2)}, function (err) {
          assert.ifError(err);
          done();
        });
      });
    });
  });
});

function validateMurmurReplicas(client) {
  var replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 1]));
  assert.ok(replicas);
  //2 replicas per each dc
  assert.strictEqual(replicas.length, 4);
  assert.strictEqual(replicas.reduce(function (val, h) { return val + (h.datacenter === 'dc1' ? 1 : 0);}, 0), 2);

  //pre-calculated based on murmur3
  var lastOctets = replicas.map(helper.lastOctetOf);
  assert.strictEqual(lastOctets[0], '3');
  assert.strictEqual(lastOctets[1], '7');
  assert.strictEqual(lastOctets[2], '4');
  assert.strictEqual(lastOctets[3], '8');

  replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 3]));
  assert.ok(replicas);
  //2 replicas per each dc
  assert.strictEqual(replicas.length, 4);
  assert.strictEqual(replicas.reduce(function (val, h) { return val + (h.datacenter === 'dc1' ? 1 : 0);}, 0), 2);
  //pre-calculated based on murmur3
  lastOctets = replicas.map(helper.lastOctetOf);
  assert.strictEqual(lastOctets[0], '1');
  assert.strictEqual(lastOctets[1], '5');
  assert.strictEqual(lastOctets[2], '2');
  assert.strictEqual(lastOctets[3], '6');
}

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}