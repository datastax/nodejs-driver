/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const Encoder = require('../../../lib/encoder');
const Client = require('../../../lib/client');
const utils = require('../../../lib/utils');
const tokenizer = require('../../../lib/tokenizer');

describe('Client', function () {
  this.timeout(240000);
  describe('#getReplicas() with MurmurPartitioner', function () {
    before(function (done) {
      const client = newInstance();
      const createQuery = "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : %d, 'dc2' : %d}";
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
      const client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        validateMurmurReplicas(client);
        done();
      });
    });
    it('should get the local and remote replicas for a given keyspace if isMetadataSyncEnabled is false but keyspace metadata is present', function (done) {
      const client = newInstance({ isMetadataSyncEnabled: false });
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
      const client = newInstance({ isMetadataSyncEnabled: false });
      client.connect(function (err) {
        assert.ifError(err);
        const replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 1]));
        assert.strictEqual(null, replicas);
        done();
      });
    });
    it('should get the closest replica if no keyspace specified', function (done) {
      const client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        const replicas = client.getReplicas(null, utils.allocBufferFromArray([0, 0, 0, 1]));
        assert.ok(replicas);
        assert.strictEqual(replicas.length, 1);
        //pre-calculated based on murmur3
        const lastOctets = replicas.map(helper.lastOctetOf);
        assert.strictEqual(lastOctets[0], '3');
        done();
      });
    });
  });
  describe('#getReplicas() with ByteOrderPartitioner', function () {
    const ccmOptions = {
      vnodes: true,
      yaml: ['partitioner:org.apache.cassandra.dht.ByteOrderedPartitioner']
    };
    before(helper.ccmHelper.start('2', ccmOptions));
    after(helper.ccmHelper.remove);
    it('should get the replica', function (done) {
      function compareReplicas(val, expectedReplica) {
        const replicas = client.metadata.getReplicas(null, val);
        assert.ok(replicas);
        //2 replicas per each dc
        assert.strictEqual(replicas.length, 1);
        //pre-calculated based on Byte ordered partitioner
        assert.strictEqual(replicas[0].address, expectedReplica.address);
      }
      const client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        for (let i = 0; i < client.metadata.ring.length; i++) {
          const token = client.metadata.ring[i];
          const replica = client.metadata.primaryReplicas[client.metadata.tokenizer.stringify(token)];
          compareReplicas(token, replica);
        }
        done();
      });
    });
  });
  describe('#getReplicas() with RandomPartitioner', function () {
    const ccmOptions = {
      vnodes: true,
      yaml: ['partitioner:org.apache.cassandra.dht.RandomPartitioner']
    };
    before(helper.ccmHelper.start('2', ccmOptions));
    after(helper.ccmHelper.remove);
    it('should get the replica', function (done) {
      const client = newInstance();
      client.connect(function (err) {
        helper.assertInstanceOf(client.metadata.tokenizer, tokenizer.RandomTokenizer);
        assert.ifError(err);
        for (let i = 0; i < client.metadata.ring.length; i++) {
          const token = client.metadata.ring[i];
          const position = utils.binarySearch(client.metadata.ring, token, (t1, t2) => t1.compare(t2));
          assert.ok(position >= 0);
        }
        client.execute('select key from system.local', [], { routingKey: utils.allocBufferUnsafe(2)}, function (err) {
          assert.ifError(err);
          done();
        });
      });
    });
  });
  partitionerSuite('Murmur3Partitioner');
  partitionerSuite('RandomPartitioner');
  partitionerSuite('ByteOrderedPartitioner');
});

function partitionerSuite(partitionerName) {
  return (
    describe(partitionerName, () => {
      [false, true].forEach((vnodes) => {
        describe(vnodes ? 'with vnodes' : 'with single token', () => {
          const rangesPerNode = vnodes ? 256 : 1;
          const expectedTokenRanges = 3 * rangesPerNode;
          const ccmOptions = {
            partitioner: partitionerName,
            vnodes: vnodes
          };

          const setupInfo = helper.setup("3:0", {
            ccmOptions: ccmOptions,
            queries: [
              'CREATE KEYSPACE ks_simple_rf1 WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': 1}',
              'CREATE KEYSPACE ks_simple_rf2 WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': 2}',
              'CREATE KEYSPACE ks_nts_rf2 WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'dc1\' : 2}',
              'CREATE TABLE ks_simple_rf1.foo (i int primary key)',
              'INSERT INTO ks_simple_rf1.foo (i) VALUES (1)',
              'INSERT INTO ks_simple_rf1.foo (i) VALUES (2)',
              'INSERT INTO ks_simple_rf1.foo (i) VALUES (3)'
            ]
          });
          const client = setupInfo.client;
          describe('#getTokenRanges()', () => {
            it('should return ' + expectedTokenRanges + ' non-overlapping token ranges', (done) => {
              const ranges = Array.from(client.metadata.getTokenRanges());
              assert.strictEqual(ranges.length, expectedTokenRanges);

              // Find the replica for the given key.
              const encoder = new Encoder(4, {});
              const key = 1;
              const keyBuf = encoder.encodeInt(key);
              const keyToken = client.metadata.newToken(keyBuf);
              const replicas = client.metadata.getReplicas('ks_simple_rf1', keyBuf);
              const replicasByToken = client.metadata.getReplicas('ks_simple_rf1', keyToken);
              // whether retrieved by token or buffer, should return same replicas.
              assert.deepEqual(replicas, replicasByToken);
              assert.strictEqual(replicas.length, 1);
              const host = replicas[0];

              // Iterate the cluster's token ranges.  For each one, use a range query to ask Cassandra which partition keys
              // are in this range.
              let foundRange;
              utils.timesLimit(ranges.length, 50, (n, timesNext) => {
                utils.eachSeries(ranges[n].unwrap(), (range, sNext) => {
                  client.execute('SELECT i from ks_simple_rf1.foo where token(i) > ? and token(i) <= ?', [range.start.getValue(), range.end.getValue()], {prepare: true}, (err, result) => {
                    assert.ifError(err);
                    result.rows.forEach((row) => {
                      if (row.i === key) {
                        if (foundRange) {
                          assert.fail('Found the same key in two ranges: ' + foundRange + ' and ' + range);
                        }
                        foundRange = range;
                        // The range should be managed by the host found in getReplicas.
                        const replicas = client.metadata.getReplicas('ks_simple_rf1', range);
                        assert.strictEqual(replicas.length, 1);
                        assert.strictEqual(replicas[0], host);
                        assert.ok(foundRange.contains(keyToken), foundRange + ' should contain token ' + keyToken);
                      }
                    });
                    sNext();
                  });
                }, timesNext);
              }, (err) => {
                assert.ifError(err);
                assert.ok(foundRange, 'No range containing key');
                done();
              });
            });
            it('should only unwrap at most one range for all ranges', () => {
              const ranges = Array.from(client.metadata.getTokenRanges());
              let wrappedRanges = ranges.filter(range => range.isWrappedAround());
              assert.ok(wrappedRanges.length <= 1, 'Should have been at most one wrapped range, but found: ' + wrappedRanges);

              // split all ranges 10 times and ensure there is still only one wrapped range.
              let splitRanges = [];
              ranges.forEach((r) => {
                splitRanges = splitRanges.concat(r.splitEvenly(10));
              });

              wrappedRanges = splitRanges.filter(range => range.isWrappedAround());
              assert.ok(wrappedRanges.length <= 1, 'Should have been at most one wrapped range, but found: ' + wrappedRanges);
            });
          });
          describe('#getTokenRangesForHost()', () => {
            it('should return the expected number of ranges per host', () => {
              const validateRangesPerHost = (keyspace, rf) => {
                let allRanges = [];
                [1, 2, 3].forEach((hostNum) => {
                  const host = helper.findHost(client, hostNum);
                  const ranges = client.metadata.getTokenRangesForHost(keyspace, host);
                  // Special case: when using vnodes the tokens are not evenly assigned to each replica
                  // so we can't check that here.
                  if (!vnodes) {
                    assert.strictEqual(ranges.size, rf);
                  }
                  allRanges = allRanges.concat(Array.from(ranges));
                });

                // Special case check for vnodes to ensure that total number of replicated ranges is correct.
                assert.strictEqual(allRanges.length, 3 * rangesPerNode * rf);
                // Once we ignore duplicates, the number of ranges should match the number of nodes.
                assert.strictEqual(new Set(allRanges).size, 3 * rangesPerNode);
              };
              validateRangesPerHost('ks_simple_rf1', 1);
              validateRangesPerHost('ks_simple_rf2', 2);
              validateRangesPerHost('ks_nts_rf2', 2);
            });
          });
        });
      });
    })
  );
}

function validateMurmurReplicas(client) {
  let replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 1]));
  assert.ok(replicas);
  //2 replicas per each dc
  assert.strictEqual(replicas.length, 4);
  assert.strictEqual(replicas.reduce((val, h) => (val + (h.datacenter === 'dc1' ? 1 : 0)), 0), 2);

  //pre-calculated based on murmur3
  let lastOctets = replicas.map(helper.lastOctetOf);
  assert.strictEqual(lastOctets[0], '3');
  assert.strictEqual(lastOctets[1], '7');
  assert.strictEqual(lastOctets[2], '4');
  assert.strictEqual(lastOctets[3], '8');

  replicas = client.getReplicas('sampleks1', utils.allocBufferFromArray([0, 0, 0, 3]));
  assert.ok(replicas);
  //2 replicas per each dc
  assert.strictEqual(replicas.length, 4);
  assert.strictEqual(replicas.reduce((val, h) => (val + (h.datacenter === 'dc1' ? 1 : 0)), 0), 2);
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