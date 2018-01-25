"use strict";
const assert = require('assert');
const util = require('util');
const events = require('events');
const rewire = require('rewire');

const helper = require('../test-helper.js');
const clientOptions = require('../../lib/client-options.js');
const Host = require('../../lib/host.js').Host;
const HostMap = require('../../lib/host').HostMap;
const Metadata = require('../../lib/metadata');
const TableMetadata = require('../../lib/metadata/table-metadata');
const Murmur3Token = require('../../lib/token').Murmur3Token;
const TokenRange = require('../../lib/token').TokenRange;
const tokenizer = require('../../lib/tokenizer');
const types = require('../../lib/types');
const MutableLong = require('../../lib/types/mutable-long');
const dataTypes = types.dataTypes;
const utils = require('../../lib/utils');
const errors = require('../../lib/errors');
const Encoder = require('../../lib/encoder');

function expectRanges(metadata, keyspace, host, expectedRanges) {
  const eRanges = new Set(expectedRanges.map((tokens) => new TokenRange(token(tokens[0]), token(tokens[1]), metadata.tokenizer)));
  assert.deepEqual(metadata.getTokenRangesForHost(keyspace, host), eRanges);
}

describe('Metadata', function () {
  describe('#refreshKeyspaces()', function () {
    it('should parse C*2 keyspace metadata for simple strategy', function (done) {
      const cc = {
        query: function (q, w, cb) {
          cb(null, { rows: [{
            'keyspace_name': 'ks1',
            'strategy_class': 'org.apache.cassandra.locator.SimpleStrategy',
            'strategy_options': '{"replication_factor": 3}',
            'durable_writes': false
          }]});
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.tokenizer = getTokenizer();
      const hosts = new HostMap();
      const h1 = new Host('127.0.0.1', 2, clientOptions.defaultOptions());
      const h2 = new Host('127.0.0.2', 2, clientOptions.defaultOptions());
      const h3 = new Host('127.0.0.3', 2, clientOptions.defaultOptions());
      const h4 = new Host('127.0.0.4', 2, clientOptions.defaultOptions());
      const h5 = new Host('127.0.0.5', 2, clientOptions.defaultOptions());
      const h6 = new Host('127.0.0.6', 2, clientOptions.defaultOptions());
      h1.tokens = ['0'];
      h2.tokens = ['1'];
      h3.tokens = ['2'];
      h4.tokens = ['3'];
      h5.tokens = ['4'];
      h6.tokens = ['5'];
      [h1, h2, h3, h4, h5, h6].forEach((h) => hosts.push(h.address, h));
      metadata.buildTokens(hosts);
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function (err) {
        assert.ifError(err);
        assert.ok(metadata.keyspaces);
        const ks = metadata.keyspaces['ks1'];
        assert.ok(ks);
        assert.strictEqual(ks.strategy, 'org.apache.cassandra.locator.SimpleStrategy');
        assert.ok(ks.strategyOptions);
        assert.strictEqual(ks.strategyOptions['replication_factor'], 3);
        assert.strictEqual(ks.durableWrites, false);
        expectRanges(metadata, 'ks1', h1, [[3, 4],[4, 5],[5, 0]]);
        expectRanges(metadata, 'ks1', h2, [[4, 5],[5, 0],[0, 1]]);
        expectRanges(metadata, 'ks1', h3, [[5, 0],[0, 1],[1, 2]]);
        expectRanges(metadata, 'ks1', h4, [[0, 1],[1, 2],[2, 3]]);
        expectRanges(metadata, 'ks1', h5, [[1, 2],[2, 3],[3, 4]]);
        done();
      });
    });
    it('should parse C*2 keyspace metadata for network strategy', function (done) {
      const cc = {
        query: function (q, w, cb) {
          cb(null, { rows: [{
            'keyspace_name': 'ks2',
            'strategy_class': 'org.apache.cassandra.locator.NetworkTopologyStrategy',
            'strategy_options': '{"dc1": 3, "dc2": 1}'
          }]});
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.tokenizer = getTokenizer();
      const hosts = new HostMap();
      const h1 = new Host('127.0.0.1', 2, clientOptions.defaultOptions());
      const h2 = new Host('127.0.0.2', 2, clientOptions.defaultOptions());
      const h3 = new Host('127.0.0.3', 2, clientOptions.defaultOptions());
      const h4 = new Host('127.0.0.4', 2, clientOptions.defaultOptions());
      const h5 = new Host('127.0.0.5', 2, clientOptions.defaultOptions());
      const h6 = new Host('127.0.0.6', 2, clientOptions.defaultOptions());
      h1.tokens = ['0'];
      h1.datacenter = 'dc1';
      h1.rack = 'rack1';
      h2.tokens = ['1'];
      h2.datacenter = 'dc1';
      h2.rack = 'rack2';
      h3.tokens = ['2'];
      h3.datacenter = 'dc2';
      h3.rack = 'rack1';
      h4.tokens = ['3'];
      h4.datacenter = 'dc1';
      h4.rack = 'rack3';
      h5.tokens = ['4'];
      h5.datacenter = 'dc1';
      h5.rack = 'rack4';
      h6.tokens = ['5'];
      h6.datacenter = 'dc2';
      h6.rack = 'rack2';
      [h1, h2, h3, h4, h5, h6].forEach((h) => hosts.push(h.address, h));
      metadata.buildTokens(hosts);
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function (err) {
        assert.ifError(err);
        assert.ok(metadata.keyspaces);
        const ks = metadata.keyspaces['ks2'];
        assert.ok(ks);
        assert.strictEqual(ks.strategy, 'org.apache.cassandra.locator.NetworkTopologyStrategy');
        assert.ok(ks.strategyOptions);
        assert.strictEqual(ks.strategyOptions['dc1'], 3);
        assert.strictEqual(ks.strategyOptions['dc2'], 1);
        // For DC1, when next node is DC1, expect 5 ranges, when next node is DC2 expect 4 ranges.
        // For DC2, each node will have 3 ranges (since next DC2 node is always 3 nodes away)
        // all ranges except ]0, 1]
        expectRanges(metadata, 'ks2', h1, [[1, 2], [2, 3], [3, 4], [4, 5], [5, 0]]);
        // all ranges except ]1, 2], ]2, 3]
        expectRanges(metadata, 'ks2', h2, [[0, 1], [3, 4], [4, 5], [5, 0]]);
        // ]5, 2] (1/2 ring)
        expectRanges(metadata, 'ks2', h3, [[0, 1], [1, 2], [5, 0]]);
        // all nodes except ]3, 4]
        expectRanges(metadata, 'ks2', h4, [[0, 1], [1, 2], [2, 3], [4, 5], [5, 0]]);
        // ann nodes except ]4, 5], ]5, 0]
        expectRanges(metadata, 'ks2', h5, [[0, 1], [1, 2], [2, 3], [3, 4]]);
        // ]2, 5] (1/2 ring)
        expectRanges(metadata, 'ks2', h6, [[2, 3], [3, 4], [4, 5]]);
        done();
      });
    });
    it('should parse C*3 keyspace metadata for simple strategy', function (done) {
      const cc = {
        query: function (q, w, cb) {
          cb(null, { rows: [{
            'keyspace_name': 'ks1',
            'replication': {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3'},
            'durable_writes': true
          }]});
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.tokenizer = getTokenizer();
      metadata.setCassandraVersion([3, 0]);
      metadata.ring = [0, 1, 2, 3, 4, 5].map((t) => token(t));
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function (err) {
        assert.ifError(err);
        assert.ok(metadata.keyspaces);
        const ks = metadata.keyspaces['ks1'];
        assert.ok(ks);
        assert.strictEqual(ks.strategy, 'org.apache.cassandra.locator.SimpleStrategy');
        assert.ok(ks.strategyOptions);
        assert.strictEqual(ks.strategyOptions['replication_factor'], '3');
        assert.strictEqual(ks.durableWrites, true);
        done();
      });
    });
    it('should parse C*3 keyspace metadata for network strategy', function (done) {
      const cc = {
        query: function (q, w, cb) {
          cb(null, { rows: [{
            'keyspace_name': 'ks2',
            'replication': {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '2'},
            'durable_writes': true
          }]});
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.tokenizer = getTokenizer();
      metadata.setCassandraVersion([3, 0]);
      metadata.ring = [0, 1, 2, 3, 4, 5].map((t) => token(t));
      metadata.ringTokensAsStrings = ['0', '1', '2', '3', '4', '5'];
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function (err) {
        assert.ifError(err);
        assert.ok(metadata.keyspaces);
        const ks = metadata.keyspaces['ks2'];
        assert.ok(ks);
        assert.strictEqual(ks.strategy, 'org.apache.cassandra.locator.NetworkTopologyStrategy');
        assert.ok(ks.strategyOptions);
        assert.strictEqual(ks.strategyOptions['datacenter1'], '2');
        done();
      });
    });
  });
  describe('#getReplicas()', function () {
    it('should return depending on the rf and ring size with simple strategy', function () {
      const cc = {
        query: function (q, w, cb) {
          cb(null, { rows: [{
            'keyspace_name': 'dummy',
            'strategy_class': 'SimpleStrategy',
            'strategy_options': '{"replication_factor": 3}'
          }]});
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.tokenizer = getTokenizer();
      metadata.ring = [0, 1, 2, 3, 4, 5].map((t) => token(t));
      metadata.ringTokensAsStrings = ['0', '1', '2', '3', '4', '5'];
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.log = helper.noop;
      metadata.refreshKeyspaces();
      let replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([0]));

      const t = metadata.newToken(utils.allocBufferFromArray([0]));
      const replicasByToken = metadata.getReplicas('dummy', t);
      assert.deepEqual(replicasByToken, replicas);

      assert.ok(replicas);
      //Primary replica plus the 2 next tokens
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '0');
      assert.strictEqual(replicas[1], '1');
      assert.strictEqual(replicas[2], '2');

      replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([5]));
      assert.ok(replicas);
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '5');
      assert.strictEqual(replicas[1], '0');
      assert.strictEqual(replicas[2], '1');
    });
    it('should return depending on the dc rf with network topology', function (done) {
      const cc = getControlConnectionForRows([{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "1"}'
      }]);
      const options = clientOptions.extend({}, helper.baseOptions);
      const metadata = new Metadata(options, cc);
      metadata.tokenizer = getTokenizer();
      const racks = new utils.HashSet();
      racks.add('rack1');
      metadata.datacenters = {
        'dc1': { hostLength: 4, racks: racks },
        'dc2': { hostLength: 4, racks: racks }};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7].map((t) => token(t));
      metadata.ringTokensAsStrings = ['0', '1', '2', '3', '4', '5', '6', '7'];
      //load primary replicas
      metadata.primaryReplicas = {};
      for (let i = 0; i < metadata.ring.length; i ++) {
        const h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        h.rack = 'rack1';
        metadata.primaryReplicas[i.toString()] = h;
      }
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function () {
        const replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([0]));
        assert.ok(replicas);
        const t = metadata.newToken(utils.allocBufferFromArray([0]));
        const replicasByToken = metadata.getReplicas('dummy', t);
        assert.deepEqual(replicasByToken, replicas);
        //3 replicas from dc1 and 1 replica from dc2
        assert.strictEqual(replicas.length, 4);
        assert.strictEqual(replicas[0].address, '0');
        assert.strictEqual(replicas[1].address, '1');
        assert.strictEqual(replicas[2].address, '2');
        assert.strictEqual(replicas[3].address, '4');
        done();
      });
    });
    it('should return depending on the dc rf and rack with network topology', function (done) {
      const cc = getControlConnectionForRows([{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "3", "non_existent_dc": "1"}'
      }]);
      const options = clientOptions.extend({}, helper.baseOptions);
      const metadata = new Metadata(options, cc);
      metadata.tokenizer = getTokenizer();
      const racksDc1 = new utils.HashSet();
      racksDc1.add('dc1_r1');
      racksDc1.add('dc1_r2');
      const racksDc2 = new utils.HashSet();
      racksDc2.add('dc2_r1');
      racksDc2.add('dc2_r2');
      metadata.datacenters = {
        'dc1': { hostLength: 4, racks: racksDc1 },
        'dc2': { hostLength: 4, racks: racksDc2 }};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7].map((t) => token(t));
      metadata.ringTokensAsStrings = ['0', '1', '2', '3', '4', '5', '6', '7'];
      //load primary replicas
      metadata.primaryReplicas = {};
      for (let i = 0; i < metadata.ring.length; i ++) {
        // Hosts with in alternate dc and alternate rack
        const h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        h.rack = h.datacenter + '_r' + ((i % 4) <= 1 ? 1 : 2);
        metadata.primaryReplicas[i.toString()] = h;
      }
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function () {
        let replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([0]));
        assert.ok(replicas);
        const t = metadata.newToken(utils.allocBufferFromArray([0]));
        const replicasByToken = metadata.getReplicas('dummy', t);
        assert.deepEqual(replicasByToken, replicas);
        assert.deepEqual(replicas.map(getAddress), [ '0', '1', '2', '3', '4', '5' ]);
        replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([1]));
        assert.ok(replicas);
        assert.deepEqual(replicas.map(getAddress), [ '1', '2', '3', '4', '5', '6' ]);
        replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([3]));
        assert.ok(replicas);
        assert.deepEqual(replicas.map(getAddress), [ '3', '4', '5', '6', '7', '0' ]);
        done();
      });
    });
    it('should return depending on the dc rf and rack with network topology and skipping hosts', function (done) {
      const cc = getControlConnectionForRows([{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "2"}'
      }]);
      const options = clientOptions.extend({}, helper.baseOptions);
      const metadata = new Metadata(options, cc);
      metadata.tokenizer = getTokenizer();
      const racksDc1 = new utils.HashSet();
      racksDc1.add('dc1_r1');
      racksDc1.add('dc1_r2');
      const racksDc2 = new utils.HashSet();
      racksDc2.add('dc2_r1');
      racksDc2.add('dc2_r2');
      metadata.datacenters = {
        'dc1': { hostLength: 4, racks: racksDc1 },
        'dc2': { hostLength: 4, racks: racksDc2 }};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7].map((t) => token(t));
      metadata.ringTokensAsStrings = ['0', '1', '2', '3', '4', '5', '6', '7'];
      //load primary replicas
      metadata.primaryReplicas = {};
      for (let i = 0; i < metadata.ring.length; i ++) {
        // Hosts with in alternate dc and alternate rack
        const h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        h.rack = h.datacenter + '_r' + ((i % 4) <= 1 ? 1 : 2);
        metadata.primaryReplicas[i.toString()] = h;
      }
      //reorganize racks in dc1 to set contiguous tokens in the same rack
      metadata.primaryReplicas['0'].rack = 'dc1_rack1';
      metadata.primaryReplicas['2'].rack = 'dc1_rack1';
      metadata.primaryReplicas['4'].rack = 'dc1_rack2';
      metadata.primaryReplicas['6'].rack = 'dc1_rack2';
      metadata.log = helper.noop;
      metadata.refreshKeyspaces(function () {
        const replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([0]));
        assert.ok(replicas);
        const t = metadata.newToken(utils.allocBufferFromArray([0]));
        const replicasByToken = metadata.getReplicas('dummy', t);
        assert.deepEqual(replicasByToken, replicas);
        // For DC1, it should skip the replica with the same rack (node2) and add it at the end: 0, 4, 2
        assert.deepEqual(replicas.map(getAddress), [ '0', '1', '3', '4', '2' ]);
        done();
      });
    });
    it('should return quickly with many replicas and 0 nodes in one DC.', function (done) {
      this.timeout(5000);
      const cc = getControlConnectionForRows([{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "2"}'
      }]);
      const options = clientOptions.extend({}, helper.baseOptions);
      const metadata = new Metadata(options, cc);
      metadata.tokenizer = getTokenizer();
      const racksDc1 = new utils.HashSet();
      racksDc1.add('dc1_r1');
      const racksDc2 = new utils.HashSet();
      racksDc2.add('dc2_r1');
      metadata.datacenters = {
        'dc1': {hostLength: 100, racks: racksDc1},
        'dc2': {hostLength: 0, racks: racksDc2}
      };

      metadata.ring = [];
      // create ring with 100 replicas and 256 vnodes each.  place every replica in DC1.
      metadata.primaryReplicas = {};
      for (let r = 0; r < 100; r++) {
        const h = new Host(r.toString(), 2, options);
        h.datacenter = 'dc1';
        h.rack = 'dc1_r1';
        // 256 vnodes per replica.
        for (let v = 0; v < 256; v++) {
          const t = (v * 256) + r;
          metadata.ring.push(token(t));
          metadata.primaryReplicas[t.toString()] = h;
        }
      }
      metadata.ring.sort(function (a, b) {
        return a - b;
      });
      metadata.ringTokensAsStrings = [];
      for (let i = 0; i < metadata.ring.length; i++) {
        metadata.ringTokensAsStrings.push(metadata.ring[i].toString());
      }

      metadata.log = helper.noop;
      // Get the replicas of 5.  Since DC2 has 0 replicas, we only expect 3 replicas (the number of DC1).
      metadata.refreshKeyspaces(function () {
        const replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([5]));
        assert.ok(replicas);
        assert.deepEqual(replicas.map(getAddress), ['5', '6', '7']);
        done();
      });
    });
    it('should return quickly with many replicas and not enough nodes in a DC to satisfy RF.', function (done) {
      this.timeout(5000);
      const cc = getControlConnectionForRows([{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "2"}'
      }]);
      const options = clientOptions.extend({}, helper.baseOptions);
      const metadata = new Metadata(options, cc);
      metadata.tokenizer = getTokenizer();
      const racksDc1 = new utils.HashSet();
      racksDc1.add('dc1_r1');
      const racksDc2 = new utils.HashSet();
      racksDc2.add('dc2_r1');
      metadata.datacenters = {
        'dc1': {hostLength: 100, racks: racksDc1},
        'dc2': {hostLength: 1, racks: racksDc2}
      };

      metadata.ring = [];
      // create ring with 100 replicas and 256 vnodes each.  place every replica in DC1 except replica 0.
      metadata.primaryReplicas = {};
      for (let r = 0; r < 100; r++) {
        const h = new Host(r.toString(), 2, options);
        h.datacenter = 'dc1';
        h.rack = 'dc1_r1';
        // Place replica 0 in DC2.
        if (r === 0) {
          h.datacenter = 'dc2';
          h.rack = 'dc2_r1';
        }
        // 256 vnodes per replica.
        for (let v = 0; v < 256; v++) {
          const t = (v * 256) + r;
          metadata.ring.push(token(t));
          metadata.primaryReplicas[t.toString()] = h;
        }
      }
      // sort the ring so the tokens are in order (this is done in metadata buildTokens, but it accounts for
      // partitioner which we don't need to use here).
      metadata.ring.sort(function (a, b) {
        return a - b;
      });
      metadata.ringTokensAsStrings = [];
      for (let i = 0; i < metadata.ring.length; i++) {
        metadata.ringTokensAsStrings.push(metadata.ring[i].toString());
      }

      metadata.log = helper.noop;
      // Get the replicas of 0.  Since token 0 is a replica in DC2 and DC2 only has 1, it should return 1 replica
      // in addition to the next 3 replicas from DC1.
      metadata.refreshKeyspaces(function () {
        let replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([0]));
        assert.ok(replicas);
        assert.deepEqual(replicas.map(getAddress), ['0', '1', '2', '3']);
        // Get the replicas for token 51752 which should resolve to primary replica 40 (202nd vnode, 212 * 256 + 40),
        // its next two subsequent replicas for DC1, and then 0 for DC2 since it only has that one replica.
        replicas = metadata.getReplicas('dummy', utils.allocBufferFromArray([51752]));
        assert.ok(replicas);
        assert.deepEqual(replicas.map(getAddress), ['40', '41', '42', '0']);
        done();
      });
    });
  });
  describe('#clearPrepared()', function () {
    it('should clear the internal state', function () {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      const info1 = metadata.getPreparedInfo(null, 'QUERY1');
      // Should be created once
      assert.strictEqual(info1, metadata.getPreparedInfo(null, 'QUERY1'));
      const info2 = metadata.getPreparedInfo(null, 'QUERY2');
      metadata.clearPrepared();
      assert.notEqual(info1, metadata.getPreparedInfo(null, 'QUERY1'));
      assert.notEqual(info2, metadata.getPreparedInfo(null, 'QUERY2'));
    });
  });
  describe('#getPreparedInfo()', function () {
    it('should create a new EventEmitter when the query has not been prepared', function () {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      let info = metadata.getPreparedInfo(null, 'query1');
      helper.assertInstanceOf(info, events.EventEmitter);
      info = metadata.getPreparedInfo(null, 'query2');
      helper.assertInstanceOf(info, events.EventEmitter);
    });
    it('should get the same EventEmitter when the query is the same', function () {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      const info1 = metadata.getPreparedInfo(null, 'query1');
      helper.assertInstanceOf(info1, events.EventEmitter);
      const info2 = metadata.getPreparedInfo(null, 'query1');
      helper.assertInstanceOf(info2, events.EventEmitter);
      assert.strictEqual(info1, info2);
    });
    it('should create a new EventEmitter when the query is the same but the keyspace is different', function () {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      const info0 = metadata.getPreparedInfo(null, 'query1');
      const info1 = metadata.getPreparedInfo('ks1', 'query1');
      const info2 = metadata.getPreparedInfo('ks2', 'query1');
      assert.notStrictEqual(info0, info1);
      assert.notStrictEqual(info0, info2);
      assert.notStrictEqual(info1, info2);
    });
  });
  describe('#getUdt()', function () {
    it('should retrieve the udt information', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({
              rows: [ {
                field_names: ['field1', 'field2', 'field3'],
                field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type',
                  'org.apache.cassandra.db.marshal.DynamicCompositeType('
                  + 's=>org.apache.cassandra.db.marshal.UTF8Type,'
                  + 'i=>org.apache.cassandra.db.marshal.Int32Type)']}],
              flags: utils.emptyObject
            }));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.ok(udtInfo);
        assert.strictEqual(udtInfo.name, 'udt1');
        assert.ok(udtInfo.fields);
        assert.strictEqual(udtInfo.fields.length, 3);
        done();
      });
    });
    it('should callback in err when there is an error', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(new Error('Test error'));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
    });
    it('should be null when it is not found', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [], flags: utils.emptyObject}));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should be null when keyspace does not exists', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({
              rows: [ {
                field_names: ['field1', 'field2'],
                field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']}],
              flags: utils.emptyObject
            }));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = {};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      let queried = 0;
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({
              rows: [ {
                field_names: ['field1', 'field2'],
                field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']}],
              flags: utils.emptyObject
            }));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      utils.times(50, function (n, next) {
        metadata.getUdt('ks1', 'udt5', function (err, udtInfo) {
          if (err) {
            return next(err);
          }
          assert.ok(udtInfo);
          assert.ok(util.isArray(udtInfo.fields));
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 1);
        done();
      });
    });
    it('should query once and cache when called serially', function (done) {
      let queried = 0;
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({
              rows: [ {
                field_names: ['field1', 'field2'],
                field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.BooleanType']}],
              flags: utils.emptyObject
            }));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      utils.timesSeries(50, function (n, next) {
        metadata.getUdt('ks1', 'udt10', function (err, udtInfo) {
          if (err) {return next(err);}
          assert.ok(udtInfo);
          assert.ok(util.isArray(udtInfo.fields));
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 1);
        done();
      });
    });
    it('should query the following times if it was null', function (done) {
      let queried = 0;
      const cc = {
        query: function (q, cb) {
          queried++;
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [], flags: utils.emptyObject}));
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      utils.timesSeries(20, function (n, next) {
        metadata.getUdt('ks1', 'udt20', function (err, udtInfo) {
          if (err) {return next(err);}
          assert.strictEqual(udtInfo, null);
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(queried, 20);
        done();
      });
    });
  });
  describe('#getTrace()', function () {
    it('should return the trace if its already stored', function (done) {
      const sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: 2002
      };
      const eventRows = [ {
        event_id: types.TimeUuid.now(),
        activity: 'act 1',
        source: types.InetAddress.fromString('10.10.10.2'),
        source_elapsed: 101
      }];
      const cc = {
        query: function (r, cb) {
          setImmediate(function () {
            if (r.query.indexOf('system_traces.sessions') >= 0) {
              return cb(null, { rows: [ sessionRow], flags: utils.emptyObject});
            }
            cb(null, { rows: eventRows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), types.consistencies.all, function (err, trace) {
        assert.ifError(err);
        assert.ok(trace);
        assert.strictEqual(trace.requestType, sessionRow.request);
        assert.strictEqual(trace.parameters, sessionRow.parameters);
        assert.strictEqual(trace.coordinator, sessionRow.coordinator);
        assert.strictEqual(trace.startedAt, sessionRow.started_at);
        assert.strictEqual(trace.events.length, 1);
        assert.strictEqual(trace.events[0].id, eventRows[0].event_id);
        assert.strictEqual(trace.events[0].activity, eventRows[0].activity);
        done();
      });
    });
    it('should parse the new client address column', function (done) {
      const sessionRow = {
        session_id: types.Uuid.fromString('69e37ff0-0475-11e5-9798-f3efee551757'),
        client: types.InetAddress.fromString('127.0.0.2'),
        command: 'QUERY',
        coordinator: types.InetAddress.fromString('127.0.0.2'),
        duration: 11922,
        parameters: {
          page_size: '5000',
          query: 'SELECT * FROM system.schema_keyspaces'
        },
        request: 'Execute CQL3 query',
        started_at: new Date()
      };
      const eventRows = [ {
        event_id: types.TimeUuid.now(),
        activity: 'act 1',
        source: types.InetAddress.fromString('10.10.10.2'),
        source_elapsed: 101
      }];
      const cc = {
        query: function (r, cb) {
          setImmediate(function () {
            if (r.query.indexOf('system_traces.sessions') >= 0) {
              return cb(null, { rows: [ sessionRow], flags: utils.emptyObject});
            }
            cb(null, { rows: eventRows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (err, trace) {
        assert.ifError(err);
        assert.ok(trace);
        assert.strictEqual(trace.requestType, sessionRow.request);
        assert.strictEqual(trace.parameters, sessionRow.parameters);
        assert.strictEqual(trace.coordinator, sessionRow.coordinator);
        assert.strictEqual(trace.startedAt, sessionRow.started_at);
        assert.strictEqual(trace.clientAddress, sessionRow.client);
        assert.strictEqual(trace.events.length, 1);
        assert.strictEqual(trace.events[0].id, eventRows[0].event_id);
        assert.strictEqual(trace.events[0].activity, eventRows[0].activity);
        done();
      });
    });
    it('should retry if its not already stored', function (done) {
      const sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: null
      };
      const eventRows = [ {
        event_id: types.TimeUuid.now(),
        activity: 'act 2',
        source: types.InetAddress.fromString('10.10.10.1'),
        source_elapsed: 102
      }];
      let calls = 0;
      const cc = {
        query: function (r, cb) {
          setImmediate(function () {
            if (r.query.indexOf('system_traces.sessions') >= 0) {
              if (++calls > 1) {
                sessionRow.duration = 2002;
              }
              return cb(null, { rows: [ sessionRow]});
            }
            cb(null, { rows: eventRows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (err, trace) {
        assert.ifError(err);
        assert.ok(trace);
        assert.strictEqual(calls, 2);
        assert.strictEqual(trace.requestType, sessionRow.request);
        assert.strictEqual(trace.parameters, sessionRow.parameters);
        assert.strictEqual(trace.coordinator, sessionRow.coordinator);
        assert.strictEqual(trace.startedAt, sessionRow.started_at);
        assert.strictEqual(trace.events.length, 1);
        assert.strictEqual(trace.events[0].id, eventRows[0].event_id);
        assert.strictEqual(trace.events[0].activity, eventRows[0].activity);
        done();
      });
    });
    it('should stop retrying after a few attempts', function (done) {
      const sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: null //duration null means that trace is not fully flushed
      };
      let calls = 0;
      const cc = {
        query: function (r, cb) {
          setImmediate(function () {
            //try with empty result and null duration
            let rows = [];
            if (++calls > 1) {
              rows = [ sessionRow ];
            }
            cb(null, { rows: rows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (err, trace) {
        assert.ok(err);
        assert.ok(!trace);
        assert.ok(err.message);
        assert.ok(err.message.indexOf('attempt') > 0);
        done();
      });
    });
    it('should callback in error if there was an error retrieving the trace', function (done) {
      const err = new Error('dummy err');
      const cc = {
        query: function (r, cb) {
          setImmediate(function () {
            cb(err);
          });
        }
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (receivedErr, trace) {
        assert.ok(err);
        assert.strictEqual(receivedErr, err);
        assert.ok(!trace);
        done();
      });
    });
  });
  describe('#getTable()', function () {
    it('should be null when it is not found', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system.schema_columnfamilies') >= 0) {
              return cb(null, {rows: []});
            }
            cb(null, {rows: []});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      metadata.getTable('ks_tbl_meta', 'tbl_does_not_exists', function (err, table) {
        assert.ifError(err);
        assert.strictEqual(table, null);
        done();
      });
    });
    it('should be null when keyspace does not exists', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), {});
      metadata.keyspaces = { };
      metadata.getTable('ks_does_not_exists', 'tbl1', function (err, table) {
        assert.ifError(err);
        assert.strictEqual(table, null);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
        column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
        comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
        key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
      const columnRows = [
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' }
      ];
      let calledTable = 0;
      let calledRows = 0;
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system.schema_columnfamilies') >= 0) {
              calledTable++;
              return cb(null, {rows: [tableRow]});
            }
            calledRows++;
            cb(null, {rows: columnRows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      utils.map(new Array(100), function (n, next) {
        metadata.getTable('ks_tbl_meta', 'tbl1', next);
      }, function (err, results) {
        assert.ifError(err);
        assert.strictEqual(calledTable, 1);
        assert.strictEqual(calledRows, 1);
        assert.strictEqual(results.length, 100);
        assert.strictEqual(results[0].name, 'tbl1');
        assert.strictEqual(results[0], results[1]);
        assert.strictEqual(results[0], results[99]);
        done();
      });
    });
    it('should query once if query the same table serially', function (done) {
      const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
        column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
        comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
        key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
      const columnRows = [
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' }
      ];
      let calledTable = 0;
      let calledRows = 0;
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system.schema_columnfamilies') >= 0) {
              calledTable++;
              return cb(null, {rows: [tableRow]});
            }
            calledRows++;
            cb(null, {rows: columnRows});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      utils.mapSeries(new Array(100), function (n, next) {
        metadata.getTable('ks_tbl_meta', 'tbl1', next);
      }, function (err, results) {
        assert.ifError(err);
        assert.strictEqual(calledTable, 1);
        assert.strictEqual(calledRows, 1);
        assert.strictEqual(results.length, 100);
        assert.strictEqual(results[0].name, 'tbl1');
        assert.strictEqual(results[0], results[1]);
        assert.strictEqual(results[0], results[99]);
        done();
      });
    });
    it('should query the following times if it was not found', function (done) {
      let calledTable = 0;
      let calledRows = 0;
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system.schema_columnfamilies') >= 0) {
              calledTable++;
              return cb(null, {rows: []});
            }
            calledRows++;
            cb(null, {rows: []});
          });
        },
        getEncoder: () => new Encoder(1, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      utils.mapSeries(new Array(100), function (n, next) {
        metadata.getTable('ks_tbl_meta', 'tbl1', next);
      }, function (err, results) {
        assert.ifError(err);
        assert.strictEqual(calledTable, 100);
        assert.strictEqual(calledRows, 0);
        assert.strictEqual(results.length, 100);
        assert.strictEqual(results[0], null);
        assert.strictEqual(results[1], null);
        assert.strictEqual(results[99], null);
        done();
      });
    });
    it('should query each time if metadata retrieval flag is false', function (done) {
      const tableRow = {"keyspace_name":"ks_tbl_meta","table_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},"compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},"dclocal_read_repair_chance":0.1,"default_time_to_live":0,"extensions":{},"flags":["compound"],"gc_grace_seconds":864000,"id":"7e0e8bf0-5862-11e5-84f8-c7d0c38d1d8d","max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
      const columnRows = [
        {"keyspace_name": "ks_tbl_meta", "table_name": "tbl1", "column_name": "id", "clustering_order": "none", "column_name_bytes": "0x6964", "kind": "partition_key", "position": -1, "type": "uuid"},
        {"keyspace_name": "ks_tbl_meta", "table_name": "tbl1", "column_name": "text_sample", "clustering_order": "none", "column_name_bytes": "0x746578745f73616d706c65", "kind": "regular", "position": -1, "type": "text"}
      ];
      const options = utils.extend({}, clientOptions.defaultOptions());
      options.isMetadataSyncEnabled = false;
      const cc = getControlConnectionForTable(tableRow, columnRows);
      const metadata = new Metadata(options, cc);
      metadata.keyspaces = { };
      metadata.setCassandraVersion([3, 0]);
      utils.mapSeries(new Array(100), function (n, next) {
        metadata.getTable('ks_tbl_meta', 'tbl1', next);
      }, function (err, results) {
        assert.ifError(err);
        assert.strictEqual(cc.queriedTable, 100);
        assert.strictEqual(cc.queriedRows, 100);
        assert.strictEqual(results.length, 100);
        helper.assertInstanceOf(results[0], TableMetadata);
        helper.assertInstanceOf(results[1], TableMetadata);
        helper.assertInstanceOf(results[99], TableMetadata);
        done();
      });
    });
    describe('with C*2.0 metadata rows', function () {
      it('should parse partition and clustering keys', function (done) {
        const customType ="org.apache.cassandra.db.marshal.DynamicCompositeType(s=>org.apache.cassandra.db.marshal.UTF8Type, i=>org.apache.cassandra.db.marshal.Int32Type)";
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
          column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
          key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 3000, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
        const columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val1', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.Int32Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.BytesType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'valcus3', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'regular', validator: customType }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.02);
          assert.strictEqual(table.isCompact, false);
          assert.ok(table.caching);
          assert.strictEqual(table.caching, 'KEYS_ONLY');
          assert.strictEqual(table.populateCacheOnFlush, false);
          assert.strictEqual(table.speculativeRetry, '99.0PERCENTILE');
          assert.strictEqual(table.indexInterval, 128);
          assert.strictEqual(table.memtableFlushPeriod, 3000);
          assert.strictEqual(table.minIndexInterval, null);
          assert.strictEqual(table.maxIndexInterval, null);
          assert.strictEqual(table.crcCheckChance, null);
          assert.strictEqual(table.extensions, null);
          assert.strictEqual(table.defaultTtl, 0);
          assert.strictEqual(table.columns.length, 6);
          assert.strictEqual(table.columns[0].name, 'apk2');
          assert.strictEqual(table.columns[0].type.code, types.dataTypes.varchar);
          assert.strictEqual(table.columns[1].name, 'ck');
          assert.strictEqual(table.columns[1].type.code, types.dataTypes.timeuuid);
          assert.strictEqual(table.columns[2].name, 'pk1');
          assert.strictEqual(table.columns[2].type.code, types.dataTypes.uuid);
          assert.strictEqual(table.columns[3].name, 'val1');
          assert.strictEqual(table.columns[3].type.code, types.dataTypes.int);
          assert.strictEqual(table.columns[4].name, 'val2');
          assert.strictEqual(table.columns[4].type.code, types.dataTypes.blob);
          assert.strictEqual(table.columns[5].name, 'valcus3');
          assert.strictEqual(table.columns[5].type.code, types.dataTypes.custom);
          assert.strictEqual(table.columns[5].type.info, customType);
          assert.strictEqual(table.partitionKeys.length, 2);
          assert.strictEqual(table.partitionKeys[0].name, 'pk1');
          assert.strictEqual(table.partitionKeys[1].name, 'apk2');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'ck');
          assert.strictEqual(table.clusteringOrder.length, 1);
          assert.strictEqual(table.clusteringOrder[0], 'ASC');
          done();
        });
      });
      it('should parse table with compact storage with clustering key', function (done) {
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: '{"keys":"ALL", "rows_per_partition":"NONE"}', cf_id: types.Uuid.fromString('96c86920-ed84-11e4-8991-199e66562428'),
          column_aliases: '["id2"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.TimeUUIDType', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.UTF8Type', dropped_columns: null, gc_grace_seconds: 864000, index_interval: null, is_dense: true, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0.1, max_compaction_threshold: 32, max_index_interval: 2048, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, min_index_interval: 128, read_repair_chance: 0, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: 'text1' };
        const columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'id1', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'id2', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text1', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'compact_value', validator: 'org.apache.cassandra.db.marshal.UTF8Type' }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.columns[0].name, 'id1');
          assert.strictEqual(table.columns[1].name, 'id2');
          assert.strictEqual(table.columns[2].name, 'text1');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id1');
          done();
        });
      });
      it('should parse table with compact storage', function (done) {
        const tableRow = {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":"{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}","cf_id":"609f53a0-038b-11e5-be48-0d419bfb85c8","column_aliases":"[]","comment":"","compaction_strategy_class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy","compaction_strategy_options":"{}","comparator":"org.apache.cassandra.db.marshal.UTF8Type","compression_parameters":"{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}","default_time_to_live":0,"default_validator":"org.apache.cassandra.db.marshal.BytesType","dropped_columns":null,"gc_grace_seconds":864000,"index_interval":null,"is_dense":false,"key_aliases":"[\"id\"]","key_validator":"org.apache.cassandra.db.marshal.UUIDType","local_read_repair_chance":0.1,"max_compaction_threshold":32,"max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_compaction_threshold":4,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99.0PERCENTILE","subcomparator":null,"type":"Standard","value_alias":null};
        const columnRows = [
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"id","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"partition_key","validator":"org.apache.cassandra.db.marshal.UUIDType"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text1","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text2","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.columns[0].name, 'id');
          assert.strictEqual(table.columns[1].name, 'text1');
          assert.strictEqual(table.columns[2].name, 'text2');
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
      it('should parse custom index (legacy)', function (done) {
        const tableRow = {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":"{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}","cf_id":"609f53a0-038b-11e5-be48-0d419bfb85c8","column_aliases":"[]","comment":"","compaction_strategy_class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy","compaction_strategy_options":"{}","comparator":"org.apache.cassandra.db.marshal.UTF8Type","compression_parameters":"{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}","default_time_to_live":0,"default_validator":"org.apache.cassandra.db.marshal.BytesType","dropped_columns":null,"gc_grace_seconds":864000,"index_interval":null,"is_dense":false,"key_aliases":"[\"id\"]","key_validator":"org.apache.cassandra.db.marshal.UUIDType","local_read_repair_chance":0.1,"max_compaction_threshold":32,"max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_compaction_threshold":4,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99.0PERCENTILE","subcomparator":null,"type":"Standard","value_alias":null};
        const columnRows = [
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"id","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"partition_key","validator":"org.apache.cassandra.db.marshal.UUIDType"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text1","component_index":null,"index_name":"custom_index","index_options":'{"foo":"bar", "class_name":"dummy.DummyIndex"}',"index_type":"CUSTOM","type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length === 1);
          const index = table.indexes[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'custom_index');
          assert.strictEqual(index.target, 'text1');
          assert.strictEqual(index.isCompositesKind(), false);
          assert.strictEqual(index.isCustomKind(), true);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          assert.strictEqual(index.options['foo'], 'bar');
          assert.strictEqual(index.options['class_name'], 'dummy.DummyIndex');
          done();
        });
      });
      it('should parse keys index (legacy)', function (done) {
        const tableRow = {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":"{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}","cf_id":"609f53a0-038b-11e5-be48-0d419bfb85c8","column_aliases":"[]","comment":"","compaction_strategy_class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy","compaction_strategy_options":"{}","comparator":"org.apache.cassandra.db.marshal.UTF8Type","compression_parameters":"{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}","default_time_to_live":0,"default_validator":"org.apache.cassandra.db.marshal.BytesType","dropped_columns":null,"gc_grace_seconds":864000,"index_interval":null,"is_dense":false,"key_aliases":"[\"id\"]","key_validator":"org.apache.cassandra.db.marshal.UUIDType","local_read_repair_chance":0.1,"max_compaction_threshold":32,"max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_compaction_threshold":4,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99.0PERCENTILE","subcomparator":null,"type":"Standard","value_alias":null};
        const columnRows = [
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"id","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"partition_key","validator":"org.apache.cassandra.db.marshal.UUIDType"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text1","component_index":null,"index_name":"custom_index","index_options":'{"index_keys": ""}',"index_type":"KEYS","type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'b@706172656e745f70617468', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length === 1);
          const index = table.indexes[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'custom_index');
          // target should be column name since we weren't able to parse index_keys from index_options.
          assert.strictEqual(index.target, 'keys(text1)');
          assert.strictEqual(index.isCompositesKind(), false);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), true);
          assert.ok(index.options);
          done();
        });
      });
      it('should parse keys index with null string index options', function (done) {
        // Validates a special case where a 'KEYS' index was created using thrift.  In this particular case the index
        // lacks index_options, however the index_options value is a 'null' string rather than a null value.
        // DSE Analytics at some point did this with its cfs tables.

        const tableRow = {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":"{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}","cf_id":"609f53a0-038b-11e5-be48-0d419bfb85c8","column_aliases":"[]","comment":"","compaction_strategy_class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy","compaction_strategy_options":"{}","comparator":"org.apache.cassandra.db.marshal.UTF8Type","compression_parameters":"{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}","default_time_to_live":0,"default_validator":"org.apache.cassandra.db.marshal.BytesType","dropped_columns":null,"gc_grace_seconds":864000,"index_interval":null,"is_dense":false,"key_aliases":"[\"id\"]","key_validator":"org.apache.cassandra.db.marshal.UUIDType","local_read_repair_chance":0.1,"max_compaction_threshold":32,"max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_compaction_threshold":4,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99.0PERCENTILE","subcomparator":null,"type":"Standard","value_alias":null};
        const columnRows = [
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"id","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"partition_key","validator":"org.apache.cassandra.db.marshal.UUIDType"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"b@706172656e745f70617468","component_index":null,"index_name":"cfs_archive_parent_path","index_options":"\"null\"","index_type":"KEYS","type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'b@706172656e745f70617468', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length === 1);
          const index = table.indexes[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'cfs_archive_parent_path');
          // target should be column name since we weren't able to parse index_keys from index_options.
          assert.strictEqual(index.target, 'b@706172656e745f70617468');
          assert.strictEqual(index.isCompositesKind(), false);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), true);
          assert.ok(index.options);
          done();
        });
      });
    });
    describe('with C*1.2 metadata rows', function () {
      it('should parse partition and clustering keys', function (done) {
        const customType ="org.apache.cassandra.db.marshal.DynamicCompositeType(s=>org.apache.cassandra.db.marshal.UTF8Type, i=>org.apache.cassandra.db.marshal.Int32Type)";
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["zck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null,
          key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: true, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        const columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val2', component_index: 1, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.BytesType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'valz1', component_index: 1, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.Int32Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'valcus3', component_index: 1, index_name: null, index_options: null, index_type: null, validator: customType }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.columns.length, 6);
          assert.strictEqual(table.partitionKeys.length, 2);
          assert.strictEqual(table.partitionKeys[0].name, 'pk1');
          assert.strictEqual(table.partitionKeys[1].name, 'apk2');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'zck');
          assert.strictEqual(table.columnsByName['valcus3'].type.code, dataTypes.custom);
          assert.strictEqual(table.columnsByName['valcus3'].type.info, customType);
          assert.strictEqual(table.populateCacheOnFlush, true);
          //default as it does not exist for C* 1.2
          assert.strictEqual(table.speculativeRetry, 'NONE');
          assert.strictEqual(table.indexInterval, null);
          assert.strictEqual(table.memtableFlushPeriod, 0);
          assert.strictEqual(table.minIndexInterval, null);
          assert.strictEqual(table.maxIndexInterval, null);
          assert.strictEqual(table.defaultTtl, 0);
          assert.strictEqual(table.crcCheckChance, null);
          assert.strictEqual(table.extensions, null);
          done();
        });
      });
      it('should parse with no clustering keys', function (done) {
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '[]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}', comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null,
          key_aliases: '["id"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        const columnRows = [ { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text_sample', component_index: 0, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' } ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 2);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.partitionKeys[0].type.code, types.dataTypes.uuid);
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
      it('should parse with compact storage', function (done) {
        //1 pk, 1 ck and 1 val
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl5', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["id2"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.TimeUUIDType', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}',
          default_validator: 'org.apache.cassandra.db.marshal.UTF8Type', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard',
          value_alias: 'text1' };
        const columnRows = [];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id1');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          done();
        });
      });
      it('should parse with compact storage with pk', function (done) {
        //1 pk, 2 val
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '[]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.UTF8Type', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        const columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text1', component_index: null, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text2', component_index: null, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
      it('should parse with compact storage and all columns as clustering keys', function (done) {
        //1 pk, 2 ck
        const tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["id2","text1"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: '' };
        const columnRows = [];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.clusteringKeys.length, 2);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          assert.strictEqual(table.clusteringKeys[1].name, 'text1');
          done();
        });
      });
    });
    describe('with C*2.2 metadata rows', function () {
      it('should parse new 2.2 types', function (done) {
        const customType ="org.apache.cassandra.db.marshal.DynamicCompositeType(s=>org.apache.cassandra.db.marshal.UTF8Type, i=>org.apache.cassandra.db.marshal.Int32Type)";
        const tableRow = {
          keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', bloom_filter_fp_chance: 0.03, caching: '{"keys":"ALL", "rows_per_partition":"NONE"}',
          cf_id: types.Uuid.fromString('c05f4c40-fe05-11e4-8481-277ff03b5030'), comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}', comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, is_dense: false, key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0.1, max_compaction_threshold: 32, max_index_interval: 2048, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, min_index_interval: 128, read_repair_chance: 0, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard' };
        const columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'date_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.SimpleDateType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'id', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'smallint_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.ShortType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'time_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.TimeType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'tinyint_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.ByteType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'custom_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: customType }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl_c22', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.03);
          assert.strictEqual(table.isCompact, false);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 6);
          assert.strictEqual(table.columns[0].name, 'date_sample');
          assert.strictEqual(table.columns[0].type.code, types.dataTypes.date);
          assert.strictEqual(table.columns[1].name, 'id');
          assert.strictEqual(table.columns[1].type.code, types.dataTypes.uuid);
          assert.strictEqual(table.columns[2].name, 'smallint_sample');
          assert.strictEqual(table.columns[2].type.code, types.dataTypes.smallint);
          assert.strictEqual(table.columns[3].name, 'time_sample');
          assert.strictEqual(table.columns[3].type.code, types.dataTypes.time);
          assert.strictEqual(table.columns[4].name, 'tinyint_sample');
          assert.strictEqual(table.columns[4].type.code, types.dataTypes.tinyint);
          assert.strictEqual(table.columns[5].name, 'custom_sample');
          assert.strictEqual(table.columns[5].type.code, types.dataTypes.custom);
          assert.strictEqual(table.columns[5].type.info, customType);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
    });
    describe('with C*3.0+ metadata rows', function () {
      it('should parse partition and clustering keys', function (done) {
        const tableRow = {
          "keyspace_name":"ks_tbl_meta",
          "table_name":"tbl4",
          "bloom_filter_fp_chance":0.01,
          "caching":{"keys":"ALL","rows_per_partition":"NONE"},
          "comment":"",
          "compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},
          "compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},
          "dclocal_read_repair_chance":0.1,
          "default_time_to_live":0,
          "extensions":{'hello': utils.allocBufferFromString('world')},
          "flags":["compound"],
          "gc_grace_seconds":864000,
          "id":"8008ae40-5862-11e5-b0ce-c7d0c38d1d8d",
          "max_index_interval":1024,
          "crc_check_chance": 0.8,
          "memtable_flush_period_in_ms":0,"min_index_interval":64,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
        const customType ="org.apache.cassandra.db.marshal.DynamicCompositeType(s=>org.apache.cassandra.db.marshal.UTF8Type, i=>org.apache.cassandra.db.marshal.Int32Type)";
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "apk2", "clustering_order": "none", "column_name_bytes": "0x61706b32", "kind": "partition_key", "position": 1, "type": "text"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "pk1", "clustering_order": "none", "column_name_bytes": "0x706b31", "kind": "partition_key", "position": 0, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "val2", "clustering_order": "none", "column_name_bytes": "0x76616c32", "kind": "regular", "position": -1, "type": "blob"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "valz1", "clustering_order": "none", "column_name_bytes": "0x76616c7a31", "kind": "regular", "position": -1, "type": "int"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "valcus3", "clustering_order": "none", "column_name_bytes": "0x76611663757333", "kind": "regular", "position": -1, "type": "'" + customType + "'"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl4", "column_name": "zck", "clustering_order": "asc", "column_name_bytes": "0x7a636b", "kind": "clustering", "position": 0, "type": "timeuuid"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl4', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.speculativeRetry, '99PERCENTILE');
          assert.strictEqual(table.indexInterval, null);
          assert.strictEqual(table.memtableFlushPeriod, 0);
          assert.strictEqual(table.minIndexInterval, 64);
          assert.strictEqual(table.maxIndexInterval, 1024);
          assert.strictEqual(table.crcCheckChance, 0.8);
          assert.ok(table.extensions);
          assert.strictEqual(table.extensions['hello'].toString('utf8'), 'world');
          //not present, default
          assert.strictEqual(table.populateCacheOnFlush, false);
          assert.strictEqual(table.columns.length, 6);
          assert.deepEqual(table.columns.map(x => x.type.code),
            [dataTypes.text, dataTypes.uuid, dataTypes.blob, dataTypes.int, dataTypes.custom, dataTypes.timeuuid]);
          assert.strictEqual(table.partitionKeys.length, 2);
          assert.strictEqual(table.partitionKeys[0].name, 'pk1');
          assert.strictEqual(table.partitionKeys[1].name, 'apk2');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'zck');
          assert.strictEqual(table.columnsByName['valcus3'].type.info, customType);
          done();
        });
      });
      it('should parse with no clustering keys', function (done) {
        const tableRow = {"keyspace_name":"ks_tbl_meta","table_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},"compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},"dclocal_read_repair_chance":0.1,"default_time_to_live":0,"extensions":{},"flags":["compound"],"gc_grace_seconds":864000,"id":"7e0e8bf0-5862-11e5-84f8-c7d0c38d1d8d","max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl1", "column_name": "id", "clustering_order": "none", "column_name_bytes": "0x6964", "kind": "partition_key", "position": -1, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl1", "column_name": "text_sample", "clustering_order": "none", "column_name_bytes": "0x746578745f73616d706c65", "kind": "regular", "position": -1, "type": "text"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 2);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.partitionKeys[0].type.code, types.dataTypes.uuid);
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
      it('should parse with compact storage', function (done) {
        //1 pk, 1 ck and 1 val
        const tableRow = {
          "keyspace_name":"ks_tbl_meta","table_name":"tbl5","bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},"compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},"dclocal_read_repair_chance":0.1,"default_time_to_live":0,"extensions":{},
          "flags":["dense"],"gc_grace_seconds":864000,"id":"80fd9590-5862-11e5-84f8-c7d0c38d1d8d","max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "id1", "clustering_order": "none", "column_name_bytes": "0x696431", "kind": "partition_key", "position": -1, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "id2", "clustering_order": "asc", "column_name_bytes": "0x696432", "kind": "clustering", "position": 0, "type": "timeuuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "text1", "clustering_order": "none", "column_name_bytes": "0x7465787431", "kind": "regular", "position": -1, "type": "text"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl5', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id1');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          done();
        });
      });
      it('should parse with custom index', function (done) {
        //1 pk, 1 ck and 1 val
        const tableRow = {
          "keyspace_name":"ks_tbl_meta","table_name":"tbl5","bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},"compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},"dclocal_read_repair_chance":0.1,"default_time_to_live":0,"extensions":{},
          "flags":["dense"],"gc_grace_seconds":864000,"id":"80fd9590-5862-11e5-84f8-c7d0c38d1d8d","max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "id1", "clustering_order": "none", "column_name_bytes": "0x696431", "kind": "partition_key", "position": -1, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "id2", "clustering_order": "asc", "column_name_bytes": "0x696432", "kind": "clustering", "position": 0, "type": "timeuuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl5", "column_name": "text1", "clustering_order": "none", "column_name_bytes": "0x7465787431", "kind": "regular", "position": -1, "type": "text"}
        ];
        const indexRows = [
          {"index_name": "custom_index", "kind": "CUSTOM", "options": {"foo":"bar","class_name":"dummy.DummyIndex","target":"a, b, keys(c)"}}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows,indexRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl5', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length === 1);
          const index = table.indexes[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'custom_index');
          assert.strictEqual(index.target, 'a, b, keys(c)');
          assert.strictEqual(index.isCompositesKind(), false);
          assert.strictEqual(index.isCustomKind(), true);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          assert.strictEqual(index.options['foo'], 'bar');
          assert.strictEqual(index.options['class_name'], 'dummy.DummyIndex');
          done();
        });
      });
      it('should parse with compact storage with pk', function (done) {
        //1 pk, 2 val
        const tableRow = {
          "keyspace_name":"ks_tbl_meta","table_name":"tbl6","bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{"min_threshold":"4","max_threshold":"32","class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"},"compression":{"chunk_length_in_kb":"64","class":"org.apache.cassandra.io.compress.LZ4Compressor"},"dclocal_read_repair_chance":0.1,"default_time_to_live":0,"extensions":{},
          "flags":[],"gc_grace_seconds":864000,"id":"81a32460-5862-11e5-b0ce-c7d0c38d1d8d","max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99PERCENTILE"};
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl6", "column_name": "column1", "clustering_order": "asc", "column_name_bytes": "0x636f6c756d6e31", "kind": "clustering", "position": 0, "type": "text"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl6", "column_name": "id", "clustering_order": "none", "column_name_bytes": "0x6964", "kind": "partition_key", "position": -1, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl6", "column_name": "text1", "clustering_order": "none", "column_name_bytes": "0x7465787431", "kind": "static", "position": -1, "type": "text"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl6", "column_name": "text2", "clustering_order": "none", "column_name_bytes": "0x7465787432", "kind": "static", "position": -1, "type": "text"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl6", "column_name": "value", "clustering_order": "none", "column_name_bytes": "0x76616c7565", "kind": "regular", "position": -1, "type": "blob"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl6', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(Object.keys(table.columnsByName).length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
      it('should parse with compact storage and all columns as clustering keys', function (done) {
        //1 pk, 2 ck. similar to tbl5 but with id2 and text1 as ck
        const tableRow = {
          "keyspace_name": "ks1", "table_name": "tbl10", "bloom_filter_fp_chance": 0.01, "caching": {"keys": "ALL", "rows_per_partition": "NONE"}, "comment": "", "compaction": {"min_threshold": "4", "max_threshold": "32", "class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"}, "compression": {"chunk_length_in_kb": "64", "class": "org.apache.cassandra.io.compress.LZ4Compressor"}, "dclocal_read_repair_chance": 0.1, "default_time_to_live": 0, "extensions": {},
          "flags": ["compound", "dense"], "gc_grace_seconds": 864000, "id": "b4d56ea0-5881-11e5-8326-c7d0c38d1d8d", "max_index_interval": 2048, "memtable_flush_period_in_ms": 0, "min_index_interval": 128, "read_repair_chance": 0.0, "speculative_retry": "99PERCENTILE"};
        const columnRows = [
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl10", "column_name": "id1", "clustering_order": "none", "column_name_bytes": "0x696431", "kind": "partition_key", "position": -1, "type": "uuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl10", "column_name": "id2", "clustering_order": "asc", "column_name_bytes": "0x696432", "kind": "clustering", "position": 0, "type": "timeuuid"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl10", "column_name": "text1", "clustering_order": "asc", "column_name_bytes": "0x7465787431", "kind": "clustering", "position": 1, "type": "text"},
          {"keyspace_name": "ks_tbl_meta", "table_name": "tbl10", "column_name": "value", "clustering_order": "none", "column_name_bytes": "0x76616c7565", "kind": "regular", "position": -1, "type": "empty"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.clusteringKeys.length, 2);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          assert.strictEqual(table.clusteringKeys[1].name, 'text1');
          done();
        });
      });
    });
    describe('with Thrift secondary index', function () {
      it('should parse tables from Thrift with secondary indexes', function (done) {
        const tableRow = {
          keyspace_name: 'ks1', columnfamily_name: 'ThriftSecondaryIndexTest', bloom_filter_fp_chance: null,
          caching: {"keys":"ALL", "rows_per_partition":"NONE"}, cf_id: "121d4950-41fd-11e7-ac8d-e1535f74c6ee",
          column_aliases: [], comparator: 'org.apache.cassandra.db.marshal.UTF8Type',
          compaction_strategy_options: '{}',
          compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}',
          default_validator: 'org.apache.cassandra.db.marshal.BytesType', key_aliases: '["key"]',
          key_validator: 'org.apache.cassandra.db.marshal.UTF8Type', type: 'Standard' };
        const columnRows = [
          { keyspace_name: 'ks1', columnfamily_name: 'ThriftSecondaryIndexTest', column_name: 'ACCOUNT',
            component_index: null, index_name: 'ACCOUNT1', index_options: 'null', index_type: 'KEYS', type: 'regular',
            validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks1', columnfamily_name: 'ThriftSecondaryIndexTest', column_name: 'USER',
            component_index: null, index_name: 'USER1', index_options: 'null', index_type: 'KEYS', type: 'regular',
            validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks1', columnfamily_name: 'ThriftSecondaryIndexTest', column_name: 'key',
            component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'partition_key',
            validator: 'org.apache.cassandra.db.marshal.UTF8Type' }
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.setCassandraVersion([2, 1]);
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'ThriftSecondaryIndexTest', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.columns.length, 3);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.clusteringKeys.length, 0);
          assert.strictEqual(table.indexes.length, 2);
          assert.strictEqual(table.indexes[0].name, 'ACCOUNT1');
          assert.strictEqual(table.indexes[0].target, 'ACCOUNT');
          assert.strictEqual(table.indexes[1].name, 'USER1');
          assert.strictEqual(table.indexes[1].target, 'USER');
          done();
        });
      });
    });
  });
  describe('#getFunctions()', function () {
    it('should return an empty array when not found', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
        assert.ifError(err);
        assert.ok(funcArray);
        assert.strictEqual(funcArray.length, 0);
        done();
      });
    });
    describe('with C* 2.2 metadata rows', function () {
      it('should query once when called in parallel', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            called++;
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { functions: {}};
        utils.times(10, function (n, next) {
          metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 1);
          done();
        });
      });
      it('should query once when called serially', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            called++;
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { functions: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            assert.strictEqual(funcArray.length, 1);
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 1);
          done();
        });
      });
      it('should query the following times if was previously not found', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            if (called++ < 5) {
              return setImmediate(function () {
                cb(null, {rows: []});
              });
            }
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { functions: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            if (n < 5) {
              assert.strictEqual(funcArray.length, 0);
            }
            else {
              //there should be a row
              assert.strictEqual(funcArray.length, 1);
            }
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 6);
          done();
        });
      });
      it('should query the following times if there was an error previously', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            if (called++ < 5) {
              return setImmediate(function () {
                cb(new Error('Dummy'));
              });
            }
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { functions: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
            if (n < 5) {
              assert.ok(err);
              assert.strictEqual(err.message, 'Dummy');
            }
            else {
              assert.ifError(err);
              assert.ok(funcArray);
              assert.strictEqual(funcArray.length, 1);
            }
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 6);
          done();
        });
      });
      it('should parse function metadata with 2 parameters', function (done) {
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"},
          {"keyspace_name":"ks_udf","function_name":"plus","signature":["int","int"],"argument_names":["arg1","arg2"],"argument_types":["org.apache.cassandra.db.marshal.Int32Type","org.apache.cassandra.db.marshal.Int32Type"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.Int32Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
        metadata.keyspaces['ks_udf'] = { functions: {}};
        metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
          assert.ifError(err);
          assert.ok(funcArray);
          assert.strictEqual(funcArray.length, 2);
          assert.strictEqual(funcArray[0].name, 'plus');
          assert.strictEqual(funcArray[0].keyspaceName, 'ks_udf');
          assert.strictEqual(funcArray[0].signature.join(', '), ['bigint', 'bigint'].join(', '));
          assert.strictEqual(funcArray[0].argumentNames.join(', '), ['s', 'v'].join(', '));
          assert.ok(funcArray[0].argumentTypes[0]);
          assert.strictEqual(funcArray[0].argumentTypes[0].code, types.dataTypes.bigint);
          assert.ok(funcArray[0].argumentTypes[1]);
          assert.strictEqual(funcArray[0].argumentTypes[1].code, types.dataTypes.bigint);
          assert.strictEqual(funcArray[0].language, 'java');
          assert.ok(funcArray[0].returnType);
          assert.strictEqual(funcArray[0].returnType.code, types.dataTypes.bigint);

          assert.strictEqual(funcArray[1].name, 'plus');
          assert.strictEqual(funcArray[0].keyspaceName, 'ks_udf');
          assert.strictEqual(funcArray[1].signature.join(', '), ['int', 'int'].join(', '));
          assert.strictEqual(funcArray[1].argumentNames.join(', '), ['arg1', 'arg2'].join(', '));
          assert.ok(funcArray[1].argumentTypes[0]);
          assert.strictEqual(funcArray[1].argumentTypes[0].code, types.dataTypes.int);
          assert.ok(funcArray[1].argumentTypes[1]);
          assert.strictEqual(funcArray[1].argumentTypes[1].code, types.dataTypes.int);
          assert.strictEqual(funcArray[1].language, 'java');
          assert.ok(funcArray[1].returnType);
          assert.strictEqual(funcArray[1].returnType.code, types.dataTypes.int);
          done();
        });
      });
      it('should parse a function metadata with no parameters', function (done) {
        const rows = [
          {"keyspace_name":"ks_udf","function_name":"return_one","signature":[],"argument_names":null,"argument_types":null,"body":"return 1;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.Int32Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
        metadata.keyspaces['ks_udf'] = { functions: {}};
        metadata.getFunctions('ks_udf', 'return_one', function (err, funcArray) {
          assert.ifError(err);
          assert.ok(funcArray);
          assert.strictEqual(funcArray.length, 1);
          assert.strictEqual(funcArray[0].name, 'return_one');
          assert.strictEqual(funcArray[0].signature.length, 0);
          assert.strictEqual(funcArray[0].argumentNames, utils.emptyArray);
          assert.strictEqual(funcArray[0].argumentTypes.length, 0);
          assert.strictEqual(funcArray[0].language, 'java');
          assert.ok(funcArray[0].returnType);
          assert.strictEqual(funcArray[0].returnType.code, types.dataTypes.int);
          done();
        });
      });
    });
    describe('with C* 3.0+ metadata rows', function () {
      it('should parse function metadata with 2 parameters', function (done) {
        const rows = [
          {"keyspace_name": "ks_udf", "function_name": "plus", "argument_types": ["int", "int"], "argument_names": ["s", "v"], "body": "return s+v;", "called_on_null_input": false, "language": "java", "return_type": "int"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces['ks_udf'] = { functions: {}};
        metadata.getFunctions('ks_udf', 'plus', function (err, funcArray) {
          assert.ifError(err);
          assert.ok(funcArray);
          assert.strictEqual(funcArray.length, 1);
          assert.strictEqual(funcArray[0].name, 'plus');
          assert.strictEqual(funcArray[0].keyspaceName, 'ks_udf');
          assert.strictEqual(funcArray[0].signature.join(', '), ['int', 'int'].join(', '));
          assert.strictEqual(funcArray[0].argumentNames.join(', '), ['s', 'v'].join(', '));
          assert.ok(funcArray[0].argumentTypes[0]);
          assert.strictEqual(funcArray[0].argumentTypes[0].code, types.dataTypes.int);
          assert.ok(funcArray[0].argumentTypes[1]);
          assert.strictEqual(funcArray[0].argumentTypes[1].code, types.dataTypes.int);
          assert.strictEqual(funcArray[0].language, 'java');
          assert.ok(funcArray[0].returnType);
          assert.strictEqual(funcArray[0].returnType.code, types.dataTypes.int);
          done();
        });
      });
    });
  });
  describe('#getFunction()', function () {
    context('with no callback specified', function () {
      it('should return a Promise', function () {
        const metadata = newInstance();
        const p = metadata.getFunction('ks1', 'fn1', []);
        helper.assertInstanceOf(p, Promise);
      });
    });
    it('should callback in error if keyspace or name are not provided', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', null, [], function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should callback in error if signature is not an array', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', 'func1', {}, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should callback in error if signature types are not found', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', 'func1', [{code: 0x1000}], function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      let called = 0;
      const rows = [
        {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
      ];
      const cc = {
        query: function (q, cb) {
          called++;
          setImmediate(function () {
            cb(null, {rows: rows});
          });
        },
        getEncoder: () => new Encoder(4, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces['ks_udf'] = { functions: {}};
      utils.times(10, function (n, next) {
        metadata.getFunction('ks_udf', 'plus', ['bigint', 'bigint'], function (err, func) {
          assert.ifError(err);
          assert.ok(func);
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(called, 1);
        done();
      });
    });
    it('should query once when called serially', function (done) {
      let called = 0;
      const rows = [
        {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"}
      ];
      const cc = {
        query: function (q, cb) {
          called++;
          helper.assertContains(q, 'system.schema_functions');
          setImmediate(function () {
            cb(null, {rows: rows});
          });
        },
        getEncoder: () => new Encoder(4, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces['ks_udf'] = { functions: {}};
      utils.timesSeries(10, function (n, next) {
        metadata.getFunction('ks_udf', 'plus', ['bigint', 'bigint'], function (err, func) {
          assert.ifError(err);
          assert.ok(func);
          assert.strictEqual(func.name, 'plus');
          next();
        });
      }, function (err) {
        assert.ifError(err);
        assert.strictEqual(called, 1);
        done();
      });
    });
    it('should return null when not found', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', 'plus', [], function (err, func) {
        assert.ifError(err);
        assert.strictEqual(func, null);
        done();
      });
    });
    it('should parse function metadata with 2 parameters', function (done) {
      const rows = [
        {"keyspace_name":"ks_udf","function_name":"plus","signature":["bigint","bigint"],"argument_names":["s","v"],"argument_types":["org.apache.cassandra.db.marshal.LongType","org.apache.cassandra.db.marshal.LongType"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.LongType"},
        {"keyspace_name":"ks_udf","function_name":"plus","signature":["int","int"],"argument_names":["arg1","arg2"],"argument_types":["org.apache.cassandra.db.marshal.Int32Type","org.apache.cassandra.db.marshal.Int32Type"],"body":"return s+v;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.Int32Type"}
      ];
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', 'plus', ['int', 'int'], function (err, func) {
        assert.ifError(err);
        assert.ok(func);
        assert.strictEqual(func.name, 'plus');
        assert.strictEqual(func.signature.join(', '), ['int', 'int'].join(', '));
        assert.strictEqual(func.argumentNames.join(', '), ['arg1', 'arg2'].join(', '));
        assert.ok(func.argumentTypes[0]);
        assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
        assert.ok(func.argumentTypes[1]);
        assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
        assert.strictEqual(func.language, 'java');
        assert.ok(func.returnType);
        assert.strictEqual(func.returnType.code, types.dataTypes.int);
        done();
      });
    });
    it('should parse a function metadata with no parameters', function (done) {
      const rows = [
        {"keyspace_name":"ks_udf","function_name":"return_one","signature":[],"argument_names":null,"argument_types":null,"body":"return 1;","called_on_null_input":false,"language":"java","return_type":"org.apache.cassandra.db.marshal.Int32Type"}
      ];
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
      metadata.keyspaces['ks_udf'] = { functions: {}};
      metadata.getFunction('ks_udf', 'return_one', [], function (err, func) {
        assert.ifError(err);
        assert.ok(func);
        assert.strictEqual(func.name, 'return_one');
        assert.strictEqual(func.signature.length, 0);
        assert.strictEqual(func.argumentNames, utils.emptyArray);
        assert.strictEqual(func.argumentTypes.length, 0);
        assert.strictEqual(func.language, 'java');
        assert.ok(func.returnType);
        assert.strictEqual(func.returnType.code, types.dataTypes.int);
        done();
      });
    });
  });
  describe('#getAggregates()', function () {
    it('should return an empty array when not found', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
      metadata.keyspaces['ks_udf'] = { aggregates: {}};
      metadata.getAggregates('ks_udf', 'plus', function (err, funcArray) {
        assert.ifError(err);
        assert.ok(funcArray);
        assert.strictEqual(funcArray.length, 0);
        done();
      });
    });
    describe('with C* 2.2 metadata rows', function () {
      it('should query once when called in parallel', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","aggregate_name":"sum","signature":["bigint"],"argument_types":["org.apache.cassandra.db.marshal.LongType"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0,0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.LongType","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            called++;
            helper.assertContains(q, 'system.schema_aggregates');
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { aggregates: {}};
        utils.times(10, function (n, next) {
          metadata.getAggregates('ks_udf', 'sum', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 1);
          done();
        });
      });
      it('should query once when called serially', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","aggregate_name":"sum","signature":["bigint"],"argument_types":["org.apache.cassandra.db.marshal.LongType"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0,0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.LongType","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            called++;
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { aggregates: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getAggregates('ks_udf', 'sum', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            assert.strictEqual(funcArray.length, 1);
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 1);
          done();
        });
      });
      it('should query the following times if was previously not found', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","aggregate_name":"sum","signature":["bigint"],"argument_types":["org.apache.cassandra.db.marshal.LongType"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0,0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.LongType","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            if (called++ < 5) {
              return setImmediate(function () {
                cb(null, {rows: []});
              });
            }
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { aggregates: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getAggregates('ks_udf', 'sum', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            if (n < 5) {
              assert.strictEqual(funcArray.length, 0);
            }
            else {
              //there should be a row
              assert.strictEqual(funcArray.length, 1);
            }
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 6);
          done();
        });
      });
      it('should query the following times if there was an error previously', function (done) {
        let called = 0;
        const rows = [
          {"keyspace_name":"ks_udf","aggregate_name":"sum","signature":["bigint"],"argument_types":["org.apache.cassandra.db.marshal.LongType"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0,0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.LongType","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.LongType"}
        ];
        const cc = {
          query: function (q, cb) {
            helper.assertContains(q, 'system.schema_aggregates');
            if (called++ < 5) {
              return setImmediate(function () {
                cb(new Error('Dummy'));
              });
            }
            setImmediate(function () {
              cb(null, {rows: rows});
            });
          },
          getEncoder: () => new Encoder(4, {})
        };
        const metadata = new Metadata(clientOptions.defaultOptions(), cc);
        metadata.keyspaces['ks_udf'] = { aggregates: {}};
        utils.timesSeries(10, function (n, next) {
          metadata.getAggregates('ks_udf', 'sum', function (err, funcArray) {
            if (n < 5) {
              assert.ok(err);
              assert.strictEqual(err.message, 'Dummy');
            }
            else {
              assert.ifError(err);
              assert.ok(funcArray);
              assert.strictEqual(funcArray.length, 1);
            }
            next();
          });
        }, function (err) {
          assert.ifError(err);
          assert.strictEqual(called, 6);
          done();
        });
      });
      it('should parse aggregate metadata with 1 parameter', function (done) {
        const rows = [
          {"keyspace_name":"ks_udf1","aggregate_name":"sum","signature":["bigint"],"argument_types":["org.apache.cassandra.db.marshal.LongType"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0,0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.LongType","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.LongType"},
          {"keyspace_name":"ks_udf1","aggregate_name":"sum","signature":["int"],"argument_types":["org.apache.cassandra.db.marshal.Int32Type"],"final_func":null,"initcond":utils.allocBufferFromArray([0,0,0,0]),"return_type":"org.apache.cassandra.db.marshal.Int32Type","state_func":"plus","state_type":"org.apache.cassandra.db.marshal.Int32Type"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
        metadata.keyspaces['ks_udf1'] = { aggregates: {}};
        metadata.getAggregates('ks_udf1', 'sum', function (err, aggregatesArray) {
          assert.ifError(err);
          assert.ok(aggregatesArray);
          assert.strictEqual(aggregatesArray.length, 2);
          assert.strictEqual(aggregatesArray[0].name, 'sum');
          assert.strictEqual(aggregatesArray[0].keyspaceName, 'ks_udf1');
          assert.strictEqual(aggregatesArray[0].signature.join(', '), ['bigint'].join(', '));
          assert.strictEqual(aggregatesArray[0].argumentTypes.length, 1);
          assert.ok(aggregatesArray[0].argumentTypes[0]);
          assert.strictEqual(aggregatesArray[0].argumentTypes[0].code, types.dataTypes.bigint);
          assert.ok(aggregatesArray[0].returnType);
          assert.strictEqual(aggregatesArray[0].returnType.code, types.dataTypes.bigint);
          assert.strictEqual(aggregatesArray[0].finalFunction, null);
          assert.ok(aggregatesArray[0].stateType);
          assert.strictEqual(aggregatesArray[0].stateType.code, types.dataTypes.bigint);
          assert.strictEqual(aggregatesArray[0].stateFunction, 'plus');
          assert.strictEqual(aggregatesArray[0].initCondition, '0');

          assert.strictEqual(aggregatesArray[1].name, 'sum');
          assert.strictEqual(aggregatesArray[0].keyspaceName, 'ks_udf1');
          assert.strictEqual(aggregatesArray[1].signature.join(', '), ['int'].join(', '));
          assert.strictEqual(aggregatesArray[1].argumentTypes.length, 1);
          assert.ok(aggregatesArray[1].argumentTypes[0]);
          assert.strictEqual(aggregatesArray[1].argumentTypes[0].code, types.dataTypes.int);
          assert.ok(aggregatesArray[1].returnType);
          assert.strictEqual(aggregatesArray[1].returnType.code, types.dataTypes.int);
          assert.strictEqual(aggregatesArray[1].finalFunction, null);
          assert.ok(aggregatesArray[1].stateType);
          assert.strictEqual(aggregatesArray[1].stateType.code, types.dataTypes.int);
          assert.strictEqual(aggregatesArray[1].stateFunction, 'plus');
          assert.strictEqual(aggregatesArray[1].initCondition, '0');
          done();
        });
      });
    });
    describe('with C* 3.0+ metadata rows', function () {
      it('should parse aggregate metadata with 1 parameter', function (done) {
        const rows = [
          {"keyspace_name": "ks_udf1", "aggregate_name": "sum", "argument_types": ["bigint"], "final_func": null, "initcond": '2', "return_type": "bigint", "state_func": "plus", "state_type": "bigint"},
          {"keyspace_name": "ks_udf1", "aggregate_name": "sum", "argument_types": ["int"], "final_func": null, "initcond": '1', "return_type": "int", "state_func": "plus", "state_type": "int"}
        ];
        const metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows(rows));
        metadata.setCassandraVersion([3, 0]);
        metadata.keyspaces['ks_udf1'] = { aggregates: {}};
        metadata.getAggregates('ks_udf1', 'sum', function (err, aggregatesArray) {
          assert.ifError(err);
          assert.ok(aggregatesArray);
          assert.strictEqual(aggregatesArray.length, 2);
          assert.strictEqual(aggregatesArray[0].name, 'sum');
          assert.strictEqual(aggregatesArray[0].keyspaceName, 'ks_udf1');
          assert.strictEqual(aggregatesArray[0].signature.join(', '), ['bigint'].join(', '));
          assert.strictEqual(aggregatesArray[0].argumentTypes.length, 1);
          assert.ok(aggregatesArray[0].argumentTypes[0]);
          assert.strictEqual(aggregatesArray[0].argumentTypes[0].code, types.dataTypes.bigint);
          assert.ok(aggregatesArray[0].returnType);
          assert.strictEqual(aggregatesArray[0].returnType.code, types.dataTypes.bigint);
          assert.strictEqual(aggregatesArray[0].finalFunction, null);
          assert.ok(aggregatesArray[0].stateType);
          assert.strictEqual(aggregatesArray[0].stateType.code, types.dataTypes.bigint);
          assert.strictEqual(aggregatesArray[0].stateFunction, 'plus');
          assert.strictEqual(aggregatesArray[0].initCondition, '2');

          assert.strictEqual(aggregatesArray[1].name, 'sum');
          assert.strictEqual(aggregatesArray[0].keyspaceName, 'ks_udf1');
          assert.strictEqual(aggregatesArray[1].signature.join(', '), ['int'].join(', '));
          assert.strictEqual(aggregatesArray[1].argumentTypes.length, 1);
          assert.ok(aggregatesArray[1].argumentTypes[0]);
          assert.strictEqual(aggregatesArray[1].argumentTypes[0].code, types.dataTypes.int);
          assert.ok(aggregatesArray[1].returnType);
          assert.strictEqual(aggregatesArray[1].returnType.code, types.dataTypes.int);
          assert.strictEqual(aggregatesArray[1].finalFunction, null);
          assert.ok(aggregatesArray[1].stateType);
          assert.strictEqual(aggregatesArray[1].stateType.code, types.dataTypes.int);
          assert.strictEqual(aggregatesArray[1].stateFunction, 'plus');
          assert.strictEqual(typeof aggregatesArray[1].initCondition, 'string');
          assert.strictEqual(aggregatesArray[1].initCondition, '1');
          done();
        });
      });
    });
  });
  describe('#getMaterializedView()', function () {
    const scoresTableMetadata = new TableMetadata('scores');
    scoresTableMetadata.columnsByName = {
      'score': { type: { code: types.dataTypes.int}, name: 'score' },
      'user': { type: { code: types.dataTypes.text}, name: 'user' },
      'game': { type: { code: types.dataTypes.text}, name: 'game' },
      'year': { type: { code: types.dataTypes.int}, name: 'year'},
      'month': { type: { code: types.dataTypes.int}, name: 'month'},
      'day': { type: { code: types.dataTypes.int}, name: 'day'}
    };
    it('should return null when the view is not found', function (done) {
      const cc = {
        query: function (q, cb) {
          setImmediate(function () {
            //return an empty array
            cb(null, {rows: []});
          });
        },
        getEncoder: () => new Encoder(4, {})
      };
      const metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.setCassandraVersion([3, 0]);
      metadata.keyspaces['ks_mv'] = { views: {}};
      metadata.getMaterializedView('ks_mv', 'not_found', function (err, view) {
        assert.ifError(err);
        assert.strictEqual(view, null);
        done();
      });
    });
    it('should callback in error when cassandra version is lower than 3.0', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), {});
      metadata.setCassandraVersion([2, 1]);
      metadata.keyspaces['ks_mv'] = { views: {}};
      metadata.getTable = function (ksName, name, cb) {
        cb(null, scoresTableMetadata);
      };
      metadata.getMaterializedView('ks_mv', 'view1', function (err) {
        helper.assertInstanceOf(err, errors.NotSupportedError);
        done();
      });
    });
  });
  describe('#buildTokens', function() {
    it('should set sorted tokens from a single host', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      const tokenizer = getTokenizer();
      metadata.tokenizer = tokenizer;
      const hosts = new HostMap();
      const h1 = new Host('127.0.0.1', 2, clientOptions.defaultOptions());
      h1.tokens = ['10', '20', '400', '15', '25', '5'];
      hosts.push(h1.address, h1);
      metadata.buildTokens(hosts);
      const sortedTokens = ['5', '10', '15', '20', '25', '400'];
      const sortedParsedTokens = [];
      sortedTokens.forEach(function (token) {
        sortedParsedTokens.push(tokenizer.parse(token));
      });
      assert.deepEqual(metadata.ring, sortedParsedTokens);
      assert.deepEqual(metadata.ringTokensAsStrings, sortedTokens);
      const ranges = metadata.getTokenRanges();
      const expectedRanges = new Set([ [5, 10], [10, 15], [15, 20], [20, 25], [25, 400], [400, 5] ].map((r) => 
        new TokenRange(token(r[0]), token(r[1]), tokenizer)
      ));
      assert.deepEqual(ranges, expectedRanges);

      // Replicas for a range should be h1 since there is only that host.
      assert.deepEqual(metadata.getReplicas(null, Array.from(ranges)[0]), [h1]);
      done();
    });
    it('should set sorted tokens from multiple hosts', function (done) {
      const metadata = new Metadata(clientOptions.defaultOptions(), null);
      const tokenizer = getTokenizer();
      metadata.tokenizer = tokenizer;
      const hosts = new HostMap();
      const h1 = new Host('127.0.0.1', 2, clientOptions.defaultOptions());
      h1.tokens = ['10', '400', '25', '5'];
      hosts.push(h1.address, h1);
      const h2 = new Host('127.0.0.2', 2, clientOptions.defaultOptions());
      h2.tokens = ['13', '203', '18', '8'];
      hosts.push(h2.address, h2);
      metadata.buildTokens(hosts);
      const sortedTokens = ['5', '8', '10', '13', '18', '25', '203', '400'];
      const sortedParsedTokens = [];
      sortedTokens.forEach(function (token) {
        sortedParsedTokens.push(tokenizer.parse(token));
      });
      assert.deepEqual(metadata.ringTokensAsStrings, sortedTokens);
      assert.deepEqual(metadata.ring, sortedParsedTokens);

      const ranges = metadata.getTokenRanges();
      const expectedRanges = new Set([ [5, 8], [8, 10], [10, 13], [13, 18], [18, 25], [25, 203], [203, 400], [400, 5] ].map((r) => 
        new TokenRange(token(r[0]), token(r[1]), tokenizer)
      ));
      assert.deepEqual(ranges, expectedRanges);

      // Replicas for first range should h2 since it owns ]5,8]
      assert.deepEqual(metadata.getReplicas(null, Array.from(ranges)[0]), [h2]);
      done();
    });
  });
  describe('#newToken()', function () {
    const metadata = new Metadata(clientOptions.defaultOptions(), null);
    metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
    it('should be an instance of Murmur3Token when tokenizer is Murmur3Tokenizer', function () {
      const token = metadata.newToken(utils.allocBufferFromArray([0, 0, 0, 1]));
      helper.assertInstanceOf(token, Murmur3Token);
    });
    it('should create newToken from Buffer', function () {
      const token = metadata.newToken(utils.allocBufferFromArray([0, 0, 0, 1]));
      assert.strictEqual(token.getValue().toString(), '-4069959284402364209');
    });
    it('should create newToken from multiple Buffer components', function () {
      const token = metadata.newToken([utils.allocBufferFromArray([0, 0, 0, 1]), utils.allocBufferFromArray([0,0,0,2])]);
      assert.strictEqual(token.getValue().toString(), '-5168409507470576865');
    });
    it('should create newToken from String', function () {
      const token = metadata.newToken('-4069959284402364209');
      assert.strictEqual(token.getValue().toString(), '-4069959284402364209');
    });
    it('should throw an error if tokenizer isn\'t set yet', function () {
      const metadataNoTokenizer = new Metadata(clientOptions.defaultOptions(), null);
      assert.throws(function () {
        metadataNoTokenizer.newToken('-4069959284402364209');
      }, Error);
    });
  });
  describe('#newTokenRange()', function () {
    const metadata = new Metadata(clientOptions.defaultOptions(), null);
    metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
    const token0 = metadata.newToken(utils.allocBufferFromArray([0, 0, 0, 1]));
    const token1 = metadata.newToken(utils.allocBufferFromArray([0, 0, 0, 2]));
    it('should create TokenRange', function () {
      const range = metadata.newTokenRange(token0, token1);
      assert.deepEqual(range, new TokenRange(token0, token1, tokenizer));
    });
    it('should throw an error if tokenizer isn\'t set yet', function () {
      const metadataNoTokenizer = new Metadata(clientOptions.defaultOptions(), null);
      assert.throws(function () {
        metadataNoTokenizer.newTokenRange(token0, token1, tokenizer);
      }, Error);
    });
  });
});
describe('SchemaParser', function () {
  const isDoneForToken = rewire('../../lib/metadata/schema-parser')['__get__']('isDoneForToken');
  describe('isDoneForToken()', function () {
    it('should skip if dc not included in topology', function () {
      const replicationFactors = { 'dc1': 3, 'dc2': 1 };
      //dc2 does not exist
      const datacenters = {
        'dc1': { hostLength: 6 }
      };
      assert.strictEqual(false, isDoneForToken(replicationFactors, datacenters, {}));
    });
    it('should skip if rf equals to 0', function () {
      //rf 0 for dc2
      const replicationFactors = { 'dc1': 4, 'dc2': 0 };
      const datacenters = {
        'dc1': { hostLength: 6 },
        'dc2': { hostLength: 6 }
      };
      assert.strictEqual(true, isDoneForToken(replicationFactors, datacenters, { 'dc1': 4 }));
    });
    it('should return false for undefined replicasByDc[dcName]', function () {
      const replicationFactors = { 'dc1': 3, 'dc2': 1 };
      //dc2 does not exist
      const datacenters = {
        'dc1': { hostLength: 6 }
      };
      assert.strictEqual(false, isDoneForToken(replicationFactors, datacenters, {}));
    });
  });
});

function getControlConnectionForTable(tableRow, columnRows, indexRows) {
  return {
    queriedTable: 0,
    queriedRows: 0,
    queriedIndexes: 0,
    query: function (q, cb) {
      const self = this;
      setImmediate(function () {
        if (q.indexOf('system.schema_columnfamilies') >= 0 || q.indexOf('system_schema.tables') >= 0) {
          self.queriedTable++;
          return cb(null, { rows: [tableRow]});
        }
        if (q.indexOf('system_schema.indexes') >= 0) {
          self.queriedIndexes++;
          return cb(null, { rows: (indexRows || [])});
        }
        self.queriedRows++;
        cb(null, {rows: columnRows});
      });
    },
    getEncoder: () => new Encoder(1, {})
  };
}

function getControlConnectionForRows(rows, protocolVersion) {
  return {
    query: function (q, w, cb) {
      if (typeof w === 'function') {
        cb = w;
      }
      setImmediate(function () {
        cb(null, {rows: rows});
      });
    },
    getEncoder: () => new Encoder(protocolVersion || 4, {})
  };
}

function getAddress(h) {
  return h.address;
}

/**
 * Creates a dummy tokenizer based on the first byte of the buffer.
 * @returns {Murmur3Tokenizer}
 */
function getTokenizer() {
  const t = new tokenizer.Murmur3Tokenizer();
  //Use the first byte as token
  t.hash = (b) => new Murmur3Token(MutableLong.fromNumber(b[0]));
  t.stringify = stringifyDefault;
  return t;
}

function token(val) {
  return new Murmur3Token(MutableLong.fromNumber(val));
}

function stringifyDefault(v) {
  return v.getValue().toString();
}

/** @returns {Metadata} */
function newInstance() {
  return new Metadata(clientOptions.defaultOptions(), getControlConnectionForRows([]));
}
