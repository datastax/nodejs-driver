"use strict";
var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../test-helper.js');
var Client = require('../../lib/client.js');
var clientOptions = require('../../lib/client-options.js');
var Host = require('../../lib/host.js').Host;
var Metadata = require('../../lib/metadata');
var tokenizer = require('../../lib/tokenizer');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var Encoder = require('../../lib/encoder');

describe('Metadata', function () {
  describe('#getReplicas()', function () {
    it('should return depending on the rf and ring size with simple strategy', function () {
      var metadata = new Metadata(clientOptions.defaultOptions());
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.ring = [0, 1, 2, 3, 4, 5];
      metadata.primaryReplicas = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5'};
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'SimpleStrategy',
        'strategy_options': '{"replication_factor": 3}'
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //Primary replica plus the 2 next tokens
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '0');
      assert.strictEqual(replicas[1], '1');
      assert.strictEqual(replicas[2], '2');

      replicas = metadata.getReplicas('dummy', new Buffer([5]));
      assert.ok(replicas);
      assert.strictEqual(replicas.length, 3);
      assert.strictEqual(replicas[0], '5');
      assert.strictEqual(replicas[1], '0');
      assert.strictEqual(replicas[2], '1');
    });
    it('should return depending on the dc rf with network topology', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var metadata = new Metadata(options);
      metadata.tokenizer = new tokenizer.Murmur3Tokenizer();
      //Use the value as token
      metadata.tokenizer.hash = function (b) { return b[0]};
      metadata.tokenizer.compare = function (a, b) {if (a > b) return 1; if (a < b) return -1; return 0};
      metadata.datacenters = {'dc1': 4, 'dc2': 4};
      metadata.ring = [0, 1, 2, 3, 4, 5, 6, 7];
      //load even primary replicas
      metadata.primaryReplicas = {};
      for (var i = 0; i < metadata.ring.length; i ++) {
        var h = new Host(i.toString(), 2, options);
        h.datacenter = 'dc' + ((i % 2) + 1);
        metadata.primaryReplicas[i.toString()] = h;
      }
      metadata.setKeyspaces({rows: [{
        'keyspace_name': 'dummy',
        'strategy_class': 'NetworkTopologyStrategy',
        'strategy_options': '{"dc1": "3", "dc2": "1"}'
      }]});
      var replicas = metadata.getReplicas('dummy', new Buffer([0]));
      assert.ok(replicas);
      //3 replicas from dc1 and 1 replica from dc2
      assert.strictEqual(replicas.length, 4);
      assert.strictEqual(replicas[0].address, '0');
      assert.strictEqual(replicas[1].address, '1');
      assert.strictEqual(replicas[2].address, '2');
      assert.strictEqual(replicas[3].address, '4');
    });
  });
  describe('#clearPrepared()', function () {
    it('should clear the internal state', function () {
      var metadata = new Metadata(clientOptions.defaultOptions());
      metadata.getPreparedInfo('QUERY1');
      metadata.getPreparedInfo('QUERY2');
      assert.strictEqual(metadata.preparedQueries['__length'], 2);
      metadata.clearPrepared();
      assert.strictEqual(metadata.preparedQueries['__length'], 0);
    });
  });
  describe('#getUdt()', function () {
    it('should retrieve the udt information', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.ok(udtInfo);
        assert.strictEqual(udtInfo.name, 'udt1');
        assert.ok(udtInfo.fields);
        assert.strictEqual(udtInfo.fields.length, 2);
        done();
      });
    });
    it('should callback in err when there is an error', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(new Error('Test error'));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
    });
    it('should be null when it is not found', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: []}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should be null when keyspace does not exists', function (done) {
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = {};
      metadata.getUdt('ks1', 'udt1', function (err, udtInfo) {
        assert.ifError(err);
        assert.strictEqual(udtInfo, null);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.UTF8Type']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      async.times(50, function (n, next) {
        metadata.getUdt('ks1', 'udt5', function (err, udtInfo) {
          if (err) return next(err);
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
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            queried++;
            cb(null, new types.ResultSet({ rows: [ {
              field_names: ['field1', 'field2'],
              field_types: ['org.apache.cassandra.db.marshal.UUIDType', 'org.apache.cassandra.db.marshal.BooleanType']
            }]}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      //no keyspace named ks1 in metadata
      metadata.keyspaces = { ks1: { udts: {}}};
      //Invoke multiple times in parallel
      async.timesSeries(50, function (n, next) {
        metadata.getUdt('ks1', 'udt10', function (err, udtInfo) {
          if (err) return next(err);
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
      var queried = 0;
      var cc = {
        query: function (q, cb) {
          queried++;
          setImmediate(function () {
            cb(null, new types.ResultSet({ rows: []}));
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks1: { udts: {}}};
      async.timesSeries(20, function (n, next) {
        metadata.getUdt('ks1', 'udt20', function (err, udtInfo) {
          if (err) return next(err);
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
      var sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: 2002
      };
      var eventRows = [ {
        event_id: types.TimeUuid.now(),
        activity: 'act 1',
        source: types.InetAddress.fromString('10.10.10.2'),
        source_elapsed: 101
      }];
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system_traces.sessions') >= 0) {
              return cb(null, { rows: [ sessionRow]});
            }
            cb(null, { rows: eventRows})
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (err, trace) {
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
    it('should retry if its not already stored', function (done) {
      var sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: null
      };
      var eventRows = [ {
        event_id: types.TimeUuid.now(),
        activity: 'act 2',
        source: types.InetAddress.fromString('10.10.10.1'),
        source_elapsed: 102
      }];
      var calls = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system_traces.sessions') >= 0) {
              if (++calls > 1) {
                sessionRow.duration = 2002;
              }
              return cb(null, { rows: [ sessionRow]});
            }
            cb(null, { rows: eventRows})
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
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
      var sessionRow = {
        request: 'request value',
        coordinator: types.InetAddress.fromString('10.10.10.1'),
        parameters: ['a', 'b'],
        started_at: new Date(),
        duration: null //duration null means that trace is not fully flushed
      };
      var calls = 0;
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            //try with empty result and null duration
            var rows = [];
            if (++calls > 1) {
              rows = [ sessionRow ];
            }
            cb(null, { rows: rows});
          });
        },
        getEncoder: function () { return new Encoder(1, {})}
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.getTrace(types.Uuid.random(), function (err, trace) {
        assert.ok(err);
        assert.ok(!trace);
        assert.ok(err.message);
        assert.ok(err.message.indexOf('attempt') > 0);
        done();
      });
    });
    it('should callback in error if there was an error retrieving the trace', function (done) {
      var err = new Error('dummy err');
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            cb(err);
          });
        }
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
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
      var cc = {
        query: function (q, cb) {
          setImmediate(function () {
            if (q.indexOf('system.schema_columnfamilies') >= 0) {
              return cb(null, {rows: []});
            }
            cb(null, {rows: []});
          });
        },
        getEncoder: function () { return new Encoder(1, {}); }
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      metadata.getTable('ks_tbl_meta', 'tbl_does_not_exists', function (err, table) {
        assert.ifError(err);
        assert.strictEqual(table, null);
        done();
      });
    });
    it('should be null when keyspace does not exists', function (done) {
      var metadata = new Metadata(clientOptions.defaultOptions(), {});
      metadata.keyspaces = { };
      metadata.getTable('ks_does_not_exists', 'tbl1', function (err, table) {
        assert.ifError(err);
        assert.strictEqual(table, null);
        done();
      });
    });
    it('should query once when called in parallel', function (done) {
      var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
        column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
        comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
        key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
      var columnRows = [
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' }
      ];
      var calledTable = 0;
      var calledRows = 0;
      var cc = {
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
        getEncoder: function () { return new Encoder(1, {}); }
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      async.times(100, function (n, next) {
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
      var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
        column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
        comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
        key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
      var columnRows = [
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
        { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' }
      ];
      var calledTable = 0;
      var calledRows = 0;
      var cc = {
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
        getEncoder: function () { return new Encoder(1, {}); }
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      async.timesSeries(100, function (n, next) {
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
      var calledTable = 0;
      var calledRows = 0;
      var cc = {
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
        getEncoder: function () { return new Encoder(1, {}); }
      };
      var metadata = new Metadata(clientOptions.defaultOptions(), cc);
      metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
      async.timesSeries(100, function (n, next) {
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
    describe('with C*2.0+ metadata rows', function () {
      it('should parse partition and clustering keys', function (done) {
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.02, caching: 'KEYS_ONLY',
          column_aliases: '["ck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, index_interval: 128, is_dense: false,
          key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0.1, max_compaction_threshold: 32, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0, replicate_on_write: true, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: null };
        var columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'apk2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'ck', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'pk1', component_index: 0, index_name: null, index_options: null, index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val1', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.Int32Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val2', component_index: 1, index_name: null, index_options: null, index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.BytesType' }
        ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.02);
          assert.strictEqual(table.isCompact, false);
          assert.ok(table.caching);
          assert.strictEqual(table.caching, 'KEYS_ONLY');
          assert.strictEqual(table.columns.length, 5);
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
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: '{"keys":"ALL", "rows_per_partition":"NONE"}', cf_id: types.Uuid.fromString('96c86920-ed84-11e4-8991-199e66562428'),
          column_aliases: '["id2"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.TimeUUIDType', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.UTF8Type', dropped_columns: null, gc_grace_seconds: 864000, index_interval: null, is_dense: true, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0.1, max_compaction_threshold: 32, max_index_interval: 2048, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, min_index_interval: 128, read_repair_chance: 0, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard', value_alias: 'text1' };
        var columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'id1', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'id2', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'clustering_key', validator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text1', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'compact_value', validator: 'org.apache.cassandra.db.marshal.UTF8Type' }
        ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
        var tableRow = {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","bloom_filter_fp_chance":0.01,"caching":"{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}","cf_id":"609f53a0-038b-11e5-be48-0d419bfb85c8","column_aliases":"[]","comment":"","compaction_strategy_class":"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy","compaction_strategy_options":"{}","comparator":"org.apache.cassandra.db.marshal.UTF8Type","compression_parameters":"{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}","default_time_to_live":0,"default_validator":"org.apache.cassandra.db.marshal.BytesType","dropped_columns":null,"gc_grace_seconds":864000,"index_interval":null,"is_dense":false,"key_aliases":"[\"id\"]","key_validator":"org.apache.cassandra.db.marshal.UUIDType","local_read_repair_chance":0.1,"max_compaction_threshold":32,"max_index_interval":2048,"memtable_flush_period_in_ms":0,"min_compaction_threshold":4,"min_index_interval":128,"read_repair_chance":0,"speculative_retry":"99.0PERCENTILE","subcomparator":null,"type":"Standard","value_alias":null};
        var columnRows = [
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"id","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"partition_key","validator":"org.apache.cassandra.db.marshal.UUIDType"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text1","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"},
          {"keyspace_name":"ks_tbl_meta","columnfamily_name":"tbl1","column_name":"text2","component_index":null,"index_name":null,"index_options":"null","index_type":null,"type":"regular","validator":"org.apache.cassandra.db.marshal.UTF8Type"}
        ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
    });
    describe('with C*1.2 metadata rows', function () {
      it('should parse partition and clustering keys', function (done) {
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["zck"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null,
          key_aliases: '["pk1","apk2"]', key_validator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        var columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'val2', component_index: 1, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.BytesType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'valz1', component_index: 1, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.Int32Type' }
        ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.columns.length, 5);
          assert.strictEqual(table.partitionKeys.length, 2);
          assert.strictEqual(table.partitionKeys[0].name, 'pk1');
          assert.strictEqual(table.partitionKeys[1].name, 'apk2');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'zck');
          done();
        });
      });
      it('should parse with no clustering keys', function (done) {
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '[]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}', comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null,
          key_aliases: '["id"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        var columnRows = [ { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text_sample', component_index: 0, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' } ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl5', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["id2"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.TimeUUIDType', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}',
          default_validator: 'org.apache.cassandra.db.marshal.UTF8Type', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard',
          value_alias: 'text1' };
        var columnRows = [];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '[]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.UTF8Type', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: null };
        var columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text1', component_index: null, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', column_name: 'text2', component_index: null, index_name: null, index_options: null, index_type: null, validator: 'org.apache.cassandra.db.marshal.UTF8Type' }
        ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
        var tableRow = { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl1', bloom_filter_fp_chance: 0.01, caching: 'KEYS_ONLY',
          column_aliases: '["id2","text1"]', comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}',
          comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimeUUIDType,org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.SnappyCompressor"}', default_validator: 'org.apache.cassandra.db.marshal.BytesType', gc_grace_seconds: 864000, id: null, key_alias: null, key_aliases: '["id1"]', key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0, max_compaction_threshold: 32, min_compaction_threshold: 4, populate_io_cache_on_flush: false, read_repair_chance: 0.1, replicate_on_write: true, subcomparator: null, type: 'Standard', value_alias: '' };
        var columnRows = [];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
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
    describe('with C*2.2+ metadata rows', function () {
      it('should parse new 2.2 types', function (done) {
        var tableRow = {
          keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', bloom_filter_fp_chance: 0.03, caching: '{"keys":"ALL", "rows_per_partition":"NONE"}',
          cf_id: types.Uuid.fromString('c05f4c40-fe05-11e4-8481-277ff03b5030'), comment: '', compaction_strategy_class: 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', compaction_strategy_options: '{}', comparator: 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)', compression_parameters: '{"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"}', default_time_to_live: 0, default_validator: 'org.apache.cassandra.db.marshal.BytesType', dropped_columns: null, gc_grace_seconds: 864000, is_dense: false, key_validator: 'org.apache.cassandra.db.marshal.UUIDType', local_read_repair_chance: 0.1, max_compaction_threshold: 32, max_index_interval: 2048, memtable_flush_period_in_ms: 0, min_compaction_threshold: 4, min_index_interval: 128, read_repair_chance: 0, speculative_retry: '99.0PERCENTILE', subcomparator: null, type: 'Standard' };
        var columnRows = [
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'date_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.SimpleDateType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'id', component_index: null, index_name: null, index_options: 'null', index_type: null, type: 'partition_key', validator: 'org.apache.cassandra.db.marshal.UUIDType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'smallint_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.ShortType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'time_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.TimeType' },
          { keyspace_name: 'ks_tbl_meta', columnfamily_name: 'tbl_c22', column_name: 'tinyint_sample', component_index: 0, index_name: null, index_options: 'null', index_type: null, type: 'regular', validator: 'org.apache.cassandra.db.marshal.ByteType' } ];
        var metadata = new Metadata(clientOptions.defaultOptions(), getControlConnectionForTable(tableRow, columnRows));
        metadata.keyspaces = { ks_tbl_meta: { tables: {}}};
        metadata.getTable('ks_tbl_meta', 'tbl_c22', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.03);
          assert.strictEqual(table.isCompact, false);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 5);
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
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
    });
  });
});

function getControlConnectionForTable(tableRow, columnRows) {
  return {
    query: function (q, cb) {
      setImmediate(function () {
        if (q.indexOf('system.schema_columnfamilies') >= 0) {
          return cb(null, {rows: [tableRow]});
        }
        cb(null, {rows: columnRows});
      });
    },
    getEncoder: function () { return new Encoder(1, {}); }
  };
}