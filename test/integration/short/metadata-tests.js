"use strict";
var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var vit = helper.vit;
var vdescribe = helper.vdescribe;

describe('Metadata', function () {
  this.timeout(60000);
  before(helper.ccmHelper.start(2, {vnodes: true}));
  after(helper.ccmHelper.remove);
  describe('#keyspaces', function () {
    it('should keep keyspace information up to date', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var m = client.metadata;
        assert.ok(m);
        assert.ok(m.keyspaces);
        assert.ok(m.keyspaces['system']);
        assert.ok(m.keyspaces['system'].strategy);
        async.series([
          helper.toTask(client.execute, client, "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}"),
          helper.toTask(client.execute, client, "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}"),
          helper.toTask(client.execute, client, "CREATE KEYSPACE ks3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}"),
          helper.toTask(client.execute, client, "CREATE KEYSPACE ks4 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : 1}")
        ], function (err) {
          function checkKeyspace(name, strategy, optionName, optionValue) {
            var ks = m.keyspaces[name];
            assert.ok(ks);
            assert.strictEqual(ks.strategy, strategy);
            assert.ok(ks.strategyOptions);
            assert.strictEqual(ks.strategyOptions[optionName], optionValue);
          }
          assert.ifError(err);
          assert.ok(Object.keys(m.keyspaces).length > 4);
          checkKeyspace('ks1', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '3');
          checkKeyspace('ks2', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '2');
          checkKeyspace('ks3', 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor', '1');
          checkKeyspace('ks4', 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1', '1');
          client.execute("ALTER KEYSPACE ks3 WITH replication = {'class' : 'NetworkTopologyStrategy', 'datacenter2' : 1}", function (err) {
            assert.ifError(err);
            setTimeout(function() {
              checkKeyspace('ks3', 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter2', '1');
              done();
            }, 2000);
          });
        });
      });
    });
    it('should delete keyspace information on drop', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var m = client.metadata;
        assert.ok(m);
        async.series([
          helper.toTask(client.execute, client, helper.createKeyspaceCql('ks_todelete', 1)),
          function checkKeyspaceExists(next) {
            var ks = m.keyspaces['ks_todelete'];
            assert.ok(ks);
            next();
          },
          helper.toTask(client.execute, client, 'DROP KEYSPACE ks_todelete;'),
          function (next) {
            setTimeout(next, 2000);
          },
          function checkKeyspaceDropped(next) {
            var ks = m.keyspaces['ks_todelete'];
            assert.strictEqual(ks, undefined);
            next();
          }
        ], done);
      });
    });
  });
  describe('#getUdt()', function () {
    vit('2.1', 'should return null if it does not exists', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var m = client.metadata;
        async.timesSeries(10, function (n, next) {
          m.getUdt('ks1', 'udt_does_not_exists', function (err, udtInfo) {
            assert.ifError(err);
            assert.strictEqual(udtInfo, null);
            next();
          });
        }, helper.finish(client, done));
      });
    });
    vit('2.1', 'should return the udt information', function (done) {
      var client = newInstance();
      var createUdtQuery1 = 'CREATE TYPE phone (alias text, number text, country_code int)';
      var createUdtQuery2 = 'CREATE TYPE address (street text, "ZIP" int, phones set<frozen<phone>>)';
      async.series([
        helper.toTask(client.connect, client),
        helper.toTask(client.execute, client, helper.createKeyspaceCql('ks_udt1', 3)),
        helper.toTask(client.execute, client, 'USE ks_udt1'),
        helper.toTask(client.execute, client, createUdtQuery1),
        helper.toTask(client.execute, client, createUdtQuery2),
        function checkPhoneUdt(next) {
          var m = client.metadata;
          m.getUdt('ks_udt1', 'phone', function (err, udtInfo) {
            assert.ifError(err);
            assert.ok(udtInfo);
            assert.strictEqual(udtInfo.name, 'phone');
            assert.ok(udtInfo.fields);
            assert.strictEqual(udtInfo.fields.length, 3);
            assert.strictEqual(udtInfo.fields[0].name, 'alias');
            assert.strictEqual(udtInfo.fields[0].type.code, types.dataTypes.varchar);
            assert.strictEqual(udtInfo.fields[1].name, 'number');
            assert.strictEqual(udtInfo.fields[1].type.code, types.dataTypes.varchar);
            assert.strictEqual(udtInfo.fields[2].name, 'country_code');
            assert.strictEqual(udtInfo.fields[2].type.code, types.dataTypes.int);
            next();
          });
        },
        function checkAddressUdt(next) {
          var m = client.metadata;
          m.getUdt('ks_udt1', 'address', function (err, udtInfo) {
            assert.ifError(err);
            assert.ok(udtInfo);
            assert.strictEqual(udtInfo.name, 'address');
            assert.strictEqual(udtInfo.fields.length, 3);
            assert.strictEqual(udtInfo.fields[0].name, 'street');
            assert.strictEqual(udtInfo.fields[0].type.code, types.dataTypes.varchar);
            assert.strictEqual(udtInfo.fields[1].name, 'ZIP');
            assert.strictEqual(udtInfo.fields[1].type.code, types.dataTypes.int);
            assert.strictEqual(udtInfo.fields[2].name, 'phones');
            assert.strictEqual(udtInfo.fields[2].type.code, types.dataTypes.set);
            assert.strictEqual(udtInfo.fields[2].type.info.code, types.dataTypes.udt);
            assert.strictEqual(udtInfo.fields[2].type.info.info.name, 'phone');
            assert.strictEqual(udtInfo.fields[2].type.info.info.fields.length, 3);
            assert.strictEqual(udtInfo.fields[2].type.info.info.fields[0].name, 'alias');
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should retrieve the updated metadata after a schema change', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, helper.createKeyspaceCql('ks_udt_meta', 3)),
        helper.toTask(client.execute, client, 'USE ks_udt_meta'),
        helper.toTask(client.execute, client, 'CREATE TYPE type_changing (id uuid, name ascii)'),
        function checkType1(next) {
          client.metadata.getUdt('ks_udt_meta', 'type_changing', function (err, udt) {
            assert.ifError(err);
            assert.ok(udt);
            assert.strictEqual(udt.fields.length, 2);
            next();
          });
        },
        helper.toTask(client.execute, client, 'ALTER TYPE type_changing ALTER name TYPE varchar'),
        function (next) {
          setTimeout(next, 2000);
        },
        function checkType2(next) {
          client.metadata.getUdt('ks_udt_meta', 'type_changing', function (err, udt) {
            assert.ifError(err);
            assert.ok(udt);
            assert.strictEqual(udt.fields.length, 2);
            assert.strictEqual(udt.fields[1].name, 'name');
            assert.ok(udt.fields[1].type.code === types.dataTypes.varchar || udt.fields[1].type.code === types.dataTypes.text);
            next();
          });
        }
      ], done);
    });
  });
  describe('#getTrace()', function () {
    it('should retrieve the trace immediately after', function (done) {
      var client = newInstance();
      async.waterfall([
        client.connect.bind(client),
        function executeQuery(next) {
          client.execute(helper.queries.basic, [], { traceQuery: true}, next);
        },
        function getTrace(result, next) {
          client.metadata.getTrace(result.info.traceId, next);
        },
        function checkTrace(trace, next) {
          assert.ok(trace);
          assert.strictEqual(typeof trace.duration, 'number');
          if (client.controlConnection.protocolVersion >= 4) {
            //Check the new field added in C* 2.2
            helper.assertInstanceOf(trace.clientAddress, types.InetAddress);
          }
          assert.ok(trace.events.length);
          next();
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the trace a few seconds after', function (done) {
      var client = newInstance();
      async.waterfall([
        client.connect.bind(client),
        function executeQuery(next) {
          client.execute('SELECT * FROM system.local', [], { traceQuery: true}, next);
        },
        function getTrace(result, next) {
          client.metadata.getTrace(result.info.traceId, function (err, trace) {
            setTimeout(function () {
              next(err, trace);
            }, 1500);
          });
        },
        function checkTrace(trace, next) {
          assert.ok(trace);
          assert.strictEqual(typeof trace.duration, 'number');
          assert.ok(trace.events.length);
          next();
        }
      ], done);
    });
  });
  describe('#getTable()', function () {
    var keyspace = 'ks_tbl_meta';
    var is3  = helper.isCassandraGreaterThan('3.0');
    var valuesIndex = (is3 ? "(values(map_values))" : "(map_values)");
    before(function createTables(done) {
      var client = newInstance();
      var queries = [
        "CREATE KEYSPACE ks_tbl_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
        "USE ks_tbl_meta",
        "CREATE TABLE tbl1 (id uuid PRIMARY KEY, text_sample text)",
        "CREATE TABLE tbl2 (id uuid, text_sample text, PRIMARY KEY ((id, text_sample)))",
        "CREATE TABLE tbl3 (id uuid, text_sample text, PRIMARY KEY (id, text_sample))",
        "CREATE TABLE tbl4 (zck timeuuid, apk2 text, pk1 uuid, val2 blob, valz1 int, PRIMARY KEY ((pk1, apk2), zck))",
        "CREATE TABLE tbl5 (id1 uuid, id2 timeuuid, text1 text, PRIMARY KEY (id1, id2)) WITH COMPACT STORAGE",
        "CREATE TABLE tbl6 (id uuid, text1 text, text2 text, PRIMARY KEY (id)) WITH COMPACT STORAGE",
        "CREATE TABLE tbl7 (id1 uuid, id3 timeuuid, zid2 text, int_sample int, PRIMARY KEY (id1, zid2, id3)) WITH CLUSTERING ORDER BY (zid2 ASC, id3 DESC)",
        "CREATE TABLE tbl8 (id uuid, rating_value counter, rating_votes counter, PRIMARY KEY (id))",
        "CREATE TABLE ks_tbl_meta.tbl_collections (id uuid, ck blob, list_sample list<int>, set_sample list<text>, int_sample int, map_sample map<text,int>, PRIMARY KEY (id, ck))",
        "CREATE INDEX text_index ON tbl1 (text_sample)"
      ];
      if (helper.isCassandraGreaterThan('2.1')) {
        queries.push(
          "CREATE TABLE tbl_indexes1 (id uuid PRIMARY KEY, map_values map<text,int>, map_keys map<text,int>, map_entries map<text,int>, map_all map<text,int>, list_sample frozen<list<blob>>)",
          "CREATE INDEX map_keys_index ON tbl_indexes1 (keys(map_keys))",
          "CREATE INDEX map_values_index ON tbl_indexes1 " + valuesIndex,
          "CREATE INDEX list_index ON tbl_indexes1 (full(list_sample))",
          "CREATE TYPE udt1 (i int, b blob, t text)",
          "CREATE TABLE tbl_udts1 (id uuid PRIMARY KEY, udt_sample frozen<udt1>)",
          "CREATE TABLE tbl_udts2 (id frozen<udt1> PRIMARY KEY)"
        );
      }
      if (helper.isCassandraGreaterThan('2.2')) {
        queries.push(
          'CREATE INDEX map_entries_index ON tbl_indexes1 (entries(map_entries))'
        );
      }
      if (is3) {
        queries.push(
          "CREATE INDEX map_all_entries_index on tbl_indexes1 (entries(map_all))",
          "CREATE INDEX map_all_keys_index on tbl_indexes1 (keys(map_all))",
          "CREATE INDEX map_all_values_index on tbl_indexes1 (values(map_all))"
        )
      }
      async.eachSeries(queries, client.execute.bind(client), helper.finish(client, done));
    });
    it('should retrieve the metadata of a single partition key table', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.01);
          assert.ok(table.caching);
          assert.strictEqual(typeof table.caching, 'string');
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id', 'text_sample']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 0);
          assert.strictEqual(table.clusteringOrder.length, 0);
          done();
        });
      });
    });
    it('should retrieve the metadata of a composite partition key table', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl2', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 2);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id', 'text_sample']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 2);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.partitionKeys[1].name, 'text_sample');
          done();
        });
      });
    });
    it('should retrieve the metadata of a partition key and clustering key table', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl3', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.01);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 2);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id', 'text_sample']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'text_sample');
          assert.strictEqual(table.clusteringOrder.length, 1);
          assert.strictEqual(table.clusteringOrder[0], 'ASC');
          done();
        });
      });
    });
    it('should retrieve the metadata of a table with reversed clustering order', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl7', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.01);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 4);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id1', 'id3', 'int_sample', 'zid2']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id1');
          assert.strictEqual(table.clusteringKeys.length, 2);
          assert.strictEqual(table.clusteringKeys[0].name, 'zid2');
          assert.strictEqual(table.clusteringKeys[1].name, 'id3');
          assert.strictEqual(table.clusteringOrder.length, 2);
          assert.strictEqual(table.clusteringOrder[0], 'ASC');
          assert.strictEqual(table.clusteringOrder[1], 'DESC');
          done();
        });
      });
    });
    it('should retrieve the metadata of a counter table', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl8', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 3);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id', 'rating_value', 'rating_votes']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.partitionKeys[0].type.code, types.dataTypes.uuid);
          assert.strictEqual(table.clusteringKeys.length, 0);
          assert.strictEqual(table.columnsByName['rating_value'].type.code, types.dataTypes.counter);
          assert.strictEqual(table.columnsByName['rating_votes'].type.code, types.dataTypes.counter);
          //true counter tables
          assert.strictEqual(table.replicateOnWrite, true);
          done();
        });
      });
    });
    it('should retrieve the metadata of a compact storaged table', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl6', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.bloomFilterFalsePositiveChance, 0.01);
          assert.ok(table.caching);
          assert.strictEqual(table.columns.length, 3);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id', 'text1', 'text2']);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 0);
          done();
        });
      });
    });
    it('should retrieve the metadata of a compact storaged table with clustering key', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl5', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 3);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['id1', 'id2', 'text1']);
          assert.strictEqual(table.isCompact, true);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id1');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'id2');
          done();
        });
      });
    });
    it('should retrieve the updated metadata after a schema change', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, 'CREATE TABLE ks_tbl_meta.tbl_changing (id uuid PRIMARY KEY, text_sample text)'),
        function checkTable1(next) {
          client.metadata.getTable('ks_tbl_meta', 'tbl_changing', function (err, table) {
            assert.ifError(err);
            assert.ok(table);
            assert.strictEqual(table.columns.length, 2);
            next();
          });
        },
        helper.toTask(client.execute, client, 'ALTER TABLE ks_tbl_meta.tbl_changing ADD new_col1 timeuuid'),
        function (next) {
          setTimeout(next, 2000);
        },
        function checkTable2(next) {
          client.metadata.getTable('ks_tbl_meta', 'tbl_changing', function (err, table) {
            assert.ifError(err);
            assert.ok(table);
            assert.strictEqual(table.columns.length, 3);
            assert.ok(table.columnsByName['new_col1']);
            assert.strictEqual(table.columnsByName['new_col1'].type.code, types.dataTypes.timeuuid);
            next();
          });
        }
      ], done);
    });
    it('should retrieve the metadata of a table with ColumnToCollectionType', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_collections', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          var columns = table.columns
            .map(function (c) { return c.name; })
            .sort();
          helper.assertValueEqual(columns, ['ck', 'id', 'int_sample', 'list_sample', 'map_sample', 'set_sample']);
          assert.strictEqual(table.isCompact, false);
          assert.strictEqual(table.partitionKeys.length, 1);
          assert.strictEqual(table.partitionKeys[0].name, 'id');
          assert.strictEqual(table.clusteringKeys.length, 1);
          assert.strictEqual(table.clusteringKeys[0].name, 'ck');
          client.shutdown(done);
        });
      });
    });
    it('should retrieve a simple secondary index', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.strictEqual(table.columns.length, 2);
          assert.ok(table.indexes);
          assert.strictEqual(table.indexes.length, 1);
          var index = table.indexes[0];
          assert.strictEqual(index.name, 'text_index');
          assert.strictEqual(index.target, 'text_sample');
          assert.strictEqual(index.isCompositesKind(), true);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          client.shutdown(done);
        });
      });
    });
    vit('2.1', 'should retrieve a secondary index on map keys', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_indexes1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length > 0);
          var index = table.indexes.filter(function (x) { return x.name === 'map_keys_index'; })[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'map_keys_index');
          assert.strictEqual(index.target, 'keys(map_keys)');
          assert.strictEqual(index.isCompositesKind(), true);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          client.shutdown(done);
        });
      });
    });
    vit('2.1', 'should retrieve a secondary index on map values', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_indexes1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length > 0);
          var index = table.indexes.filter(function (x) { return x.name === 'map_values_index'; })[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'map_values_index');
          assert.strictEqual(index.target, is3 ? 'values(map_values)' : 'map_values');
          assert.strictEqual(index.isCompositesKind(), true);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          client.shutdown(done);
        });
      });
    });
    vit('2.2', 'should retrieve a secondary index on map entries', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_indexes1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length > 0);
          var index = table.indexes.filter(function (x) { return x.name === 'map_entries_index'; })[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'map_entries_index');
          assert.strictEqual(index.target, 'entries(map_entries)');
          assert.strictEqual(index.isCompositesKind(), true);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          client.shutdown(done);
        });
      });
    });
    vit('3.0', 'should retrieve multiple indexes on same map column', function (done) {
      var client = newInstance({ keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_indexes1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length > 0);

          var indexes = [
            {name: 'map_all_entries_index', target: 'entries(map_all)'},
            {name: 'map_all_keys_index', target: 'keys(map_all)'},
            {name: 'map_all_values_index', target: 'values(map_all)'}
          ];

          indexes.forEach(function(idx) {
            var index = table.indexes.filter(function (x) { return x.name === idx.name; })[0];
            assert.ok(index, 'Index not found');
            assert.strictEqual(index.name, idx.name);
            assert.strictEqual(index.target, idx.target);
            assert.strictEqual(index.isCompositesKind(), true);
            assert.strictEqual(index.isCustomKind(), false);
            assert.strictEqual(index.isKeysKind(), false);
            assert.ok(index.options);
          });
          client.shutdown(done);
        });
      });
    });
    vit('2.1', 'should retrieve a secondary index on frozen list', function (done) {
      var client = newInstance({keyspace: keyspace});
      client.connect(function (err) {
        assert.ifError(err);
        client.metadata.getTable(keyspace, 'tbl_indexes1', function (err, table) {
          assert.ifError(err);
          assert.ok(table);
          assert.ok(table.indexes);
          assert.ok(table.indexes.length > 0);
          var index = table.indexes.filter(function (x) {
            return x.name === 'list_index';
          })[0];
          assert.ok(index, 'Index not found');
          assert.strictEqual(index.name, 'list_index');
          assert.strictEqual(index.target, 'full(list_sample)');
          assert.strictEqual(index.isCompositesKind(), true);
          assert.strictEqual(index.isCustomKind(), false);
          assert.strictEqual(index.isKeysKind(), false);
          assert.ok(index.options);
          client.shutdown(done);
        });
      });
    });
    vit('2.2', 'should retrieve the metadata of a table containing new 2.2 types', function (done) {
      var client = newInstance();
      var createTableCql = 'CREATE TABLE ks_tbl_meta.tbl_c22 ' +
        '(id uuid PRIMARY KEY, smallint_sample smallint, tinyint_sample tinyint, date_sample date, time_sample time)';
      async.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, createTableCql),
        function checkTable(next) {
          client.metadata.getTable('ks_tbl_meta', 'tbl_c22', function (err, table) {
            assert.ifError(err);
            assert.ok(table);
            assert.strictEqual(table.columns.length, 5);
            assert.ok(table.columnsByName['smallint_sample']);
            assert.ok(table.columnsByName['tinyint_sample']);
            assert.ok(table.columnsByName['date_sample']);
            assert.ok(table.columnsByName['time_sample']);
            assert.strictEqual(table.columnsByName['smallint_sample'].type.code, types.dataTypes.smallint);
            assert.strictEqual(table.columnsByName['tinyint_sample'].type.code, types.dataTypes.tinyint);
            assert.strictEqual(table.columnsByName['date_sample'].type.code, types.dataTypes.date);
            assert.strictEqual(table.columnsByName['time_sample'].type.code, types.dataTypes.time);
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should retrieve the metadata of a table with udt column', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        function checkMetadata(next) {
          client.metadata.getTable(keyspace, 'tbl_udts1', function (err, table) {
            assert.ifError(err);
            assert.ok(table);
            assert.deepEqual(table.columns.map(function (c) { return c.name; }), ['id', 'udt_sample']);
            var udtColumn = table.columnsByName['udt_sample'];
            assert.ok(udtColumn);
            assert.strictEqual(udtColumn.type.code, types.dataTypes.udt);
            assert.ok(udtColumn.type.info);
            assert.strictEqual(udtColumn.type.info.name, 'udt1');
            assert.deepEqual(udtColumn.type.info.fields.map(function (f) {return f.name;}), ['i', 'b', 't']);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.1', 'should retrieve the metadata of a table with udt partition key', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        function checkMetadata(next) {
          client.metadata.getTable(keyspace, 'tbl_udts2', function (err, table) {
            assert.ifError(err);
            assert.ok(table);
            assert.deepEqual(table.columns.map(function (c) { return c.name; }), ['id']);
            var udtColumn = table.columns[0];
            assert.ok(udtColumn);
            assert.strictEqual(table.partitionKeys[0], udtColumn);
            assert.strictEqual(udtColumn.type.code, types.dataTypes.udt);
            assert.ok(udtColumn.type.info);
            assert.strictEqual(udtColumn.type.info.name, 'udt1');
            assert.deepEqual(udtColumn.type.info.fields.map(function (f) {return f.name;}), ['i', 'b', 't']);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  vdescribe('3.0', '#getMaterializedView()', function () {
    var keyspace = 'ks_view_meta';
    before(function createTables(done) {
      var client = newInstance();
      var queries = [
        "CREATE KEYSPACE ks_view_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
        "CREATE TABLE ks_view_meta.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
        "CREATE MATERIALIZED VIEW ks_view_meta.dailyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY ((game, year, month, day), score, user) WITH CLUSTERING ORDER BY (score DESC)"
      ];
      async.eachSeries(queries, client.execute.bind(client), function (err) {
        client.shutdown();
        if (err) {
          return done(err);
        }
        setTimeout(done, 2000);
      });
    });
    it('should retrieve the view and table metadata', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getMaterializedView(keyspace, 'dailyhigh', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.name, 'dailyhigh');
            assert.strictEqual(view.tableName, 'scores');
            assert.strictEqual(view.whereClause, 'game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL');
            assert.strictEqual(view.includeAllColumns, false);
            assert.strictEqual(view.clusteringKeys.length, 2);
            assert.strictEqual(view.clusteringKeys[0].name, 'score');
            assert.strictEqual(view.clusteringKeys[1].name, 'user');
            assert.strictEqual(view.partitionKeys.length, 4);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'game, year, month, day');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should refresh the view metadata via events', function (done) {
      var client = newInstance({ keyspace: 'ks_view_meta' });
      async.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, 'CREATE MATERIALIZED VIEW monthlyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL PRIMARY KEY ((game, year, month), score, user, day) WITH CLUSTERING ORDER BY (score DESC) AND compaction = { \'class\' : \'SizeTieredCompactionStrategy\' }'),
        function checkView1(next) {
          client.metadata.getMaterializedView('ks_view_meta', 'monthlyhigh', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.length, 3);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'game, year, month');
            assert.strictEqual(view.clusteringKeys.map(function (x) { return x.name;}).join(', '), 'score, user, day');
            helper.assertContains(view.compactionClass, 'SizeTieredCompactionStrategy');
            next();
          });
        },
        helper.toTask(client.execute, client, 'ALTER MATERIALIZED VIEW monthlyhigh WITH compaction = { \'class\' : \'LeveledCompactionStrategy\' }'),
        function checkView1(next) {
          client.metadata.getMaterializedView('ks_view_meta', 'monthlyhigh', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.length, 3);
            assert.strictEqual(view.clusteringKeys.length, 3);
            helper.assertContains(view.compactionClass, 'LeveledCompactionStrategy');
            next();
          });
        },
        helper.toTask(client.execute, client, 'DROP MATERIALIZED VIEW monthlyhigh'),
        function checkDropped(next) {
          client.metadata.getMaterializedView('ks_view_meta', 'monthlyhigh', function (err, view) {
            assert.ifError(err);
            assert.strictEqual(view, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should refresh the view metadata as result of table change via events', function (done) {
      var client = newInstance({ keyspace: 'ks_view_meta' });
      async.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, 'CREATE TABLE users (user TEXT PRIMARY KEY, first_name TEXT)'),
        // create a view using 'select *'.
        helper.toTask(client.execute, client, 'CREATE MATERIALIZED VIEW users_by_first_all AS SELECT * FROM users WHERE user IS NOT NULL AND first_name IS NOT NULL PRIMARY KEY (first_name, user)'),
        // create same view using 'select <columns>'.
        helper.toTask(client.execute, client, 'CREATE MATERIALIZED VIEW users_by_first AS SELECT user, first_name FROM users WHERE user IS NOT NULL AND first_name IS NOT NULL PRIMARY KEY (first_name, user)'),
        function checkAllView(next) {
          client.metadata.getMaterializedView('ks_view_meta', 'users_by_first_all', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'first_name');
            assert.strictEqual(view.clusteringKeys.map(function (x) { return x.name;}).join(', '), 'user');
            // includeAllColumns should be true since 'select *' was used.
            assert.strictEqual(view.includeAllColumns, true);
            next();
          });
        },
        function checkView(next) {
          client.metadata.getMaterializedView('ks_view_meta', 'users_by_first', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'first_name');
            assert.strictEqual(view.clusteringKeys.map(function (x) { return x.name;}).join(', '), 'user');
            assert.strictEqual(view.includeAllColumns, false);
            next();
          });
        },
        helper.toTask(client.execute, client, 'ALTER TABLE users ADD last_name text'),
        function checkForNewColumnsInAllView(next) {
          // ensure that the newly added column 'last_name' in 'users' was propagated to users_by_first_all.
          client.metadata.getMaterializedView('ks_view_meta', 'users_by_first_all', function(err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'first_name');
            assert.strictEqual(view.clusteringKeys.map(function (x) { return x.name;}).join(', '), 'user');
            assert.ok(view.columnsByName['last_name']);
            assert.ok(view.columnsByName['last_name'].type.code === types.dataTypes.varchar ||
              view.columnsByName['last_name'].type.code === types.dataTypes.text);
            assert.strictEqual(view.columns.length, 3);
            assert.strictEqual(view.includeAllColumns, true);
            next();
          });
        },
        function checkColumnNotAddedInView(next) {
          // since 'users_by_first' does not include all columns it should not detect the new column.
          client.metadata.getMaterializedView('ks_view_meta', 'users_by_first', function (err, view) {
            assert.ifError(err);
            assert.ok(view);
            assert.strictEqual(view.partitionKeys.map(function (x) { return x.name;}).join(', '), 'first_name');
            assert.strictEqual(view.clusteringKeys.map(function (x) { return x.name;}).join(', '), 'user');
            assert.strictEqual(view.columnsByName['last_name'], undefined);
            assert.strictEqual(view.columns.length, 2);
            assert.strictEqual(view.includeAllColumns, false);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    })
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
