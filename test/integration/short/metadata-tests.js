"use strict";
var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var vit = helper.vit;

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
          client.execute("ALTER KEYSPACE ks3 WITH replication = {'class' : 'NetworkTopologyStrategy', 'datacenter2' : 1}", function (err, result) {
            setTimeout(function() {
              checkKeyspace('ks3', 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter2', '1');
              done();
            }, 2000);
          });
        });
      });
    }),
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
        m.getUdt('ks1', 'udt_does_not_exists', function (err, udtInfo) {
          assert.ifError(err);
          assert.strictEqual(udtInfo, null);
          done();
        });
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
            assert.strictEqual(udt.fields[1].type.code, types.dataTypes.varchar);
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
          client.execute('SELECT * FROM system.schema_keyspaces', [], { traceQuery: true}, next);
        },
        function getTrace(result, next) {
          client.metadata.getTrace(result.info.traceId, next);
        },
        function checkTrace(trace, next) {
          assert.ok(trace);
          assert.strictEqual(typeof trace.duration, 'number');
          assert.ok(trace.events.length);
          next();
        }
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
    before(function createTables(done) {
      var client = newInstance();
      var queries = [
        "CREATE KEYSPACE ks_tbl_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
        "CREATE TABLE ks_tbl_meta.tbl1 (id uuid PRIMARY KEY, text_sample text)",
        "CREATE TABLE ks_tbl_meta.tbl2 (id uuid, text_sample text, PRIMARY KEY ((id, text_sample)))",
        "CREATE TABLE ks_tbl_meta.tbl3 (id uuid, text_sample text, PRIMARY KEY (id, text_sample))",
        "CREATE TABLE ks_tbl_meta.tbl4 (zck timeuuid, apk2 text, pk1 uuid, val2 blob, valz1 int, PRIMARY KEY ((pk1, apk2), zck))",
        "CREATE TABLE ks_tbl_meta.tbl5 (id1 uuid, id2 timeuuid, text1 text, PRIMARY KEY (id1, id2)) WITH COMPACT STORAGE",
        "CREATE TABLE ks_tbl_meta.tbl6 (id uuid, text1 text, text2 text, PRIMARY KEY (id)) WITH COMPACT STORAGE",
        "CREATE TABLE ks_tbl_meta.tbl7 (id1 uuid, id3 timeuuid, zid2 text, int_sample int, PRIMARY KEY (id1, zid2, id3)) WITH CLUSTERING ORDER BY (zid2 ASC, id3 DESC)",
        "CREATE TABLE ks_tbl_meta.tbl8 (id uuid, rating_value counter, rating_votes counter, PRIMARY KEY (id))"
      ];
      async.eachSeries(queries, client.execute.bind(client), helper.wait(500, done));
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
          assert.strictEqual(table.columns.length, 2);
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
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
