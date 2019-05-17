/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');

const helper = require('../../test-helper');
const vit = helper.vit;
const Client = require('../../../lib/client');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const vdescribe = helper.vdescribe;

vdescribe('2.2', 'Metadata', function () {
  this.timeout(60000);
  before(helper.ccmHelper.start(1, { yaml: ['enable_user_defined_functions:true']}));
  const keyspace = 'ks_udf';
  before(function createSchema(done) {
    const client = newInstance();
    const queries = [
      "CREATE KEYSPACE  ks_udf WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
      "CREATE FUNCTION  ks_udf.return_one() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'",
      "CREATE FUNCTION  ks_udf.plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;'",
      "CREATE FUNCTION  ks_udf.plus(s bigint, v bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return s+v;'",
      "CREATE AGGREGATE ks_udf.sum(int) SFUNC plus STYPE int INITCOND 1",
      "CREATE AGGREGATE ks_udf.sum(bigint) SFUNC plus STYPE bigint INITCOND 2"
    ];
    if (helper.isDseGreaterThan('6')) {
      queries.push(
        "CREATE FUNCTION ks_udf.deterministic(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return dividend / divisor;'",
        "CREATE FUNCTION ks_udf.monotonic(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int MONOTONIC LANGUAGE java AS 'return dividend / divisor;'",
        "CREATE FUNCTION ks_udf.md(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC LANGUAGE java AS 'return dividend / divisor;'",
        "CREATE FUNCTION ks_udf.monotonic_on(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int MONOTONIC ON dividend LANGUAGE java AS 'return dividend / divisor;'",
        "CREATE AGGREGATE ks_udf.deta(int) SFUNC plus STYPE int INITCOND 0 DETERMINISTIC;"
      );
    }
    utils.eachSeries(queries, client.execute.bind(client), helper.finish(client, done));
  });
  after(helper.ccmHelper.remove);
  describe('#getFunctions()', function () {
    it('should retrieve the metadata of cql functions', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunctions(keyspace, 'plus', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            assert.strictEqual(funcArray.length, 2);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the metadata for a cql function without arguments', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunctions(keyspace, 'return_one', function (err, funcArray) {
            assert.ifError(err);
            assert.ok(funcArray);
            assert.strictEqual(funcArray.length, 1);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return an empty array when not found', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunctions(keyspace, 'func_does_not_exists', function (err, funcArray) {
            assert.ifError(err);
            assert.strictEqual(funcArray.length, 0);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return an empty array when the keyspace does not exists', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunctions('ks_does_not_exists', 'func1', function (err, funcArray) {
            assert.ifError(err);
            assert.strictEqual(funcArray.length, 0);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    describe('with no callback specified', function () {
      it('should return functions in a promise', function () {
        const client = newInstance();
        return client.connect()
          .then(function () {
            return client.metadata.getFunctions(keyspace, 'plus');
          })
          .then(function (funcArray) {
            assert.ok(funcArray);
            assert.strictEqual(funcArray.length, 2);
          });
      });
    });
  });
  describe('#getFunction()', function () {
    it('should retrieve the metadata of a cql function', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'plus', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'plus');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, false);
            assert.strictEqual(func.monotonic, false);
            assert.deepEqual(func.monotonicOn, utils.emptyArray);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the metadata for a cql function without arguments', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'return_one', [], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'return_one');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.signature.length, 0);
            assert.strictEqual(func.argumentNames.length, 0);
            assert.strictEqual(func.argumentTypes.length, 0);
            assert.strictEqual(func.deterministic, false);
            assert.strictEqual(func.monotonic, false);
            assert.deepEqual(func.monotonicOn, utils.emptyArray);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('dse-6.0', 'should retrieve the metadata of a deterministic cql function', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'deterministic', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'deterministic');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, true);
            assert.strictEqual(func.monotonic, false);
            assert.deepEqual(func.monotonicOn, utils.emptyArray);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('dse-6.0', 'should retrieve the metadata of a monotonic cql function', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'monotonic', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'monotonic');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, false);
            assert.strictEqual(func.monotonic, true);
            assert.deepEqual(func.monotonicOn, ['dividend', 'divisor']);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('dse-6.0', 'should retrieve the metadata of a deterministic and monotonic cql function', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'md', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'md');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, true);
            assert.strictEqual(func.monotonic, true);
            assert.deepEqual(func.monotonicOn, ['dividend', 'divisor']);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('dse-6.0', 'should retrieve the metadata of a partially monotonic cql function', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'monotonic_on', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'monotonic_on');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, false);
            assert.strictEqual(func.monotonic, false);
            assert.deepEqual(func.monotonicOn, ['dividend']);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'func_does_not_exists', [], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found by signature', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'plus', ['int'], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when the keyspace does not exists', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction('ks_does_not_exists', 'func1', ['int'], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the most up to date metadata', function (done) {
      const client = newInstance({ keyspace: keyspace });
      const nonSyncClient = newInstance({ keyspace: keyspace, isMetadataSyncEnabled: false });
      const clients = [client, nonSyncClient];
      utils.series([
        client.connect.bind(client),
        nonSyncClient.connect.bind(nonSyncClient),
        helper.toTask(client.execute, client, "CREATE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i);'"),
        function checkMetaInit(next) {
          utils.each(clients, function(client, eachNext) {
            client.metadata.getFunction(keyspace, 'stringify', ['int'], function (err, func) {
              assert.ifError(err);
              assert.ok(func);
              assert.strictEqual(func.name, 'stringify');
              assert.strictEqual(func.body, 'return Integer.toString(i);');
              eachNext();
            });
          }, next);
        },
        helper.toTask(client.execute, client, "CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i) + \"hello\";'"),
        function checkMetaAfter(next) {
          utils.each(clients, function(client, eachNext) {
            client.metadata.getFunction(keyspace, 'stringify', ['int'], function (err, func) {
              assert.ifError(err);
              assert.ok(func);
              assert.strictEqual(func.name, 'stringify');
              assert.strictEqual(func.body, 'return Integer.toString(i) + "hello";');
              eachNext();
            });
          }, next);
        },
        client.shutdown.bind(client),
        nonSyncClient.shutdown.bind(nonSyncClient)
      ], done);
    });
    describe('with no callback specified', function () {
      it('should return function in a promise', function () {
        const client = newInstance();
        return client.connect()
          .then(function () {
            return client.metadata.getFunction(keyspace, 'plus', ['int', 'int']);
          })
          .then(function (func) {
            assert.ok(func);
            assert.strictEqual(func.name, 'plus');
            assert.strictEqual(func.keyspaceName, keyspace);
            assert.strictEqual(func.argumentTypes.length, 2);
            assert.strictEqual(func.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(func.argumentTypes[1].code, types.dataTypes.int);
            assert.strictEqual(func.returnType.code, types.dataTypes.int);
            assert.strictEqual(func.deterministic, false);
            assert.strictEqual(func.monotonic, false);
            assert.deepEqual(func.monotonicOn, utils.emptyArray);
          });
      });
    });
  });
  describe('#getAggregates()', function () {
    it('should retrieve the metadata of cql aggregates', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregates(keyspace, 'sum', function (err, aggregatesArray) {
            assert.ifError(err);
            assert.ok(aggregatesArray);
            assert.strictEqual(aggregatesArray.length, 2);
            assert.strictEqual(aggregatesArray[0].name, 'sum');
            assert.strictEqual(aggregatesArray[1].name, 'sum');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return an empty array when not found', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregates(keyspace, 'aggregate_does_not_exists', function (err, funcArray) {
            assert.ifError(err);
            assert.strictEqual(funcArray.length, 0);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return an empty array when the keyspace does not exists', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregates('ks_does_not_exists', 'aggr1', function (err, funcArray) {
            assert.ifError(err);
            assert.strictEqual(funcArray.length, 0);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    describe('with no callback specified', function () {
      it('should return aggregates in a promise', function () {
        const client = newInstance();
        return client.connect()
          .then(function () {
            return client.metadata.getAggregates(keyspace, 'sum');
          })
          .then(function (aggregatesArray) {
            assert.ok(aggregatesArray);
            assert.strictEqual(aggregatesArray.length, 2);
            assert.strictEqual(aggregatesArray[0].name, 'sum');
            assert.strictEqual(aggregatesArray[1].name, 'sum');
          });
      });
    });
  });
  describe('#getAggregate()', function () {
    it('should retrieve the metadata of a cql aggregate', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregate(keyspace, 'sum', ['int'], function (err, aggregate) {
            assert.ifError(err);
            assert.ok(aggregate);
            assert.strictEqual(aggregate.name, 'sum');
            assert.strictEqual(aggregate.keyspaceName, keyspace);
            assert.strictEqual(aggregate.argumentTypes.length, 1);
            assert.strictEqual(aggregate.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(aggregate.returnType.code, types.dataTypes.int);
            assert.strictEqual(aggregate.stateFunction, 'plus');
            assert.strictEqual(aggregate.initCondition, '1');
            assert.strictEqual(aggregate.deterministic, false);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('dse-6.0', 'should retrieve the metadata of a deterministic cql aggregate', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregate(keyspace, 'deta', ['int'], function (err, aggregate) {
            assert.ifError(err);
            assert.ok(aggregate);
            assert.strictEqual(aggregate.name, 'deta');
            assert.strictEqual(aggregate.keyspaceName, keyspace);
            assert.strictEqual(aggregate.argumentTypes.length, 1);
            assert.strictEqual(aggregate.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(aggregate.returnType.code, types.dataTypes.int);
            assert.strictEqual(aggregate.stateFunction, 'plus');
            assert.strictEqual(aggregate.initCondition, '0');
            assert.strictEqual(aggregate.deterministic, true);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregate(keyspace, 'aggregate_does_not_exists', [], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found by signature', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregate(keyspace, 'sum', ['text'], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when the keyspace does not exists', function (done) {
      const client = newInstance();
      utils.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getAggregate('ks_does_not_exists', 'func1', ['int'], function (err, func) {
            assert.ifError(err);
            assert.strictEqual(func, null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the most up to date metadata', function (done) {
      const client = newInstance({ keyspace: keyspace });
      const nonSyncClient = newInstance({ keyspace: keyspace, isMetadataSyncEnabled: false });
      const clients = [client, nonSyncClient];
      utils.series([
        client.connect.bind(client),
        nonSyncClient.connect.bind(nonSyncClient),
        helper.toTask(client.execute, client, "CREATE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 0"),
        function checkMetaInit(next) {
          utils.each(clients, function(client, eachNext) {
            client.metadata.getAggregate(keyspace, 'sum2', ['int'], function (err, func) {
              assert.ifError(err);
              assert.ok(func);
              assert.strictEqual(func.name, 'sum2');
              assert.strictEqual(func.initCondition, '0');
              eachNext();
            });
          }, next);
        },
        helper.toTask(client.execute, client, "CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 200"),
        function checkMetaAfter(next) {
          utils.each(clients, function(client, eachNext) {
            client.metadata.getAggregate(keyspace, 'sum2', ['int'], function (err, func) {
              assert.ifError(err);
              assert.ok(func);
              assert.strictEqual(func.name, 'sum2');
              //changed
              assert.strictEqual(func.initCondition, '200');
              eachNext();
            });
          }, next);
        },
        client.shutdown.bind(client),
        nonSyncClient.shutdown.bind(nonSyncClient)
      ], done);
    });
    describe('with no callback specified', function () {
      it('should return aggregate in a promise', function () {
        const client = newInstance();
        return client.connect()
          .then(function () {
            return client.metadata.getAggregate(keyspace, 'sum', ['int']);
          })
          .then(function (aggregate) {
            assert.ok(aggregate);
            assert.strictEqual(aggregate.name, 'sum');
            assert.strictEqual(aggregate.keyspaceName, keyspace);
            assert.strictEqual(aggregate.argumentTypes.length, 1);
            assert.strictEqual(aggregate.argumentTypes[0].code, types.dataTypes.int);
            assert.strictEqual(aggregate.returnType.code, types.dataTypes.int);
            assert.strictEqual(aggregate.stateFunction, 'plus');
            assert.strictEqual(aggregate.initCondition, '1');
          });
      });
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}
