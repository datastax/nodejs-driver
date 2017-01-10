"use strict";
var assert = require('assert');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var errors = require('../../../lib/errors');
var vdescribe = helper.vdescribe;

vdescribe('2.2', 'Metadata', function () {
  this.timeout(60000);
  before(helper.ccmHelper.start(1, { yaml: ['enable_user_defined_functions:true']}));
  var keyspace = 'ks_udf';
  before(function createSchema(done) {
    var client = newInstance();
    var queries = [
      "CREATE KEYSPACE  ks_udf WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
      "CREATE FUNCTION  ks_udf.return_one() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'",
      "CREATE FUNCTION  ks_udf.plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;'",
      "CREATE FUNCTION  ks_udf.plus(s bigint, v bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return s+v;'",
      "CREATE AGGREGATE ks_udf.sum(int) SFUNC plus STYPE int INITCOND 1",
      "CREATE AGGREGATE ks_udf.sum(bigint) SFUNC plus STYPE bigint INITCOND 2"
    ];
    utils.eachSeries(queries, client.execute.bind(client), helper.finish(client, done));
  });
  after(helper.ccmHelper.remove);
  describe('#getFunctions()', function () {
    it('should retrieve the metadata of cql functions', function (done) {
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance();
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
      if(!helper.promiseSupport) {
        it('should throw an ArgumentError', function (done) {
          var client = newInstance();
          utils.series([
            client.connect.bind(client),
            function (next) {
              assert.throws(function () {
                client.metadata.getFunctions(keyspace, 'plus');
              }, errors.ArgumentError);
              next();
            },
            client.shutdown.bind(client)
          ], done);
        });
        return;
      }
      it('should return functions in a promise', function () {
        var client = newInstance();
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
      var client = newInstance();
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
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retrieve the metadata for a cql function without arguments', function (done) {
      var client = newInstance();
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
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found', function (done) {
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance({ keyspace: keyspace });
      var nonSyncClient = newInstance({ keyspace: keyspace, isMetadataSyncEnabled: false });
      var clients = [client, nonSyncClient];
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
              assert.strictEqual(func.body, 'return Integer.toString(i) + \"hello\";');
              eachNext();
            });
          }, next);
        },
        client.shutdown.bind(client),
        nonSyncClient.shutdown.bind(nonSyncClient)
      ], done);
    });
    describe('with no callback specified', function () {
      if(!helper.promiseSupport) {
        it('should throw an ArgumentError', function (done) {
          var client = newInstance();
          utils.series([
            client.connect.bind(client),
            function (next) {
              assert.throws(function () {
                client.metadata.getFunction(keyspace, 'plus', ['int', 'int']);
              }, errors.ArgumentError);
              next();
            },
            client.shutdown.bind(client)
          ], done);
        });
        return;
      }
      it('should return function in a promise', function () {
        var client = newInstance();
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
          });
      });
    });
  });
  describe('#getAggregates()', function () {
    it('should retrieve the metadata of cql aggregates', function (done) {
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance();
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
      if(!helper.promiseSupport) {
        it('should throw an ArgumentError', function (done) {
          var client = newInstance();
          utils.series([
            client.connect.bind(client),
            function (next) {
              assert.throws(function () {
                client.metadata.getAggregates(keyspace, 'sum');
              }, errors.ArgumentError);
              next();
            },
            client.shutdown.bind(client)
          ], done);
        });
        return;
      }
      it('should return aggregates in a promise', function () {
        var client = newInstance();
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
      var client = newInstance();
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
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should return null when not found', function (done) {
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance();
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
      var client = newInstance({ keyspace: keyspace });
      var nonSyncClient = newInstance({ keyspace: keyspace, isMetadataSyncEnabled: false });
      var clients = [client, nonSyncClient];
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
      if(!helper.promiseSupport) {
        it('should throw an ArgumentError', function (done) {
          var client = newInstance();
          utils.series([
            client.connect.bind(client),
            function (next) {
              assert.throws(function () {
                client.metadata.getAggregate(keyspace, 'sum', ['int'])
              }, errors.ArgumentError);
              next();
            },
            client.shutdown.bind(client)
          ], done);
        });
        return;
      }
      it('should return aggregate in a promise', function () {
        var client = newInstance();
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
