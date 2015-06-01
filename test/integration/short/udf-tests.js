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
  before(helper.ccmHelper.start(1, { yaml: ['enable_user_defined_functions: true']}));
  var keyspace = 'ks_udf';
  before(function createSchema(done) {
    var client = newInstance();
    var queries = [
      "CREATE KEYSPACE ks_udf WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
      "CREATE FUNCTION ks_udf.return_one() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'",
      "CREATE FUNCTION ks_udf.plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;'",
      "CREATE FUNCTION ks_udf.plus(s bigint, v bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return s+v;'"
    ];
    async.eachSeries(queries, client.execute.bind(client), helper.finish(client, done));
  });
  after(helper.ccmHelper.remove);
  describe('#getFunctions()', function () {
    it('should retrieve the metadata of cql functions', function (done) {
      var client = newInstance();
      async.series([
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
      async.series([
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
      async.series([
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
      async.series([
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
  });
  describe('#getFunction()', function () {
    it('should retrieve the metadata of cql functions', function (done) {
      var client = newInstance();
      async.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'plus', ['int', 'int'], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'plus');
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
      async.series([
        client.connect.bind(client),
        function checkMeta(next) {
          client.metadata.getFunction(keyspace, 'return_one', [], function (err, func) {
            assert.ifError(err);
            assert.ok(func);
            assert.strictEqual(func.name, 'return_one');
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
      async.series([
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
    it('should return null when the keyspace does not exists', function (done) {
      var client = newInstance();
      async.series([
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
  });
});


/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({}, helper.baseOptions, options));
}
