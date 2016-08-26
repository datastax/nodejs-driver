"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var vit = helper.vit;

describe('custom payload', function () {
  this.timeout(60000);
  var keyspace = helper.getRandomName('ks');
  var table = keyspace + '.' + helper.getRandomName('tbl');
  before(helper.ccmHelper.start(1, {
    jvmArgs: ['-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler']
  }));
  before(function (done) {
    var client = newInstance();
    utils.series([
      client.connect.bind(client),
      helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace)),
      helper.toTask(client.execute, client, helper.createTableCql(table)),
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccmHelper.remove);
  describe('using Client#execute(query, params, { prepare: 0 }, callback)', function () {
    vit('2.2', 'should encode and decode the payload with rows response', function (done) {
      var client = newInstance();
      var payload = {
        'key1': new Buffer('val1')
      };
      utils.series([
        client.connect.bind(client),
        function execute(next) {
          client.execute('SELECT key FROM system.local', [], { customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key1'], Buffer);
            assert.strictEqual(result.info.customPayload['key1'].toString(), 'val1');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.2', 'should encode and decode the payload with void response', function (done) {
      var client = newInstance();
      var payload = {
        'key2': new Buffer('val2')
      };
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          client.execute(query, [types.Uuid.random(), 'text1'], { customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key2'], Buffer);
            assert.strictEqual(result.info.customPayload['key2'].toString(), 'val2');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.2', 'should encode and decode the payload and trace', function (done) {
      var client = newInstance();
      var payload = {
        'key3': new Buffer('val3')
      };
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          client.execute(query, [types.Uuid.random(), 'text3'], { customPayload: payload, traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            helper.assertInstanceOf(result.info.customPayload['key3'], Buffer);
            assert.strictEqual(result.info.customPayload['key3'].toString(), 'val3');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('using Client#execute(query, params, { prepare: 1 }, callback)', function () {
    vit('2.2', 'should encode and decode the payload with rows response', function (done) {
      var client = newInstance();
      var payload = {
        'key-prep1': new Buffer('val-prep1'),
        'key-prep2': new Buffer('val-prep2')
      };
      utils.series([
        client.connect.bind(client),
        function execute(next) {
          client.execute('SELECT key FROM system.local', [], { prepare: 1, customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key-prep1'], Buffer);
            assert.strictEqual(result.info.customPayload['key-prep1'].toString(), 'val-prep1');
            helper.assertInstanceOf(result.info.customPayload['key-prep2'], Buffer);
            assert.strictEqual(result.info.customPayload['key-prep2'].toString(), 'val-prep2');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('using Client#batch(queries, { prepare: 0 }, callback)', function () {
    vit('2.2', 'should encode and decode the payload', function (done) {
      var client = newInstance();
      var payload = {
        'key-batch1': new Buffer('val-batch1')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          var q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          var queries = [
            { query: q, params: [types.Uuid.random(), 'text-batch1'] },
            { query: q, params: [types.Uuid.random(), 'text-batch2'] }
          ];
          client.batch(queries, { customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key-batch1'], Buffer);
            assert.strictEqual(result.info.customPayload['key-batch1'].toString(), 'val-batch1');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.2', 'should encode and decode the payload with warnings', function (done) {
      var client = newInstance();
      var payload = {
        'key-batch2': new Buffer('val-batch2')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          var q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          var queries = [
            { query: q, params: [types.Uuid.random(), 'text-batch2'] },
            { query: q, params: [types.Uuid.random(), utils.stringRepeat('a', 5 * 1025)] }
          ];
          client.batch(queries, { customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key-batch2'], Buffer);
            assert.strictEqual(result.info.customPayload['key-batch2'].toString(), 'val-batch2');
            assert.ok(result.info.warnings);
            assert.strictEqual(result.info.warnings.length, 1);
            helper.assertContains(result.info.warnings[0], 'batch');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('using Client#batch(queries, { prepare: 1 }, callback)', function () {
    vit('2.2', 'should encode and decode the payload', function (done) {
      var client = newInstance();
      var payload = {
        'key-batch-prep1': new Buffer('val-batch-prep1')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          var q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          var queries = [
            { query: q, params: [types.Uuid.random(), 'text-batch1'] },
            { query: q, params: [types.Uuid.random(), 'text-batch2'] }
          ];
          client.batch(queries, { prepare: 1, customPayload: payload}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.info.customPayload);
            helper.assertInstanceOf(result.info.customPayload['key-batch-prep1'], Buffer);
            assert.strictEqual(result.info.customPayload['key-batch-prep1'].toString(), 'val-batch-prep1');
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});

/**
 * @param [options]
 * @returns {Client}
 */
function newInstance(options) {
  return new Client(utils.deepExtend({ pooling: { heartBeatInterval: 0 }}, helper.baseOptions, options));
}
