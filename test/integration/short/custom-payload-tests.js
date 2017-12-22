"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const Client = require('../../../lib/client');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');
const vit = helper.vit;

describe('custom payload', function () {
  this.timeout(60000);
  const keyspace = helper.getRandomName('ks');
  const table = keyspace + '.' + helper.getRandomName('tbl');
  before(helper.ccmHelper.start(1, {
    jvmArgs: ['-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler']
  }));
  before(function (done) {
    const client = newInstance();
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
      const client = newInstance();
      const payload = {
        'key1': utils.allocBufferFromString('val1')
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
      const client = newInstance();
      const payload = {
        'key2': utils.allocBufferFromString('val2')
      };
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
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
      const client = newInstance();
      const payload = {
        'key3': utils.allocBufferFromString('val3')
      };
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
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
      const client = newInstance();
      const payload = {
        'key-prep1': utils.allocBufferFromString('val-prep1'),
        'key-prep2': utils.allocBufferFromString('val-prep2')
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
      const client = newInstance();
      const payload = {
        'key-batch1': utils.allocBufferFromString('val-batch1')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          const q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          const queries = [
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
      const client = newInstance();
      const payload = {
        'key-batch2': utils.allocBufferFromString('val-batch2')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          const q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          const queries = [
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
      const client = newInstance();
      const payload = {
        'key-batch-prep1': utils.allocBufferFromString('val-batch-prep1')
      };
      utils.series([
        client.connect.bind(client),
        function executeBatch(next) {
          const q = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          const queries = [
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
