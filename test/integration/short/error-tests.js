'use strict';
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils');
const errors = require('../../../lib/errors');
const protocolVersion = types.protocolVersion;
const vdescribe = helper.vdescribe;

describe('Client', function () {
  this.timeout(120000);
  vdescribe('2.2', 'with protocol v4 errors', function () {
    const failWritesKs = helper.getRandomName('ks');
    const failWritesTable = failWritesKs + '.tbl1';
    const setupInfo = helper.setup(2, {
      clientOptions: {
        policies: { retry: new helper.FallthroughRetryPolicy() }
      },
      queries: [
        'CREATE TABLE read_fail_tbl(pk int, cc int, v int, primary key (pk, cc))',
        helper.createKeyspaceCql(failWritesKs, 2, true),
        helper.createTableCql(failWritesTable),
        helper.createKeyspaceCql('ks_func'),
        'CREATE TABLE ks_func.tbl1 (id int PRIMARY KEY, v1 int, v2 int)',
        'INSERT INTO ks_func.tbl1 (id, v1, v2) VALUES (1, 1, 0)',
        'CREATE FUNCTION ks_func.div(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS' +
          ' \'return a / b;\''
      ],
      ccmOptions: {
        yaml: ['tombstone_failure_threshold:1000', 'enable_user_defined_functions:true'],
        jvmArgs: ['-Dcassandra.test.fail_writes_ks=' + failWritesKs]
      }
    });
    it('should callback with readFailure error when tombstone overwhelmed on replica', function (done) {
      const client = setupInfo.client;
      utils.series([
        function generateTombstones(next) {
          utils.timesSeries(2000, function (n, timesNext) {
            const query = 'INSERT INTO read_fail_tbl (pk, cc, v) VALUES (?, ?, ?)';
            client.execute(query, [ 1, n, null ], { prepare: true }, timesNext);
          }, next);
        },
        function (next) {
          client.execute('SELECT * FROM read_fail_tbl WHERE pk = 1', function (err) {
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(err.code, types.responseErrorCodes.readFailure);
            if (client.controlConnection.protocolVersion >= protocolVersion.v5) {
              assert.strictEqual(typeof err.reasons, 'object');
              Object.keys(err.reasons).forEach(function (key) {
                assert.strictEqual(typeof err.reasons[key], 'number');
              });
            }
            else {
              assert.strictEqual(err.failures, 1);
            }
            assert.strictEqual(err.received, 0);
            next();
          });
        }], done);
    });
    it('should callback with writeFailure error when encountered', function (done) {
      const client = setupInfo.client;
      const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', failWritesTable);
      client.execute(query, [ types.Uuid.random(), '1' ], { consistency: types.consistencies.all }, function (err) {
        helper.assertInstanceOf(err, errors.ResponseError);
        assert.strictEqual(err.code, types.responseErrorCodes.writeFailure);
        if (client.controlConnection.protocolVersion >= protocolVersion.v5) {
          assert.strictEqual(typeof err.reasons, 'object');
          Object.keys(err.reasons).forEach(function (key) {
            assert.strictEqual(typeof err.reasons[key], 'number');
          });
        }
        else {
          assert.strictEqual(typeof err.failures, 'number');
          assert.ok(err.failures > 0);
        }
        assert.strictEqual(err.writeType, 'SIMPLE');
        done();
      });
    });
    it('should callback with functionFailure error when the cql function throws an error', function (done) {
      const client = setupInfo.client;
      client.execute('SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1', function (err) {
        helper.assertInstanceOf(err, errors.ResponseError);
        assert.strictEqual(err.code, types.responseErrorCodes.functionFailure);
        assert.strictEqual(err.keyspace, 'ks_func');
        assert.strictEqual(err.functionName, 'div');
        done();
      });
    });
  });
});