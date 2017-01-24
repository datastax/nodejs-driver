/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');
var vit = helper.vit;

describe('Client', function () {
  this.timeout(120000);
  vit('2.2', 'should callback with readFailure error when tombstone overwhelmed on replica', function (done) {
    var client = newInstance({
      contactPoints: ['127.0.0.2'],
      policies: { loadBalancing: new helper.WhiteListPolicy(['2'])}
    });
    utils.series([
      helper.ccmHelper.start(2, { yaml: ['tombstone_failure_threshold:1000']}),
      client.connect.bind(client),
      helper.toTask(client.execute, client, "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"),
      helper.toTask(client.execute, client, "CREATE TABLE test.foo(pk int, cc int, v int, primary key (pk, cc))"),
      function generateTombstones(next) {
        // The rest of the test relies on the fact that the PK '1' will be placed on node1 with MurmurPartitioner
        utils.timesSeries(2000, function (n, timesNext) {
          client.execute('INSERT INTO test.foo (pk, cc, v) VALUES (1, ?, null)', [n], {prepare: true}, function (err, result) {
            if (err) {
              return next(err);
            }
            assert.strictEqual(helper.lastOctetOf(result.info.queriedHost), '2');
            timesNext();
          });
        }, next);
      },
      function (next) {
        client.execute('SELECT * FROM test.foo WHERE pk = 1', function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          assert.strictEqual(err.code, types.responseErrorCodes.readFailure);
          assert.strictEqual(err.failures, 1);
          assert.strictEqual(err.received, 0);
          next();
        });
      },
      client.shutdown.bind(client),
      helper.ccmHelper.remove
    ], done);
  });
  vit('2.2', 'should callback with writeFailure error when encountered', function (done) {
    var client = newInstance();
    var keyspace = 'ks_wfail';
    var table = keyspace + '.tbl1';
    utils.series([
      helper.ccmHelper.removeIfAny,
      helper.toTask(helper.ccmHelper.exec, null, ['create', 'test', '-v', helper.getDseVersion()]),
      helper.toTask(helper.ccmHelper.exec, null, ['populate', '-n', 2]),
      helper.toTask(helper.ccmHelper.exec, null, ['node1', 'start', '--wait-for-binary-proto', '--jvm_arg=-Dcassandra.test.fail_writes_ks=' + keyspace]),
      helper.toTask(helper.ccmHelper.exec, null, ['node2', 'start', '--wait-for-binary-proto']),
      client.connect.bind(client),
      helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 2, true)),
      helper.toTask(client.execute, client, helper.createTableCql(table)),
      function (next) {
        var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
        client.execute(query, [types.Uuid.random(), '1'], { consistency: types.consistencies.all}, function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          assert.strictEqual(err.code, types.responseErrorCodes.writeFailure);
          assert.strictEqual(err.failures, 1);
          assert.strictEqual(err.writeType, 'SIMPLE');
          next();
        });
      },
      client.shutdown.bind(client),
      helper.ccmHelper.remove
    ], done);
  });
  vit('2.2', 'should callback with functionFailure error when the cql function throws an error', function (done) {
    var client = newInstance({});
    utils.series([
      helper.ccmHelper.start(1, { yaml: ['enable_user_defined_functions:true']}),
      client.connect.bind(client),
      helper.toTask(client.execute, client, helper.createKeyspaceCql('ks_func')),
      helper.toTask(client.execute, client, 'CREATE TABLE ks_func.tbl1 (id int PRIMARY KEY, v1 int, v2 int)'),
      helper.toTask(client.execute, client, 'INSERT INTO ks_func.tbl1 (id, v1, v2) VALUES (1, 1, 0)'),
      helper.toTask(client.execute, client, "CREATE FUNCTION ks_func.div(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return a / b;'"),
      function (next) {
        client.execute('SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1', function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          assert.strictEqual(err.code, types.responseErrorCodes.functionFailure);
          assert.strictEqual(err.keyspace, 'ks_func');
          assert.strictEqual(err.functionName, 'div');
          next();
        });
      },
      client.shutdown.bind(client),
      helper.ccmHelper.remove
    ], done);
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}
