'use strict';
const assert = require('assert');
const errors = require('../../../lib/errors');
const types = require('../../../lib/types');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');

const Client = require('../../../lib/client.js');
const OrderedLoadBalancingPolicy = require('../../test-helper').OrderedLoadBalancingPolicy;

const query = "select * from data";

describe('Client', function() {
  this.timeout(10000);
  const setupInfo = simulacron.setup([3], { initClient: false });
  const cluster = setupInfo.cluster;

  const clientOptions = {
    contactPoints: [simulacron.startingIp],
    policies: { 
      loadBalancing: new OrderedLoadBalancingPolicy()
    }
  };

  const client = new Client(clientOptions);
  before(client.connect.bind(client));
  after(client.shutdown.bind(client));

  function errorResultTest(primeResult, assertFn) {
    return (done) => {
      utils.series([
        primeWithResult(cluster, primeResult),
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            try {
              assertFn(err, result);
              next();
            } catch(e) {
              next(e);
            }
          });
        },
      ], done);
    };
  }

  it ('should error with unavailable', errorResultTest({
    result: 'unavailable',
    message: 'unavailable', // issue #39 of simulacron requires message.
    alive: 4,
    required: 5,
    consistency_level: 'LOCAL_QUORUM'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.unavailableException);
    assert.strictEqual(err.consistencies, types.consistencies.localQuorum);
    assert.strictEqual(err.alive, 4);
    assert.strictEqual(err.required, 5);
    assert.strictEqual(err.message, 'Not enough replicas available for query at consistency LOCAL_QUORUM (5 required but only 4 alive)');
  }));
  it ('should error with readTimeout with not enough received', errorResultTest({
    result: 'read_timeout',
    received: 1,
    block_for: 2,
    consistency_level: 'TWO',
    data_present: false
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.readTimeout);
    assert.strictEqual(err.consistencies, types.consistencies.two);
    assert.strictEqual(err.received, 1);
    assert.strictEqual(err.blockFor, 2);
    assert.strictEqual(err.isDataPresent, 0);
    assert.strictEqual(err.message, 'Server timeout during read query at consistency TWO (1 replica(s) responded over 2 required)');
  }));
  it ('should error with readTimeout with no data present', errorResultTest({
    result: 'read_timeout',
    received: 2,
    block_for: 2,
    consistency_level: 'TWO',
    data_present: false
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.readTimeout);
    assert.strictEqual(err.consistencies, types.consistencies.two);
    assert.strictEqual(err.received, 2);
    assert.strictEqual(err.blockFor, 2);
    assert.strictEqual(err.isDataPresent, 0);
    assert.strictEqual(err.message, 'Server timeout during read query at consistency TWO (the replica queried for the data didn\'t respond)');
  }));
  it ('should error with readTimeout with repair timeout', errorResultTest({
    result: 'read_timeout',
    received: 2,
    block_for: 2,
    consistency_level: 'TWO',
    data_present: true
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.readTimeout);
    assert.strictEqual(err.consistencies, types.consistencies.two);
    assert.strictEqual(err.received, 2);
    assert.strictEqual(err.blockFor, 2);
    assert.strictEqual(err.isDataPresent, 1);
    assert.strictEqual(err.message, 'Server timeout during read query at consistency TWO (timeout while waiting for repair of inconsistent replica)');
  }));
  it ('should error with readFailure', errorResultTest({
    result: 'read_failure',
    received: 3,
    block_for: 5,
    consistency_level: 'EACH_QUORUM',
    data_present: true,
    failure_reasons: {
      '127.0.0.1': 'READ_TOO_MANY_TOMBSTONES',
      '127.0.0.2': 'UNKNOWN'
    },
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.readFailure);
    assert.strictEqual(err.consistencies, types.consistencies.eachQuorum);
    assert.strictEqual(err.received, 3);
    assert.strictEqual(err.failures, 2);
    assert.strictEqual(err.blockFor, 5);
    assert.strictEqual(err.isDataPresent, 1);
    assert.strictEqual(err.message, 'Server failure during read query at consistency EACH_QUORUM (5 responses were required but only 3 replicas responded, 2 failed)');
  }));
  it ('should error with SIMPLE writeTimeout', errorResultTest({
    result: 'write_timeout',
    received: 1,
    block_for: 3,
    consistency_level: 'QUORUM',
    write_type: 'SIMPLE'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.writeTimeout);
    assert.strictEqual(err.consistencies, types.consistencies.quorum);
    assert.strictEqual(err.received, 1);
    assert.strictEqual(err.blockFor, 3);
    assert.strictEqual(err.writeType, 'SIMPLE');
    assert.strictEqual(err.message, 'Server timeout during write query at consistency QUORUM (1 peer(s) acknowledged the write over 3 required)');
  }));
  it ('should error with BATCH_LOG writeTimeout', errorResultTest({
    result: 'write_timeout',
    received: 0,
    block_for: 1,
    consistency_level: 'ONE',
    write_type: 'BATCH_LOG'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.writeTimeout);
    assert.strictEqual(err.consistencies, types.consistencies.one);
    assert.strictEqual(err.received, 0);
    assert.strictEqual(err.blockFor, 1);
    assert.strictEqual(err.writeType, 'BATCH_LOG');
    assert.strictEqual(err.message, 'Server timeout during batchlog write at consistency ONE (0 peer(s) acknowledged the write over 1 required)');
  }));
  it ('should error with writeFailure', errorResultTest({
    result: 'write_failure',
    received: 2,
    block_for: 3,
    failure_reasons: {
      '127.0.0.1': 'UNKNOWN'
    },
    consistency_level: 'THREE',
    write_type: 'COUNTER'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.writeFailure);
    assert.strictEqual(err.consistencies, types.consistencies.three);
    assert.strictEqual(err.received, 2);
    assert.strictEqual(err.blockFor, 3);
    assert.strictEqual(err.failures, 1);
    assert.strictEqual(err.writeType, 'COUNTER');
    assert.strictEqual(err.message, 'Server failure during write query at consistency THREE (3 responses were required but only 2 replicas responded, 1 failed)');
  }));
  it ('should error with functionFailure', errorResultTest({
    result: 'function_failure',
    keyspace: 'myks',
    function: 'foo',
    arg_types: ['int', 'varchar', 'blob'],
    detail: 'Could not execute function'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.functionFailure);
    assert.strictEqual(err.keyspace, 'myks');
    assert.strictEqual(err.functionName, 'foo');
    assert.deepEqual(err.argTypes, ['int', 'varchar', 'blob']);
    assert.ok(err.message.indexOf('Could not execute function') !== -1);
  }));
  it ('should error with alreadyExists (Table)', errorResultTest({
    result: 'already_exists',
    message: 'The table already exists!',
    keyspace: 'myks',
    table: 'myTbl'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.alreadyExists);
    assert.strictEqual(err.keyspace, 'myks');
    assert.strictEqual(err.table, 'myTbl');
    assert.strictEqual(err.message, 'The table already exists!');
  }));
  it ('should error with alreadyExists (Keyspace)', errorResultTest({
    result: 'already_exists',
    message: 'The keyspace already exists!',
    keyspace: 'myks',
    table: ''
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.alreadyExists);
    assert.strictEqual(err.keyspace, 'myks');
    assert.ifError(err.table); // table should not be set.
    assert.strictEqual(err.message, 'The keyspace already exists!');
  }));
  it ('should error with configError', errorResultTest({
    result: 'config_error',
    message: 'Invalid Configuration!'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.configError);
    assert.strictEqual(err.message, 'Invalid Configuration!');
  }));
  it ('should error with invalid', errorResultTest({
    result: 'invalid',
    message: 'Invalid Query!'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.invalid);
    assert.strictEqual(err.message, 'Invalid Query!');
  }));
  it ('should error with protocolError', errorResultTest({
    result: 'protocol_error',
    message: 'Protocol Error!'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.protocolError);
    assert.strictEqual(err.message, 'Protocol Error!');
  }));
  it ('should error with serverError', errorResultTest({
    result: 'server_error',
    message: 'Server Error!',
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.serverError);
    assert.strictEqual(err.message, 'Server Error!');
  }));
  it ('should error with syntaxError', errorResultTest({
    result: 'syntax_error',
    message: 'Invalid Syntax!',
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
    assert.strictEqual(err.message, 'Invalid Syntax!');
  }));
  it ('should error with unauthorized', errorResultTest({
    result: 'unauthorized',
    message: 'Unauthorized!',
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.ResponseError);
    assert.strictEqual(err.code, types.responseErrorCodes.unauthorized);
    assert.strictEqual(err.message, 'Unauthorized!');
  }));
  // errors that are retried on next host.
  it ('should error with isBootstrapping', errorResultTest({
    result: 'is_bootstrapping',
    message: 'Bootstrapping!'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.NoHostAvailableError);
    assert.strictEqual(Object.keys(err.innerErrors).length, 3);
    Object.keys(err.innerErrors).forEach(key => {
      const e = err.innerErrors[key];
      helper.assertInstanceOf(e, errors.ResponseError);
      assert.strictEqual(e.code, types.responseErrorCodes.isBootstrapping);
      assert.strictEqual(e.message, 'Bootstrapping!');
    });
  }));
  it ('should error with overloaded', errorResultTest({
    result: 'overloaded',
    message: 'Overloaded!'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.NoHostAvailableError);
    assert.strictEqual(Object.keys(err.innerErrors).length, 3);
    Object.keys(err.innerErrors).forEach(key => {
      const e = err.innerErrors[key];
      helper.assertInstanceOf(e, errors.ResponseError);
      assert.strictEqual(e.code, types.responseErrorCodes.overloaded);
      assert.strictEqual(e.message, 'Overloaded!');
    });
  }));
  it ('should error with truncateError', errorResultTest({
    result: 'truncate_error',
    message: 'Timeout while truncating table'
  }, (err, result) => {
    assert.ok(err);
    helper.assertInstanceOf(err, errors.NoHostAvailableError);
    assert.strictEqual(Object.keys(err.innerErrors).length, 3);
    Object.keys(err.innerErrors).forEach(key => {
      const e = err.innerErrors[key];
      helper.assertInstanceOf(e, errors.ResponseError);
      assert.strictEqual(e.code, types.responseErrorCodes.truncateError);
      assert.strictEqual(e.message, 'Timeout while truncating table');
    });
  }));
});

function primeWithResult(topic, then) {
  return helper.toTask(topic.prime, topic, {
    when: {
      query: query
    },
    then: then
  });
}