/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const assert = require('assert');
const requests = require('../../lib/requests');
const Encoder = require('../../lib/encoder');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const QueryRequest = requests.QueryRequest;
const ExecuteRequest = requests.ExecuteRequest;
const BatchRequest = requests.BatchRequest;
const StartupRequest = requests.StartupRequest;
const PrepareRequest = requests.PrepareRequest;
const encoder = new Encoder(types.protocolVersion.maxSupported, {});
const dseV1Encoder = new Encoder(types.protocolVersion.dseV1, {});

describe('QueryRequest', function () {
  describe('#clone()', function () {
    const request = new QueryRequest('Q1', [ 1, 2 ], { consistency: 1, hints: [] });
    testClone(request);
  });
  describe('#write()', function () {
    it('should include keyspace from options', function () {
      const request = new QueryRequest('Q1', [ ], { keyspace: 'ks1' } );
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.maxSupported,
        0, 0, 0, 0x7, // flags + stream id + opcode (0x7 = query)
        0, 0, 0, 0x11, // length
        0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
        0, 1, 0, 0, 0, 0x80, // consistency level + flags (0x80 = with keyspace)
        0, 3, 0x6b, 0x73, 0x31 // length = 3, 'ks1'
      ]);
      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });
    it('should exclude keyspace from options for older protocols', function () {
      const request = new QueryRequest('Q1', [ ], { keyspace: 'ks1' } );
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.dseV1,
        0, 0, 0, 0x7, // flags + stream id + opcode (0x7 = query)
        0, 0, 0, 0xc, // length
        0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
        0, 1, 0, 0, 0, 0, // consistency level + flags
      ]);
      assert.deepEqual(request.write(dseV1Encoder, 0), expectedBuffer);
    });
  });
});

describe('ExecuteRequest', function () {
  describe('#clone()', function () {
    const meta = { resultId: utils.allocBufferFromString('R1'), columns: [ { type: { code: types.dataTypes.int } }, { type: { code: types.dataTypes.int } } ]};
    const request = new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [ 1, 2], {}, meta);
    testClone(request);
  });
  describe('#write()', function() {
    it('should not include keyspace from options', function () {
      const meta = { resultId: utils.allocBufferFromString('R1'), columns: [ { } ] };
      const request = new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [], {keyspace: 'myks'}, meta);
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.maxSupported,
        0, 0, 0, 0xA, // flags + stream id + opcode (0xA = execute)
        0, 0, 0, 0xE, // length
        0, 2, 0x51, 0x31, // id length = 2 + id (Q1)
        0, 2, 0x52, 0x31, // result id length = 2 + id (Q1)
        0, 1, 0, 0, 0, 0, // consistency level + flags
      ]);
      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });
  });
});

describe('PrepareRequest', function () {
  describe('#write()', function () {
    it('should include keyspace from options', function () {
      const request = new PrepareRequest('Q1', 'ks1');
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.maxSupported,
        0, 0, 0, 0x9, // flags + stream id + opcode (0x9 = prepare)
        0, 0, 0, 0xf, // length
        0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
        0, 0, 0, 0x1, // flags (0x1 = with keyspace)
        0, 3, 0x6b, 0x73, 0x31 // length = 3, 'ks1'
      ]);
      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });
    it('should exclude keyspace from options for older protocols', function () {
      const request = new PrepareRequest('Q1', 'ks1');
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.dseV1,
        0, 0, 0, 0x9, // flags + stream id + opcode (0x9 = prepare)
        0, 0, 0, 0x6, // length
        0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
      ]);
      assert.deepEqual(request.write(dseV1Encoder, 0), expectedBuffer);
    });
  });
});

describe('BatchRequest', function () {
  describe('#clone()', function () {
    const request = new BatchRequest([
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], { logged: false, consistency: 1 });
    testClone(request);
  });
  it('should include keyspace from options', function () {
    const request = new BatchRequest([
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], { logged: false, consistency: 1, keyspace: 'ks1' });
    const expectedBuffer = utils.allocBufferFromArray([
      types.protocolVersion.maxSupported,
      0, 0, 0, 0xd, // flags + stream id + opcode (0x7 = batch)
      0, 0, 0, 0x20, // length
      1, 0, 2, // 1 = logged, 2 queries
      0, 0, 0, 0, 2, 0x51, 0x31, 0, 0, // simple query, length = 2, 'Q1', 0 values
      0, 0, 0, 0, 2, 0x51, 0x32, 0, 0, // simple query, length = 2, 'Q2', 0 values
      0, 1, 0, 0, 0, 0x80, // consistency level + flags (0x80 = with keyspace)
      0, 3, 0x6b, 0x73, 0x31 // length = 3, 'ks1'
    ]);
    assert.deepEqual(request.write(encoder, 0), expectedBuffer);
  });
  describe('#write()', function () {
    it('should exclude keyspace from options for older protocols', function () {
      const request = new BatchRequest([
        { query: 'Q1', params: [] },
        { query: 'Q2', params: [] }
      ], { logged: false, consistency: 1, keyspace: 'ks1' });
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.dseV1,
        0, 0, 0, 0xd, // flags + stream id + opcode (0x7 = batch)
        0, 0, 0, 0x1b, // length
        1, 0, 2, // 1 = logged, 2 queries
        0, 0, 0, 0, 2, 0x51, 0x31, 0, 0, // simple query, length = 2, 'Q1', 0 values
        0, 0, 0, 0, 2, 0x51, 0x32, 0, 0, // simple query, length = 2, 'Q2', 0 values
        0, 1, 0, 0, 0, 0, // consistency level + flags
      ]);
      assert.deepEqual(request.write(dseV1Encoder, 0), expectedBuffer);
    });
  });
});

describe('Startup', function() {
  describe('#write()', function () {
    it('should include NO_COMPACT in options if true', function() {
      const request = new StartupRequest(null, true);
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.maxSupported, // protocol version
        0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
        0, 0, 0, 40, // length
        0, 2, // map size = 2 (CQL_VERSION and NO_COMPACT)
        0, 11, // key length (CQL_VERSION = 11)
        0x43, 0x51, 0x4c, 0x5f, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e, // CQL_VERSION
        0, 5, // value length (3.0.0 = 5)
        0x33, 0x2e, 0x30, 0x2e, 0x30, // 3.0.0,
        0, 10, // key length (NO_COMPACT)
        0x4e, 0x4f, 0x5f, 0x43, 0x4f, 0x4d, 0x50, 0x41, 0x43, 0x54, // NO_COMPACT
        0, 4, // value length (true)
        0x74, 0x72, 0x75, 0x65 // true
      ]);
      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });

    const expectedBufferWithNoCompact = utils.allocBufferFromArray([
      types.protocolVersion.maxSupported, // protocol version
      0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
      0, 0, 0, 22, // length
      0, 1, // map size = 2 (CQL_VERSION and NO_COMPACT)
      0, 11, // key length (CQL_VERSION = 11)
      0x43, 0x51, 0x4c, 0x5f, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e, // CQL_VERSION
      0, 5, // value length (3.0.0 = 5)
      0x33, 0x2e, 0x30, 0x2e, 0x30, // 3.0.0,
    ]);

    it('should not include NO_COMPACT in options if false', function() {
      const request = new StartupRequest(null, false);
      assert.deepEqual(request.write(encoder, 0), expectedBufferWithNoCompact);
    });
    it('should not include NO_COMPACT in options if not provided', function() {
      const request = new StartupRequest();
      assert.deepEqual(request.write(encoder, 0), expectedBufferWithNoCompact);
    });
  });
});

describe('options', () => {
  describe('#write()', () => {
    it('should have empty body', () => {
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.maxSupported, // protocol version
        0, 0, 0, 5, // flags + stream id + opcode (5 = options)
        0, 0, 0, 0 // body length 0
      ]);
      assert.deepEqual(requests.options.write(encoder, 0), expectedBuffer);
    });
  });
  describe('#clone()', () => {
    it('should return the same instance', () => {
      assert.strictEqual(requests.options.clone(), requests.options);
    });
  });
});

function testClone(request) {
  it('should return a new instance with the same properties', function () {
    const cloned = request.clone();
    assert.notStrictEqual(request, cloned);
    Object.keys(request).forEach(function (key) {
      assert.strictEqual(request[key], cloned[key]);
    });
  });
  it('should generate the same buffer', function () {
    const cloned = request.clone();
    assert.strictEqual(
      request.write(encoder, 1).toString(),
      cloned.write(encoder, 1).toString()
    );
  });
}
