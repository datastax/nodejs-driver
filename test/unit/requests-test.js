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
const encoder = new Encoder(types.protocolVersion.maxSupported, {});

describe('QueryRequest', function () {
  describe('#clone()', function () {
    const request = getQueryRequest();
    testClone(request);
  });

  describe('#write()', function () {
    testRequestLength(getQueryRequest);
  });
});

describe('ExecuteRequest', function () {
  describe('#clone()', function () {
    const request = getExecuteRequest();
    testClone(request);
  });

  describe('#write()', function () {
    testRequestLength(getExecuteRequest);
  });
});

describe('BatchRequest', function () {

  describe('#clone()', function () {
    const request = getBatchRequest();
    testClone(request);
  });

  describe('#write()', function () {
    testRequestLength(getBatchRequest);
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

function testRequestLength(requestGetter) {
  it('should set the length of the body of the request', () => {
    const request = requestGetter();

    assert.strictEqual(request.length, 0);
    request.write(encoder, 0);
    assert.ok(request.length > 0);
  });
}

function getQueryRequest() {
  return new QueryRequest('Q1', [ 1, 2 ], { consistency: 1, hints: [] });
}

function getBatchRequest() {
  return new BatchRequest(
    [
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], { logged: false, consistency: 1 });
}

function getExecuteRequest() {
  const meta = { columns: [ { type: { code: types.dataTypes.int } }, { type: { code: types.dataTypes.int } } ]};
  return new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [ 1, 2], {}, meta);
}