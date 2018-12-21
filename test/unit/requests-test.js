'use strict';

const assert = require('assert');
const requests = require('../../lib/requests');
const Encoder = require('../../lib/encoder');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const ExecutionOptions = require('../../lib/execution-options').ExecutionOptions;
const packageInfo = require('../../package.json');
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
    const startupOptions = {
      cqlVersionKey: 'CQL_VERSION',
      cqlVersionValue: '3.0.0',
      driverNameKey: 'DRIVER_NAME',
      driverNameValue: packageInfo.description,
      driverVersionKey: 'DRIVER_VERSION',
      driverVersionValue: packageInfo.version,
      noCompactKey: 'NO_COMPACT',
      noCompactValue: 'true'
    };

    it('should include NO_COMPACT in options if true', function() {
      const request = new StartupRequest(null, true);
      const expectedBuffer = Buffer.concat([
        utils.allocBufferFromArray([
          types.protocolVersion.maxSupported, // protocol version
          0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
          0, 0, 0, 112, // length
          0, 4, // map size
        ]),
        getStringBuffer(startupOptions.cqlVersionKey),
        getStringBuffer(startupOptions.cqlVersionValue),
        getStringBuffer(startupOptions.driverNameKey),
        getStringBuffer(startupOptions.driverNameValue),
        getStringBuffer(startupOptions.driverVersionKey),
        getStringBuffer(startupOptions.driverVersionValue),
        getStringBuffer(startupOptions.noCompactKey),
        getStringBuffer(startupOptions.noCompactValue)
      ]);

      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });

    const expectedBufferWithNoCompact = Buffer.concat([
      utils.allocBufferFromArray([
        types.protocolVersion.maxSupported, // protocol version
        0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
        0, 0, 0, 94, // length
        0, 3, // map size
      ]),
      getStringBuffer(startupOptions.cqlVersionKey),
      getStringBuffer(startupOptions.cqlVersionValue),
      getStringBuffer(startupOptions.driverNameKey),
      getStringBuffer(startupOptions.driverNameValue),
      getStringBuffer(startupOptions.driverVersionKey),
      getStringBuffer(startupOptions.driverVersionValue)
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
  return new QueryRequest('Q1', [ 1, 2 ], ExecutionOptions.empty());
}

function getBatchRequest() {
  return new BatchRequest(
    [
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], ExecutionOptions.empty());
}

function getExecuteRequest() {
  const meta = { columns: [ { type: { code: types.dataTypes.int } }, { type: { code: types.dataTypes.int } } ]};
  return new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [ 1, 2], ExecutionOptions.empty(), meta);
}

function getStringBuffer(value) {
  const buffer = utils.allocBuffer(value.length + 2);
  buffer.writeUInt16BE(value.length, 0);
  buffer.write(value, 2);
  return buffer;
}