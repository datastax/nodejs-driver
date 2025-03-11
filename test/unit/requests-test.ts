/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
import assert from "assert";
import requests from "../../lib/requests";
import Encoder from "../../lib/encoder";
import types from "../../lib/types/index";
import utils from "../../lib/utils";
import packageInfo from "../../package.json";
import { ExecutionOptions } from "../../lib/execution-options";

const QueryRequest = requests.QueryRequest;
const ExecuteRequest = requests.ExecuteRequest;
const BatchRequest = requests.BatchRequest;
const StartupRequest = requests.StartupRequest;
const PrepareRequest = requests.PrepareRequest;

const encoder = new Encoder(types.protocolVersion.maxSupported, {});
const dseV1Encoder = new Encoder(types.protocolVersion.dseV1, {});

describe('QueryRequest', function () {
  describe('#clone()', function () {
    const request = getQueryRequest();
    testClone(request);
  });
  describe('#write()', function () {
    it('should include keyspace from options', function () {
      const request = getQueryRequest({ keyspace: 'ks1' }, []);
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
      const request = getQueryRequest({ keyspace: 'ks1' }, []);
      const expectedBuffer = utils.allocBufferFromArray([
        types.protocolVersion.dseV1,
        0, 0, 0, 0x7, // flags + stream id + opcode (0x7 = query)
        0, 0, 0, 0xc, // length
        0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
        0, 1, 0, 0, 0, 0, // consistency level + flags
      ]);
      assert.deepEqual(request.write(dseV1Encoder, 0), expectedBuffer);
    });

    testRequestLength(getQueryRequest);
  });
});

describe('ExecuteRequest', function () {
  describe('#clone()', function () {
    const request = getExecuteRequest();
    testClone(request);
  });
  describe('#write()', function() {
    it('should not include keyspace from options', function () {
      const meta = { resultId: utils.allocBufferFromString('R1'), columns: [ { } ] };
      const request = getExecuteRequest({keyspace: 'myks'}, meta, []);
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

    testRequestLength(getExecuteRequest);
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

  describe('#write()', function () {
    testRequestLength(getExecuteRequest);
  });
});

describe('BatchRequest', function () {

  describe('#clone()', function () {
    const request = getBatchRequest();
    testClone(request);
  });
  it('should include keyspace from options', function () {
    const request = getBatchRequest({ logged: false, consistency: 1, keyspace: 'ks1' });
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
      const request = getBatchRequest({ logged: false, consistency: 1, keyspace: 'ks1' });
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

    testRequestLength(getBatchRequest);
  });
});

describe('Startup', function() {
  const clientId = types.Uuid.random();
  const applicationName = 'My App';
  const applicationVersion = '3.20.1';

  describe('#write()', function () {
    const startupOptions = {
      cqlVersionKey: 'CQL_VERSION',
      cqlVersionValue: '3.0.0',
      driverNameKey: 'DRIVER_NAME',
      driverNameValue: packageInfo.description,
      driverVersionKey: 'DRIVER_VERSION',
      driverVersionValue: packageInfo.version,
      noCompactKey: 'NO_COMPACT',
      noCompactValue: 'true',
      clientIdKey: 'CLIENT_ID',
      clientIdValue: clientId.toString(),
      applicationNameKey: 'APPLICATION_NAME',
      applicationNameValue: applicationName,
      applicationVersionKey: 'APPLICATION_VERSION',
      applicationVersionValue: applicationVersion
    };

    const driverNameAndVersionBuffer = Buffer.concat([
      getStringBuffer(startupOptions.driverNameKey),
      getStringBuffer(startupOptions.driverNameValue),
      getStringBuffer(startupOptions.driverVersionKey),
      getStringBuffer(startupOptions.driverVersionValue)]);

    const cqlVersionLength = 22;

    it('should include NO_COMPACT in options if true', function() {
      const request = new StartupRequest({ noCompact: true });
      const expectedBuffer = Buffer.concat([
        utils.allocBufferFromArray([
          types.protocolVersion.maxSupported, // protocol version
          0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
          0, 0, 0, 40 + driverNameAndVersionBuffer.length, // length
          0, 4, // map size
        ]),
        getStringBuffer(startupOptions.cqlVersionKey),
        getStringBuffer(startupOptions.cqlVersionValue),
        driverNameAndVersionBuffer,
        getStringBuffer(startupOptions.noCompactKey),
        getStringBuffer(startupOptions.noCompactValue)
      ]);

      assert.deepEqual(request.write(encoder, 0), expectedBuffer);
    });

    const expectedBufferWithNoCompact = Buffer.concat([
      utils.allocBufferFromArray([
        types.protocolVersion.maxSupported, // protocol version
        0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
        0, 0, 0, cqlVersionLength + driverNameAndVersionBuffer.length, // length
        0, 3, // map size
      ]),
      getStringBuffer(startupOptions.cqlVersionKey),
      getStringBuffer(startupOptions.cqlVersionValue),
      driverNameAndVersionBuffer
    ]);

    it('should not include NO_COMPACT in options if false', function() {
      const request = new StartupRequest({ noCompact: false });
      assert.deepEqual(request.write(encoder, 0), expectedBufferWithNoCompact);
    });
    it('should not include NO_COMPACT in options if not provided', function() {
      const request = new StartupRequest();
      assert.deepEqual(request.write(encoder, 0), expectedBufferWithNoCompact);
    });

    it('should include client id', () => {
      const clientKeyBuffer = getStringBuffer(startupOptions.clientIdKey);
      const clientValueBuffer = getStringBuffer(startupOptions.clientIdValue);

      const expected = Buffer.concat([
        utils.allocBufferFromArray([
          types.protocolVersion.maxSupported, // protocol version
          0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
          0, 0, 0, cqlVersionLength + driverNameAndVersionBuffer.length + clientKeyBuffer.length + clientValueBuffer.length, // length
          0, 4, // map size
        ]),
        getStringBuffer(startupOptions.cqlVersionKey),
        getStringBuffer(startupOptions.cqlVersionValue),
        driverNameAndVersionBuffer,
        clientKeyBuffer,
        clientValueBuffer,
      ]);

      // ID as a UUID instance
      assert.deepEqual(new StartupRequest({ clientId }).write(encoder, 0), expected);

      // ID as a string
      assert.deepEqual(new StartupRequest({ clientId: clientId.toString() }).write(encoder, 0), expected);
    });

    it('should include application name and version', () => {

      const clientAndApplicationBuffer = Buffer.concat([
        getStringBuffer(startupOptions.clientIdKey),
        getStringBuffer(startupOptions.clientIdValue),
        getStringBuffer(startupOptions.applicationNameKey),
        getStringBuffer(startupOptions.applicationNameValue),
        getStringBuffer(startupOptions.applicationVersionKey),
        getStringBuffer(startupOptions.applicationVersionValue)]);

      const expected = Buffer.concat([
        utils.allocBufferFromArray([
          types.protocolVersion.maxSupported, // protocol version
          0, 0, 0, 1, // flags + stream id + opcode (1 = startup)
          0, 0, 0, cqlVersionLength + driverNameAndVersionBuffer.length + clientAndApplicationBuffer.length, // length
          0, 6, // map size
        ]),
        getStringBuffer(startupOptions.cqlVersionKey),
        getStringBuffer(startupOptions.cqlVersionValue),
        driverNameAndVersionBuffer,
        clientAndApplicationBuffer
      ]);

      const request = new StartupRequest({ clientId, applicationName, applicationVersion });
      assert.deepEqual(request.write(encoder, 0), expected);
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

function getQueryRequest(options, params) {
  options = options || {};
  const execOptions = ExecutionOptions.empty();
  execOptions.getKeyspace = () => options.keyspace;

  return new QueryRequest('Q1', params || [ 1, 2 ], execOptions);
}

function getBatchRequest(options) {
  options = options || {};
  const execOptions = ExecutionOptions.empty();
  execOptions.getKeyspace = () => options.keyspace;
  execOptions.isBatchLogged = () => options.logged;
  execOptions.getConsistency = () => options.consistency;

  return new BatchRequest(
    [
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], execOptions);
}

function getExecuteRequest(options, meta, params) {
  meta = meta || {
    resultId: utils.allocBufferFromString('R1'),
    columns: [ { type: { code: types.dataTypes.int } }, { type: { code: types.dataTypes.int } } ]
  };

  options = options || {};
  const execOptions = ExecutionOptions.empty();
  execOptions.getKeyspace = () => options && options.keyspace;

  return new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), params || [ 1, 2], execOptions, meta);
}

function getStringBuffer(value) {
  const buffer = utils.allocBuffer(value.length + 2);
  buffer.writeUInt16BE(value.length, 0);
  buffer.write(value, 2);
  return buffer;
}