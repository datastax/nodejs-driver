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

const assert = require('assert');
const requests = require('../../lib/requests');
const Encoder = require('../../lib/encoder');
const types = require('../../lib/types');
const utils = require('../../lib/utils');
const DefaultExecutionOptions = require('../../lib/execution-options').DefaultExecutionOptions;
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const QueryRequest = requests.QueryRequest;
const ExecuteRequest = requests.ExecuteRequest;
const BatchRequest = requests.BatchRequest;
const defaultOptions = require('../../lib/client-options').defaultOptions;

const encoder = new Encoder(types.protocolVersion.maxSupported, {});

describe('ExecuteRequest', function () {
  describe('#write()', function() {
    const queryOptions = { fetchSize: 0 };
    testGenerateOnce(queryOptions, getExecuteRequest, getExecuteRequestExpectedBuffer);
    testGenerate(queryOptions, getExecuteRequest, getExecuteRequestExpectedBuffer);
  });
});

describe('QueryRequest', function () {
  describe('#write()', function () {
    const queryOptions = { fetchSize: 0 };
    testGenerateOnce(queryOptions, getQueryRequest, getQueryRequestExpectedBuffer);
    testGenerate(queryOptions, getQueryRequest, getQueryRequestExpectedBuffer);
  });
});

describe('BatchRequest', function () {
  describe('#write()', function () {
    const queryOptions = { logged: false, consistency: 1 };
    testGenerateOnce(queryOptions, getBatchRequest, getBatchRequestExpectedBuffer);
    testGenerate(queryOptions, getBatchRequest, getBatchRequestExpectedBuffer);
  });
});

function testGenerateOnce(queryOptions, requestGetter, bufferGetter) {
  it('should generate the timestamp once', function () {
    assert.strictEqual(queryOptions.timestamp, undefined);
    const client = getClientFake();
    const request = requestGetter(client, queryOptions);

    let called = 0;
    const write = () => {
      called++;
      return request.write(encoder, 0);
    };

    const nbCalls = 4;
    for (let i = 0; i < nbCalls; i++) {
      assert.deepEqual(write(), bufferGetter(0));
    }
    assert.strictEqual(called, nbCalls);
  });
}

function testGenerate(queryOptions, requestGetter, bufferGetter) {
  it('should generate the timestamp', function () {
    assert.strictEqual(queryOptions.timestamp, undefined);
    const client = getClientFake();

    let called = 0;
    const write = (request) => {
      called++;
      return request.write(encoder, 0);
    };

    const nbCalls = 4;
    for (let i = 0; i < nbCalls; i++) {
      const request = requestGetter(client, queryOptions);
      assert.deepEqual(write(request), bufferGetter(i));
    }
    assert.strictEqual(called, nbCalls);
  });
}

function getExecuteRequestExpectedBuffer(timestamp) {
  return Buffer.concat([
    utils.allocBufferFromArray([
      types.protocolVersion.maxSupported,
      0, 0, 0, 0xA, // flags + stream id + opcode (0xA = execute)
      0, 0, 0, 0x16, // length
      0, 2, 0x51, 0x31, // id length = 2 + id (Q1)
      0, 2, 0x52, 0x31, // result id length = 2 + id (Q1)
      0, 1, 0, 0, 0, 0x20, // consistency level + flags (0x20 = timestamp)
    ]),
    longBuffer(timestamp)
  ]);
}

function getQueryRequestExpectedBuffer(timestamp) {
  return Buffer.concat([
    utils.allocBufferFromArray([
      types.protocolVersion.maxSupported,
      0, 0, 0, 0x7, // flags + stream id + opcode (0x7 = query)
      0, 0, 0, 0x14, // length
      0, 0, 0, 2, 0x51, 0x31, // query, length = 2, 'Q1'
      0, 1, 0, 0, 0, 0x20, // consistency level + flags (0x20 = timestamp)
    ]),
    longBuffer(timestamp)
  ]);
}

function getBatchRequestExpectedBuffer(timestamp) {
  return Buffer.concat([
    utils.allocBufferFromArray([
      types.protocolVersion.maxSupported,
      0, 0, 0, 0xD, // flags + stream id + opcode (0xD = batch)
      0, 0, 0, 0x23, // length
      1, 0, 2, // 1 = unlogged, 2 queries
      0, 0, 0, 0, 2, 0x51, 0x31, 0, 0, // simple query, length = 2, 'Q1', 0 values
      0, 0, 0, 0, 2, 0x51, 0x32, 0, 0, // simple query, length = 2, 'Q2', 0 values
      0, 1, 0, 0, 0, 0x20, // consistency level + flags (0x20 = timestamp)
    ]),
    longBuffer(timestamp)
  ]);
}

function longBuffer(value) {
  value = types.Long.fromNumber(value);
  return types.Long.toBuffer(value);
}

function getExecuteRequest(client, options) {
  const execOptions = DefaultExecutionOptions.create(options, client);
  const meta = { resultId: utils.allocBufferFromString('R1'), columns: [ { } ] };
  return new ExecuteRequest('Q1', utils.allocBufferFromString('Q1'), [], execOptions, meta);
}

function getQueryRequest(client, options) {
  const execOptions = DefaultExecutionOptions.create(options, client);
  return new QueryRequest('Q1', [], execOptions);
}

function getBatchRequest(client, options) {
  const execOptions = DefaultExecutionOptions.create(options, client);
  return new BatchRequest(
    [
      { query: 'Q1', params: [] },
      { query: 'Q2', params: [] }
    ], execOptions);
}

function getClientFake() {
  const clientOptions = defaultOptions();
  let timestamp = 0;
  clientOptions.policies.timestampGeneration.next = () => timestamp++;
  return {
    profileManager: { getProfile: x => new ExecutionProfile(x || 'default') },
    options: clientOptions,
    controlConnection: { protocolVersion: types.protocolVersion.maxSupported }
  };
}
