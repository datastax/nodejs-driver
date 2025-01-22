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
const utils = require('../../lib/utils');
const types = require('../../lib/types');
const helper = require('../test-helper');
const DefaultExecutionOptions = require('../../lib/execution-options').DefaultExecutionOptions;
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const defaultOptions = require('../../lib/client-options').defaultOptions;

describe('DefaultExecutionOptions', () => {
  describe('create()', () => {
    it('should get the values from the query options', () => {
      const options = {
        autoPage: true,
        captureStackTrace: true,
        consistency: 2,
        counter: true,
        customPayload: {},
        executionProfile: 'oltp',
        fetchSize: 30,
        hints: [ 'int' ],
        isIdempotent: true,
        keyspace: 'ks2',
        logged: true,
        pageState: utils.allocBufferFromArray([ 1, 2, 3, 4 ]),
        prepare: true,
        readTimeout: 123,
        retry: {},
        routingNames: [ 'a' ],
        routingIndexes: [ 1, 2 ],
        routingKey: utils.allocBufferFromArray([ 0, 1 ]),
        serialConsistency: 10,
        traceQuery: true
      };

      // Execution profile options should not be used
      const executionProfile = new ExecutionProfile('a', {
        consistency: 100, serialConsistency: 200, retry: {}, readTimeout: 1000
      });

      const execOptions = DefaultExecutionOptions.create(options, getClientFake(executionProfile));

      assertExecutionOptions(execOptions, options);
    });

    it('should default some values from the execution profile', () => {
      const options = {
        autoPage: false,
        captureStackTrace: false,
        counter: false,
        customPayload: {},
        executionProfile: 'oltp2',
        fetchSize: 30,
        hints: [ 'int' ],
        isIdempotent: false,
        keyspace: 'ks3',
        logged: false,
        pageState: utils.allocBufferFromArray([ 1, 2, 3, 4 ]),
        prepare: false,
        routingNames: [ 'ab' ],
        routingIndexes: [ 1, 2 ],
        routingKey: utils.allocBufferFromArray([ 0, 1 ]),
        traceQuery: true
      };

      // The following execution profile options should be used
      const executionProfile = new ExecutionProfile('a', {
        consistency: 1, serialConsistency: 2, retry: {}, readTimeout: 3, loadBalancing: {}
      });

      const execOptions = DefaultExecutionOptions.create(options, getClientFake(executionProfile));

      assertExecutionOptions(execOptions, options);
      assertExecutionOptions(execOptions, executionProfile);
    });

    it('should default some values from the client options', () => {
      const options = {
        autoPage: false,
        executionProfile: 'oltp2',
        hints: [ 'text' ],
        keyspace: 'ks4',
        logged: true,
        pageState: utils.allocBufferFromArray([ 1, 2, 3, 4, 5 ]),
        traceQuery: true
      };

      const clientOptions = defaultOptions();

      clientOptions.queryOptions = {
        captureStackTrace: false,
        consistency: 4,
        customPayload: {},
        fetchSize: 50,
        isIdempotent: false,
        prepare: true,
        serialConsistency: 5,
        traceQuery: true
      };
      clientOptions.socketOptions.readTimeout = 3456;
      clientOptions.policies.retry = {};

      const execOptions = DefaultExecutionOptions.create(options, getClientFake(null, clientOptions));

      assertExecutionOptions(execOptions, options);
      assertExecutionOptions(execOptions, clientOptions.queryOptions);
      assert.strictEqual(execOptions.getReadTimeout(), clientOptions.socketOptions.readTimeout);
      assert.strictEqual(execOptions.getRetryPolicy(), clientOptions.policies.retry);
    });

    it('should allow null, undefined or function queryOptions argument', () => {
      const executionProfile = new ExecutionProfile('a', {
        consistency: 1, serialConsistency: 2, retry: {}, readTimeout: 3, loadBalancing: {}
      });

      [ null, undefined, () => {}].forEach(options => {
        const execOptions = DefaultExecutionOptions.create(options, getClientFake(executionProfile));
        assertExecutionOptions(execOptions, executionProfile);
      });
    });

    it('should convert hex pageState to Buffer', () => {
      const options = { pageState: 'abcd' };
      const execOptions = DefaultExecutionOptions.create(options, getClientFake());
      assert.deepStrictEqual(execOptions.getPageState(), utils.allocBufferFromString(options.pageState, 'hex'));
    });

    it('should expose the raw query options or an empty object', () => {
      [undefined, null, () => {}, { prepare: true, myCustomOption: 1 }].forEach(options => {
        const execOptions = DefaultExecutionOptions.create(options, getClientFake());

        const expectedOptions = options && typeof options !== 'function' ? options : utils.emptyObject;
        assert.strictEqual(execOptions.getRawQueryOptions(), expectedOptions);
      });
    });
  });

  describe('#getOrCreateTimestamp()', () => {
    it('should use the provided timestamp value', () => {
      const options = { timestamp: types.Long.fromNumber(10) };
      const execOptions = DefaultExecutionOptions.create(options, getClientFake());
      assert.strictEqual(execOptions.getOrGenerateTimestamp(), options.timestamp);
    });

    it('should convert from Number to Long', () => {
      const options = { timestamp: 5 };
      const execOptions = DefaultExecutionOptions.create(options, getClientFake());
      const value = execOptions.getOrGenerateTimestamp();
      helper.assertInstanceOf(value, types.Long);
      assert.ok(value.equals(options.timestamp));
    });

    it('should use the timestamp generator when no value is provided', () => {
      const clientOptions = defaultOptions();

      this.called = 0;
      clientOptions.policies.timestampGeneration.next = () => ++this.called;

      const execOptions = DefaultExecutionOptions.create({}, getClientFake(null, clientOptions));
      const value = execOptions.getOrGenerateTimestamp();
      helper.assertInstanceOf(value, types.Long);
      assert.ok(value.equals(types.Long.ONE));
      assert.strictEqual(this.called, 1);
    });
  });
});

/**
 * @param {ExecutionOptions} execOptions
 * @param expectedOptions
 */
function assertExecutionOptions(execOptions, expectedOptions) {
  const propToMethod = new Map([
    ['traceQuery', 'isQueryTracing'], ['retry', 'getRetryPolicy'], ['autoPage', 'isAutoPage'],
    ['counter', 'isBatchCounter'], ['logged', 'isBatchLogged'], ['prepare', 'isPrepared'],
    ['loadBalancing', 'getLoadBalancingPolicy']
  ]);

  const ignoreProps = new Set(['executionProfile', 'name', 'graphOptions']);

  Object.keys(expectedOptions).forEach(prop => {
    if (ignoreProps.has(prop)) {
      return;
    }

    let methodName = propToMethod.get(prop);

    if (!methodName) {
      if (prop.indexOf('is') === 0) {
        methodName = prop;
      } else {
        methodName = `get${prop.substr(0, 1).toUpperCase()}${prop.substr(1)}`;
      }
    }

    const method = execOptions[methodName];
    if (typeof method !== 'function') {
      throw new Error(`No method "${methodName}" found`);
    }

    assert.strictEqual(expectedOptions[prop], method.call(execOptions));
  });
}

function getClientFake(executionProfile, clientOptions) {
  return {
    profileManager: { getProfile: x => executionProfile || new ExecutionProfile(x || 'default') },
    options: clientOptions || defaultOptions(),
    controlConnection: { protocolVersion: 4 }
  };
}
