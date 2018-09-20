'use strict';

const assert = require('assert');
const utils = require('../../lib/utils');
const types = require('../../lib/types');
const helper = require('../test-helper');
const DefaultExecutionInfo = require('../../lib/execution-info').DefaultExecutionInfo;
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const defaultOptions = require('../../lib/client-options').defaultOptions;

describe('DefaultExecutionInfo', () => {
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

      const info = DefaultExecutionInfo.create(options, getClientFake(executionProfile));

      assertExecutionInfo(info, options);
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

      const info = DefaultExecutionInfo.create(options, getClientFake(executionProfile));

      assertExecutionInfo(info, options);
      assertExecutionInfo(info, executionProfile);
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

      const info = DefaultExecutionInfo.create(options, getClientFake(null, clientOptions));

      assertExecutionInfo(info, options);
      assertExecutionInfo(info, clientOptions.queryOptions);
      assert.strictEqual(info.getReadTimeout(), clientOptions.socketOptions.readTimeout);
      assert.strictEqual(info.getRetryPolicy(), clientOptions.policies.retry);
    });

    it('should allow null, undefined or function queryOptions argument', () => {
      const executionProfile = new ExecutionProfile('a', {
        consistency: 1, serialConsistency: 2, retry: {}, readTimeout: 3, loadBalancing: {}
      });

      [ null, undefined, () => {}].forEach(options => {
        const info = DefaultExecutionInfo.create(options, getClientFake(executionProfile));
        assertExecutionInfo(info, executionProfile);
      });
    });

    it('should convert hex pageState to Buffer', () => {
      const options = { pageState: 'abcd' };
      const info = DefaultExecutionInfo.create(options, getClientFake());
      assert.deepStrictEqual(info.getPageState(), utils.allocBufferFromString(options.pageState, 'hex'));
    });
  });

  describe('#getOrCreateTimestamp()', () => {
    it('should use the provided timestamp value', () => {
      const options = { timestamp: types.Long.fromNumber(10) };
      const info = DefaultExecutionInfo.create(options, getClientFake());
      assert.strictEqual(info.getOrGenerateTimestamp(), options.timestamp);
    });

    it('should convert from Number to Long', () => {
      const options = { timestamp: 5 };
      const info = DefaultExecutionInfo.create(options, getClientFake());
      const value = info.getOrGenerateTimestamp();
      helper.assertInstanceOf(value, types.Long);
      assert.ok(value.equals(options.timestamp));
    });

    it('should use the timestamp generator when no value is provided', () => {
      const clientOptions = defaultOptions();

      this.called = 0;
      clientOptions.policies.timestampGeneration.next = () => ++this.called;

      const info = DefaultExecutionInfo.create({}, getClientFake(null, clientOptions));
      const value = info.getOrGenerateTimestamp();
      helper.assertInstanceOf(value, types.Long);
      assert.ok(value.equals(types.Long.ONE));
      assert.strictEqual(this.called, 1);
    });
  });
});

/**
 * @param {ExecutionInfo} info
 * @param expectedOptions
 */
function assertExecutionInfo(info, expectedOptions) {
  const propToMethod = new Map([
    ['traceQuery', 'getIsQueryTracing'], ['retry', 'getRetryPolicy'], ['autoPage', 'getIsAutoPage'],
    ['counter', 'getIsBatchCounter'], ['logged', 'getIsBatchLogged'], ['prepare', 'getIsPrepared'],
    ['loadBalancing', 'getLoadBalancingPolicy']
  ]);

  const ignoreProps = new Set(['executionProfile', 'name']);

  Object.keys(expectedOptions).forEach(prop => {
    if (ignoreProps.has(prop)) {
      return;
    }

    let methodName = propToMethod.get(prop);

    if (!methodName) {
      methodName = `get${prop.substr(0, 1).toUpperCase()}${prop.substr(1)}`;
    }

    const method = info[methodName];
    if (typeof method !== 'function') {
      throw new Error(`No method "${methodName}" found`);
    }

    assert.strictEqual(expectedOptions[prop], method.call(info));
  });
}

function getClientFake(executionProfile, clientOptions) {
  return {
    profileManager: { getProfile: x => executionProfile || new ExecutionProfile(x || 'default') },
    options: clientOptions || defaultOptions(),
    controlConnection: { protocolVersion: 4 }
  };
}
