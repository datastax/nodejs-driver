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
const Client = require('../../../lib/client');
const types = require('../../../lib/types');
const utils = require('../../../lib/utils');
const policies = require('../../../lib/policies');
const ExecutionProfile = require('../../../lib/execution-profile').ExecutionProfile;
const GraphExecutor = require('../../../lib/datastax/graph/graph-executor');
const GraphExecutionOptions = require('../../../lib/datastax/graph/options').GraphExecutionOptions;
const helper = require('../../test-helper');

const proxyExecuteKey = 'ProxyExecute';

describe('GraphExecutor', function () {
  describe('#send()', () => {
    it('should encode the parameters and build GraphExecutionOptions', function () {
      const setup = setupGraphExecutor();
      setup.instance.send('QUERY1', { a: 1 }, null, helper.noop);

      assert.strictEqual(setup.executionArguments.query, 'QUERY1');
      assert.deepStrictEqual(setup.executionArguments.params, ['{"a":1}']);
      helper.assertInstanceOf(setup.executionArguments.options, GraphExecutionOptions);
      assert.ok(setup.executionArguments.options.getCustomPayload());
      assert.strictEqual(typeof setup.executionArguments.callback, 'function');
    });

    it('should not allow a namespace of type different than string', function () {
      const setup = setupGraphExecutor();

      assert.doesNotThrow(function () {
        setup.instance.send('Q1', {}, { graphName: 'abc' }, helper.throwop);
      });
      assert.throws(function () {
        setup.instance.send('Q1', {}, { graphName: 123 }, helper.throwop);
      }, TypeError);
    });

    it('should not allow a array query parameters', done => {
      const setup = setupGraphExecutor();
      setup.instance.send('Q1', [], { }, function (err) {
        helper.assertInstanceOf(err, TypeError);
        assert.strictEqual(err.message, 'Parameters must be a Object instance as an associative array');
        done();
      });
    });

    it('should set the same default options when not set', () => {
      const setup = setupGraphExecutor();

      setup.instance.send('Q5', { z: 3 }, null, helper.noop);
      const firstOptions = setup.executionArguments.options;
      assert.ok(firstOptions.getRawQueryOptions());

      setup.instance.send('Q6', { z: 4 }, null, helper.noop);
      assert.strictEqual(firstOptions.getRawQueryOptions(), setup.executionArguments.options.getRawQueryOptions());
      assert.ok(firstOptions.getCustomPayload());
    });

    it('should set the default payload for the executions', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name1',
          source: 'a1',
          readConsistency: types.consistencies.localOne
        }
      };

      const setup = setupGraphExecutor(clientOptions);
      const optionsParameter = { anotherOption: { k: 'v'}};

      setup.instance.send('Q5', { c: 0}, optionsParameter, helper.throwop);

      const actualOptions = setup.executionArguments.options;

      assert.notStrictEqual(optionsParameter, actualOptions.getRawQueryOptions());
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.getRawQueryOptions().anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'a1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'LOCAL_ONE');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);
    });

    it('should set the default readTimeout in the payload', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          source: 'x',
          writeConsistency: types.consistencies.two
        }
      };

      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;

      // with options defined
      setup.instance.send('Q10', { c: 0}, { }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'TWO');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');

      // with payload defined
      setup.instance.send('Q10', { c: 0}, { customPayload: { 'z': utils.allocBufferFromString('zValue')} }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      helper.assertBufferString(actualOptions.getCustomPayload()['z'], 'zValue');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');

      // with timeout defined
      setup.instance.send('Q10', { c: 0}, { readTimeout: 9999 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(actualOptions.getCustomPayload()['z'], undefined);
      assert.deepEqual(actualOptions.getCustomPayload()['request-timeout'],
        types.Long.toBuffer(types.Long.fromNumber(9999)));
      assert.strictEqual(actualOptions.getReadTimeout(), 9999);

      // without options defined
      setup.instance.send('Q10', { c: 0}, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');
    });

    it('should set the read and write consistency levels', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name10'
        }
      };

      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;

      setup.instance.send('Q5', { c: 0}, helper.throwop);
      actualOptions = setup.executionArguments.options;
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'g');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name10');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);

      let optionsParameter = {
        graphReadConsistency: types.consistencies.localQuorum
      };
      setup.instance.send('Q5', { c: 0}, optionsParameter, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.notStrictEqual(optionsParameter, actualOptions);
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.getRawQueryOptions().anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'LOCAL_QUORUM');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);

      optionsParameter = {
        graphWriteConsistency: types.consistencies.quorum
      };
      setup.instance.send('Q5', { c: 0}, optionsParameter, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.notStrictEqual(optionsParameter, actualOptions);
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'QUORUM');
    });

    it('should reuse the default payload for the executions', () => {
      const clientOptions = { contactPoints: ['host1'], graphOptions: { name: 'name1' }};
      const setup = setupGraphExecutor(clientOptions);
      const optionsParameter = { anotherOption: { k: 'v2'}};

      setup.instance.send('Q5', { a: 1}, optionsParameter, helper.throwop);
      const firstOptions = setup.executionArguments.options;
      setup.instance.send('Q6', { b: 1}, optionsParameter, helper.throwop);
      assert.strictEqual(firstOptions.getCustomPayload(), setup.executionArguments.options.getCustomPayload());
    });

    it('should set the payload with the options provided', () => {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          language: 'groovy2',
          source: 'another-source',
          name: 'namespace2'
        }};

      const setup = setupGraphExecutor(clientOptions);
      const optionsParameter = { anotherOption: { k: 'v3'}};
      setup.instance.send('Q5', { 'x': 1 }, optionsParameter, helper.throwop);

      const actualOptions = setup.executionArguments.options;
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.getRawQueryOptions().anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      assert.deepStrictEqual(actualOptions.getCustomPayload()['graph-language'],
        utils.allocBufferFromString('groovy2'));
      assert.deepStrictEqual(actualOptions.getCustomPayload()['graph-source'],
        utils.allocBufferFromString('another-source'));
      assert.deepStrictEqual(actualOptions.getCustomPayload()['graph-name'], utils.allocBufferFromString('namespace2'));
    });

    it('should set the payload with the user/role provided', function () {
      const clientOptions = { contactPoints: ['host1'], graphOptions: { name: 'name2' }};
      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;

      setup.instance.send('Q5', { 'x': 1 }, null, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name2');
      assert.strictEqual(actualOptions.getCustomPayload()[proxyExecuteKey], undefined);
      const previousPayload = actualOptions.getCustomPayload();

      setup.instance.send('Q5', { 'x': 1 }, { executeAs: 'alice' }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.notStrictEqual(previousPayload, actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name2');
      helper.assertBufferString(actualOptions.getCustomPayload()[proxyExecuteKey], 'alice');
    });

    it('should set the options according to default profile', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          language: 'groovy1',
          source: 'source1',
          name: 'name1'
        },
        profiles: [
          new ExecutionProfile('default', {
            graphOptions: {
              name: 'nameZ',
              readConsistency: types.consistencies.two
            }
          })
        ]
      };

      const setup = setupGraphExecutor(clientOptions);

      setup.instance.send('Q', { 'x': 1 }, null, helper.throwop);
      const actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'groovy1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'source1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'nameZ');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'TWO');
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
    });

    it('should set the options according to specified profile', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        graphOptions: {
          source: 'source1',
          name: 'name1'
        },
        profiles: [
          new ExecutionProfile('default', {
            graphOptions: {
              name: 'nameZ',
              readConsistency: types.consistencies.two
            }
          }),
          new ExecutionProfile('graph-olap', {
            graphOptions: {
              source: 'aaa',
              readConsistency: types.consistencies.three,
              writeConsistency: types.consistencies.quorum
            },
            readTimeout: 99000
          })
        ]
      };

      const setup = setupGraphExecutor(clientOptions);
      const optionsParameter = { executionProfile: 'graph-olap' };
      let actualOptions;
      setup.instance.send('Q', { 'x': 1 }, optionsParameter, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'aaa');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'nameZ');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'THREE');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'QUORUM');
      assert.deepEqual(actualOptions.getCustomPayload()['request-timeout'],
        types.Long.toBuffer(types.Long.fromNumber(99000)));
      const lastOptions = actualOptions;

      setup.instance.send('Q2', { 'x': 2 }, optionsParameter, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.notStrictEqual(actualOptions, lastOptions);
      // Reusing same customPayload instance
      assert.strictEqual(actualOptions.getCustomPayload(), lastOptions.getCustomPayload());
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      optionsParameter.retry = new policies.retry.RetryPolicy();

      setup.instance.send('Q2', { 'x': 3 }, optionsParameter, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.strictEqual(actualOptions.getRetryPolicy(), optionsParameter.retry);
    });

    it('should use the retry policy from the default profile when specified', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.FallthroughRetryPolicy();
      const retryPolicy3 = new policies.retry.FallthroughRetryPolicy();
      const clientOptions = {
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('default', {
            retry: retryPolicy1
          }),
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy2
          })
        ]
      };

      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;
      setup.instance.send('Q', null, null, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy3 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy3);

      setup.instance.send('Q', null, { retry: retryPolicy3 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy3);
    });

    it('should use specific retry policy when not specified in the default profile', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.RetryPolicy();
      const clientOptions = {
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('default'),
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy1
          })
        ]
      };
      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;

      setup.instance.send('Q', null, null, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);

      setup.instance.send('Q', null, { executionProfile: 'default'}, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy2 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);

      setup.instance.send('Q', null, { retry: retryPolicy2 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
    });

    it('should use specific retry policy when no default profile specified', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.RetryPolicy();
      const clientOptions = {
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy1
          })
        ]
      };

      const setup = setupGraphExecutor(clientOptions);
      let actualOptions;

      setup.instance.send('Q', null, null, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);

      setup.instance.send('Q', null, { executionProfile: 'default'}, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);

      setup.instance.send('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy2 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);

      setup.instance.send('Q', null, { retry: retryPolicy2 }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
    });

    it('should use the graph language provided in the profile', function () {
      const clientOptions = {
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('graph-olap', {
            graphOptions: { language: 'lolcode' }
          })
        ]
      };
      const setup = setupGraphExecutor(clientOptions);

      let actualOptions;
      setup.instance.send('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions && actualOptions.getCustomPayload());
      assert.strictEqual(actualOptions.getCustomPayload()['graph-language'].toString(), 'lolcode');

      setup.instance.send('Q', null, null, helper.throwop);
      actualOptions = setup.executionArguments.options;
      assert.ok(actualOptions && actualOptions.getCustomPayload());
      assert.strictEqual(actualOptions.getCustomPayload()['graph-language'].toString(), 'gremlin-groovy');
    });

    context('with analytics queries', function () {
      it('should query for analytics master', () => {
        const clientOptions = {
          contactPoints: ['host1'],
          graphOptions: {
            source: 'a',
            name: 'name1'
          }};

        const setup = setupGraphExecutor(clientOptions);

        setup.instance.send('g.V()', null, {}, utils.noop);
        const actualOptions = setup.executionArguments.options;
        assert.ok(actualOptions);
        assert.ok(actualOptions.getPreferredHost());
        assert.ok(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
      });

      it('should query for analytics master when using execution profile', () => {
        const clientOptions = {
          contactPoints: ['host1'],
          profiles: [
            new ExecutionProfile('analytics', {
              graphOptions: {
                source: 'a'
              }
            })
          ]
        };

        const setup = setupGraphExecutor(clientOptions);

        setup.instance.send('g.V()', null, { executionProfile: 'analytics' }, utils.noop);
        const actualOptions = setup.executionArguments.options;
        assert.ok(actualOptions);
        assert.ok(actualOptions.getPreferredHost());
        assert.ok(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
      });

      it('should call address translator', function () {
        let translatorCalled = 0;
        const translator = new policies.addressResolution.AddressTranslator();
        translator.translate = function (ip, port, cb) {
          translatorCalled++;
          cb(ip + ':' + port);
        };

        const clientOptions = {
          contactPoints: ['host1'],
          graphOptions: {
            source: 'a',
            name: 'name1'
          },
          policies: {
            addressResolution: translator
          }
        };

        const setup = setupGraphExecutor(clientOptions);

        setup.instance.send('g.V()', null, null, utils.noop);
        const actualOptions = setup.executionArguments.options;
        assert.ok(actualOptions);
        assert.ok(actualOptions.getPreferredHost());
        assert.strictEqual(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
        assert.strictEqual(translatorCalled, 1);
      });

      it('should set preferredHost to null when RPC errors', function () {
        const clientOptions = {
          contactPoints: ['host1'],
          graphOptions: {
            source: 'a',
            name: 'name1'
          }};

        const clientProperties = {
          execute: function (q, p, options, cb) {
            if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
              return cb(new Error('Test error'));
            }

            cb(null, { rows: []});
          }
        };

        const setup = setupGraphExecutor(clientOptions, clientProperties);

        setup.instance.send('g.V()');
        const actualOptions = setup.executionArguments.options;
        assert.ok(actualOptions);
        assert.strictEqual(actualOptions.getPreferredHost(), null);
      });
    });
  });
});

function setupGraphExecutor(clientOptions, clientProperties) {
  clientOptions = clientOptions || { contactPoints: [ 'h1' ] };
  const client = new Client(clientOptions);

  const result = {
    executionArguments: null,
    instance: null
  };

  result.instance = new GraphExecutor(client, clientOptions, (query, params, options, callback) => {
    result.executionArguments = { query, params, options, callback };
  });

  client.hosts = { get: address => ({ type: 'host', address: address }) };
  client.execute = function (q, p, options, cb) {
    if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
      return cb(null, { rows: [ { result: { location: '10.10.10.10:1234' }} ]});
    }

    cb(null, { rows: []});
  };

  if (clientProperties) {
    Object.keys(clientProperties).forEach(propName => client[propName] = clientProperties[propName]);
  }

  return result;
}