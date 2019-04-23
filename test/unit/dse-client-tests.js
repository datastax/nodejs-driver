/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const Client = require('../../lib/dse-client');
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const helper = require('../test-helper');
const types = require('../../lib/types');
const Long = types.Long;
const utils = require('../../lib/utils');
const policies = require('../../lib/policies');
const errors = require('../../lib/errors');
const DseLoadBalancingPolicy = policies.loadBalancing.DseLoadBalancingPolicy;
const GraphExecutionOptions = require('../../lib/graph/options').GraphExecutionOptions;
const proxyExecuteKey = require('../../lib/execution-options').proxyExecuteKey;

describe('Client', function () {
  describe('constructor', function () {
    it('should validate options', function () {
      assert.throws(function () {
        // eslint-disable-next-line
        new Client();
      }, errors.ArgumentError);
    });

    it('should validate client id, application name and version', () => {
      assert.throws(() => new Client(Object.assign({ applicationName: 123 }, helper.baseOptions)),
        /applicationName should be a String/);

      assert.throws(() => new Client(Object.assign({ applicationVersion: 123 }, helper.baseOptions)),
        /applicationVersion should be a String/);

      assert.throws(() => new Client(Object.assign({ id: 123 }, helper.baseOptions)),
        /Client id must be a Uuid/);

      assert.doesNotThrow(() => new Client(Object.assign({ applicationName: 'App_Z', applicationVersion: '1.2.3' },
        helper.baseOptions)));

      assert.doesNotThrow(() =>
        new Client(Object.assign(
          { applicationName: 'App_Z', applicationVersion: '1.2.3', id: types.Uuid.random() },
          helper.baseOptions)));

      assert.doesNotThrow(() => new Client(Object.assign({ applicationName: 'App_Z'}, helper.baseOptions)));
      assert.doesNotThrow(() => new Client(Object.assign({ id: types.TimeUuid.now() }, helper.baseOptions)));
    });

    it('should set DseLoadBalancingPolicy as default', function () {
      let client = new Client({ contactPoints: ['host1'] });
      helper.assertInstanceOf(client.options.policies.loadBalancing, DseLoadBalancingPolicy);
      const retryPolicy = new policies.retry.RetryPolicy();
      client = new Client({
        contactPoints: ['host1'],
        // with some of the policies specified
        policies: { retry: retryPolicy }
      });
      helper.assertInstanceOf(client.options.policies.loadBalancing, DseLoadBalancingPolicy);
      assert.strictEqual(client.options.policies.retry, retryPolicy);
    });
  });
  describe('#connect()', function () {
    context('with no callback specified', function () {
      it('should return a promise', function (done) {
        const client = new Client(helper.baseOptions);
        const p = client.connect();
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          done();
        });
      });
    });
  });
  describe('#executeGraph()', function () {
    it('should allow optional parameters', function () {
      const client = new Client({ contactPoints: ['host1']});
      let parameters = {};
      client._innerExecute = function (query, params, options, callback) {
        parameters = {
          query: query,
          params: params,
          options: options,
          callback: callback
        };
      };

      client.executeGraph('QUERY1', { a: 1 }, helper.noop);
      assert.strictEqual(parameters.query, 'QUERY1');
      assert.deepEqual(parameters.params, ['{"a":1}']);
      assert.ok(parameters.options);
      assert.ok(parameters.options.getCustomPayload());
      assert.strictEqual(typeof parameters.callback, 'function');

      client.executeGraph('QUERY2', helper.noop);
      assert.strictEqual(parameters.query, 'QUERY2');
      assert.strictEqual(parameters.params, null);
      assert.ok(parameters.options);
      assert.ok(parameters.options.getCustomPayload());
      assert.strictEqual(typeof parameters.callback, 'function');
    });
    it('should not allow a namespace of type different than string', function () {
      const client = new Client({ contactPoints: ['host1']});
      client._innerExecute = helper.noop;
      assert.doesNotThrow(function () {
        client.executeGraph('Q1', {}, { graphName: 'abc' }, helper.throwop);
      });
      assert.throws(function () {
        client.executeGraph('Q1', {}, { graphName: 123 }, helper.throwop);
      }, TypeError);
    });

    it('should not allow a array query parameters', function () {
      const client = new Client({ contactPoints: ['host1']});
      client._innerExecute = helper.noop;
      client.executeGraph('Q1', [], { }, function (err) {
        helper.assertInstanceOf(err, TypeError);
        assert.strictEqual(err.message, 'Parameters must be a Object instance as an associative array');
      });
    });
    it('should execute with query and callback parameters', function (done) {
      const client = new Client({ contactPoints: ['host1']});
      client._innerExecute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q2');
        assert.strictEqual(params, null);
        helper.assertInstanceOf(options, GraphExecutionOptions);
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q2', helper.noop);
    });
    it('should execute with query, parameters and callback parameters', function (done) {
      const client = new Client({ contactPoints: ['host1']});
      client._innerExecute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q3');
        assert.deepEqual(params, [JSON.stringify({ a: 1})]);
        helper.assertInstanceOf(options, GraphExecutionOptions);
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q3', { a: 1}, helper.throwop);
    });
    it('should execute with all parameters defined', function (done) {
      const client = new Client({ contactPoints: ['host1']});
      const optionsParameter = { k: { } };

      client._innerExecute = function (query, params, execOptions, callback) {
        assert.strictEqual(query, 'Q4');
        assert.deepEqual(params, [JSON.stringify({ a: 2})]);
        helper.assertInstanceOf(execOptions, GraphExecutionOptions);
        assert.strictEqual(optionsParameter.k, execOptions.getRawQueryOptions().k);
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q4', { a: 2}, optionsParameter, helper.throwop);
    });
    it('should set the same default options when not set', function (done) {
      const client = new Client({ contactPoints: ['host1']});
      const optionsArray = [];
      client._innerExecute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q5');
        assert.deepEqual(params, [JSON.stringify({ z: 3})]);
        optionsArray.push(options);
        assert.strictEqual(typeof callback, 'function');
      };
      client.executeGraph('Q5', { z: 3 }, helper.noop);
      client.executeGraph('Q5', { z: 3 }, helper.noop);
      assert.strictEqual(optionsArray.length, 2);
      assert.strictEqual(optionsArray[0].getRawQueryOptions(), optionsArray[1].getRawQueryOptions());
      assert.ok(optionsArray[0].getCustomPayload());
      done();
    });
    it('should set the default payload for the executions', function () {
      const client = new Client({
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name1',
          source: 'a1',
          readConsistency: types.consistencies.localOne
        }
      });
      const optionsParameter = { anotherOption: { k: 'v'}};
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwop);
      assert.notStrictEqual(optionsParameter, actualOptions);
      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.getRawQueryOptions().anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'a1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'LOCAL_ONE');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);
    });
    it('should set the default readTimeout in the payload', function () {
      //noinspection JSCheckFunctionSignatures
      const client = new Client({
        contactPoints: ['host1'],
        graphOptions: {
          source: 'x',
          writeConsistency: types.consistencies.two
        }
      });
      let actualOptions = null;
      client._innerExecute = function (q, p, options) {
        actualOptions = options;
      };  
      //with options defined
      client.executeGraph('Q10', { c: 0}, { }, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'TWO');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');

      //with payload defined
      client.executeGraph('Q10', { c: 0}, { customPayload: { 'z': utils.allocBufferFromString('zValue')} }, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      helper.assertBufferString(actualOptions.getCustomPayload()['z'], 'zValue');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');

      //with timeout defined
      client.executeGraph('Q10', { c: 0}, { readTimeout: 9999 }, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(actualOptions.getCustomPayload()['z'], undefined);
      assert.deepEqual(actualOptions.getCustomPayload()['request-timeout'], Long.toBuffer(Long.fromNumber(9999)));
      assert.strictEqual(actualOptions.getReadTimeout(), 9999);

      //without options defined
      client.executeGraph('Q10', { c: 0}, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'x');
      assert.strictEqual(typeof actualOptions.getCustomPayload()['request-timeout'], 'undefined');
    });
    it('should set the read and write consistency levels', function () {
      const client = new Client({
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name10'
        }
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q5', { c: 0}, helper.throwop);
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'g');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name10');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);
      let optionsParameter = {
        graphReadConsistency: types.consistencies.localQuorum
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwop);
      assert.notStrictEqual(optionsParameter, actualOptions);

      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.getRawQueryOptions().anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'LOCAL_QUORUM');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-write-consistency'], undefined);
      optionsParameter = {
        graphWriteConsistency: types.consistencies.quorum
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwop);
      assert.notStrictEqual(optionsParameter, actualOptions);
      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.anotherOption);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      assert.strictEqual(actualOptions.getCustomPayload()['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'QUORUM');
    });
    it('should reuse the default payload for the executions', function (done) {
      const client = new Client({ contactPoints: ['host1'], graphOptions: { name: 'name1' }});
      const optionsParameter = { anotherOption: { k: 'v2'}};
      const actualOptions = [];
      client._innerExecute = function (query, params, options) {
        //do not use the actual object
        assert.notStrictEqual(optionsParameter, options);
        //shallow copy the properties
        assert.strictEqual(optionsParameter.anotherOption, options.getRawQueryOptions().anotherOption);
        assert.ok(options.getCustomPayload());
        actualOptions.push(options);
      };
      client.executeGraph('Q5', { a: 1}, optionsParameter, helper.throwop);
      client.executeGraph('Q6', { b: 1}, optionsParameter, helper.throwop);
      assert.strictEqual(actualOptions.length, 2);
      assert.strictEqual(actualOptions[0].getCustomPayload(), actualOptions[1].getCustomPayload());
      done();
    });
    it('should set the payload with the options provided', function (done) {
      const client = new Client({ contactPoints: ['host1'], graphOptions: {
        language: 'groovy2',
        source: 'another-source',
        name: 'namespace2'
      }});
      const optionsParameter = { anotherOption: { k: 'v3'}};
      client._innerExecute = function (query, params, options) {
        //do not use the actual object
        assert.notStrictEqual(optionsParameter, options);
        //shallow copy the properties
        assert.strictEqual(optionsParameter.anotherOption, options.getRawQueryOptions().anotherOption);
        assert.ok(options.getCustomPayload());
        assert.deepEqual(options.getCustomPayload()['graph-language'], utils.allocBufferFromString('groovy2'));
        assert.deepEqual(options.getCustomPayload()['graph-source'], utils.allocBufferFromString('another-source'));
        assert.deepEqual(options.getCustomPayload()['graph-name'], utils.allocBufferFromString('namespace2'));
        done();
      };
      client.executeGraph('Q5', { 'x': 1 }, optionsParameter, helper.throwop);
    });
    it('should set the payload with the user/role provided', function () {
      const client = new Client({ contactPoints: ['host1'], graphOptions: {
        name: 'name2'
      }});
      let actualOptions = null;
      client._innerExecute = function (q, p, options) {
        actualOptions = options;
      };
      client.executeGraph('Q5', { 'x': 1 }, null, helper.throwop);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name2');
      assert.strictEqual(actualOptions.getCustomPayload()[proxyExecuteKey], undefined);
      const previousPayload = actualOptions.getCustomPayload();
      client.executeGraph('Q5', { 'x': 1 }, { executeAs: 'alice' }, helper.throwop);
      assert.notStrictEqual(previousPayload, actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'name2');
      helper.assertBufferString(actualOptions.getCustomPayload()[proxyExecuteKey], 'alice');
    });
    it('should set the options according to default profile', function () {
      const client = new Client({
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
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', { 'x': 1 }, null, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-language'], 'groovy1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'source1');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'nameZ');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'TWO');
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
    });
    it('should set the options according to specified profile', function () {
      const client = new Client({
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
      });
      const optionsParameter = { executionProfile: 'graph-olap' };
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', { 'x': 1 }, optionsParameter, helper.throwop);
      assert.ok(actualOptions);
      assert.ok(actualOptions.getCustomPayload());
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-source'], 'aaa');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-name'], 'nameZ');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-read-consistency'], 'THREE');
      helper.assertBufferString(actualOptions.getCustomPayload()['graph-write-consistency'], 'QUORUM');
      assert.deepEqual(actualOptions.getCustomPayload()['request-timeout'], Long.toBuffer(Long.fromNumber(99000)));
      const lastOptions = actualOptions;
      client.executeGraph('Q2', { 'x': 2 }, optionsParameter, helper.throwop);
      assert.notStrictEqual(actualOptions, lastOptions);
      // Reusing same customPayload instance
      assert.strictEqual(actualOptions.getCustomPayload(), lastOptions.getCustomPayload());
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      optionsParameter.retry = new policies.retry.RetryPolicy();
      client.executeGraph('Q2', { 'x': 3 }, optionsParameter, helper.throwop);
      assert.strictEqual(actualOptions.getRetryPolicy(), optionsParameter.retry);
    });
    it('should let the core driver deal with profile specified not found', function (done) {
      const client = new Client({
        contactPoints: ['host1'],
        graphOptions: {
          source: 'source1'
        },
        profiles: [
          new ExecutionProfile('default', {
            graphOptions: {
              name: 'name-default'
            }
          })
        ]
      });
      const optionsParameter = { executionProfile: 'profile-x' };
      let innerExecuteCalled = false;

      client._innerExecute = () => innerExecuteCalled = true;

      client.executeGraph('Q', { 'x': 1 }, optionsParameter, err => {
        helper.assertInstanceOf(err, errors.ArgumentError);
        assert.ok(!innerExecuteCalled);
        done();
      });
    });
    it('should use the retry policy from the default profile when specified', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.FallthroughRetryPolicy();
      const retryPolicy3 = new policies.retry.FallthroughRetryPolicy();
      const client = new Client({
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('default', {
            retry: retryPolicy1
          }),
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy2
          })
        ]
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', null, null, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy3 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy3);
      client.executeGraph('Q', null, { retry: retryPolicy3 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy3);
    });
    it('should use specific retry policy when not specified in the default profile', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.RetryPolicy();
      const client = new Client({
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('default'),
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy1
          })
        ]
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', null, null, helper.throwop);
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      client.executeGraph('Q', null, { executionProfile: 'default'}, helper.throwop);
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy2 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
      client.executeGraph('Q', null, { retry: retryPolicy2 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
    });
    it('should use specific retry policy when no default profile specified', function () {
      const retryPolicy1 = new policies.retry.RetryPolicy();
      const retryPolicy2 = new policies.retry.RetryPolicy();
      const client = new Client({
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('graph-olap', {
            retry: retryPolicy1
          })
        ]
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', null, null, helper.throwop);
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      client.executeGraph('Q', null, { executionProfile: 'default'}, helper.throwop);
      assert.ok(actualOptions);
      helper.assertInstanceOf(actualOptions.getRetryPolicy(), policies.retry.FallthroughRetryPolicy);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy1);
      client.executeGraph('Q', null, { executionProfile: 'graph-olap', retry: retryPolicy2 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
      client.executeGraph('Q', null, { retry: retryPolicy2 }, helper.throwop);
      assert.ok(actualOptions);
      assert.strictEqual(actualOptions.getRetryPolicy(), retryPolicy2);
    });
    it('should use the graph language provided in the profile', function () {
      const client = new Client({
        contactPoints: ['host1'],
        profiles: [
          new ExecutionProfile('graph-olap', {
            graphOptions: { language: 'lolcode' }
          })
        ]
      });
      let actualOptions = null;
      client._innerExecute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q', null, { executionProfile: 'graph-olap' }, helper.throwop);
      assert.ok(actualOptions && actualOptions.getCustomPayload());
      assert.strictEqual(actualOptions.getCustomPayload()['graph-language'].toString(), 'lolcode');
      client.executeGraph('Q', null, null, helper.throwop);
      assert.ok(actualOptions && actualOptions.getCustomPayload());
      assert.strictEqual(actualOptions.getCustomPayload()['graph-language'].toString(), 'gremlin-groovy');
    });
    context('with analytics queries', function () {
      it('should query for analytics master', function (done) {
        const client = new Client({ contactPoints: ['host1'], graphOptions: {
          source: 'a',
          name: 'name1'
        }});
        let actualOptions;
        client._innerExecute = function (q, p, options, cb) {
          if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
            return cb(null, { rows: [ { result: { location: '10.10.10.10:1234' }} ]});
          }
          actualOptions = options;
          cb(null, { rows: []});
        };
        //noinspection JSValidateTypes
        client.hosts = { get: function (address) {
          return { type: 'host', address: address };
        }};
        utils.series([
          function withUndefinedOptions(next) {
            client.executeGraph('g.V()', function (err) {
              assert.ifError(err);
              assert.ok(actualOptions);
              assert.ok(actualOptions.getPreferredHost());
              assert.ok(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
              next();
            });
          },
          function withEmptyOptions(next) {
            client.executeGraph('g.V()', null, {}, function (err) {
              assert.ifError(err);
              assert.ok(actualOptions);
              assert.ok(actualOptions.getPreferredHost());
              assert.ok(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
              next();
            });
          }
        ], done);
      });
      it('should query for analytics master when using execution profile', function (done) {
        const client = new Client({
          contactPoints: ['host1'],
          profiles: [
            new ExecutionProfile('analytics', {
              graphOptions: {
                source: 'a'
              }
            })
          ]
        });
        let actualOptions;
        client._innerExecute = function (q, p, options, cb) {
          if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
            return cb(null, { rows: [ { result: { location: '10.10.10.10:1234' }} ]});
          }
          actualOptions = options;
          cb(null, { rows: []});
        };
        //noinspection JSValidateTypes
        client.hosts = { get: function (address) {
          return { type: 'host', address: address };
        }};
        client.executeGraph('g.V()', null, { executionProfile: 'analytics' }, function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          assert.ok(actualOptions.getPreferredHost());
          assert.ok(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
          done();
        });
      });
      it('should call address translator', function (done) {
        let translatorCalled = 0;
        const translator = new policies.addressResolution.AddressTranslator();
        translator.translate = function (ip, port, cb) {
          translatorCalled++;
          cb(ip + ':' + port);
        };
        const client = new Client({
          contactPoints: ['host1'], 
          graphOptions: { 
            source: 'a', 
            name: 'name1'
          },
          policies: {
            addressResolution: translator
          }
        });
        let actualOptions;
        client._innerExecute = function (q, p, options, cb) {
          if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
            return cb(null, { rows: [ { result: { location: '10.10.10.10:1234' }} ]});
          }
          actualOptions = options;
          cb(null, { rows: []});
        };
        //noinspection JSValidateTypes
        client.hosts = { get: function (address) {
          return { type: 'host', address: address };
        }};
        client.executeGraph('g.V()', function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          assert.ok(actualOptions.getPreferredHost());
          assert.strictEqual(actualOptions.getPreferredHost().address, '10.10.10.10:9042');
          assert.strictEqual(translatorCalled, 1);
          done();
        });
      });
      it('should set preferredHost to null when RPC errors', function (done) {
        const client = new Client({ contactPoints: ['host1'], graphOptions: {
          source: 'a',
          name: 'name1'
        }});
        let actualOptions;
        client._innerExecute = function (q, p, options, cb) {
          if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
            return cb(new Error('Test error'));
          }
          actualOptions = options;
          cb(null, { rows: []});
        };
        client.executeGraph('g.V()', function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          assert.strictEqual(actualOptions.getPreferredHost(), null);
          done();
        });
      });
    });
    context('with no callback specified', function () {
      it('should return a promise', function () {
        const client = new Client(helper.baseOptions);
        let called = 0;
        let callback;
        let options;
        let params;
        client._innerExecute = function (query, p, o, cb) {
          called++;
          params = p;
          options = o;
          callback = cb;
          cb(null, { rows: [] });
        };
        const expectedParams = { id: {} };
        const expectedOptions = { consistency: types.consistencies.three };
        const p = client.executeGraph('g.V()');
        helper.assertInstanceOf(p, Promise);
        return p
          .then(function () {
            // Should use a callback internally
            assert.strictEqual(typeof callback, 'function');
            assert.strictEqual(called, 1);
            const p = client.executeGraph('g.V(id)', expectedParams);
            helper.assertInstanceOf(p, Promise);
            return p;
          })
          .then(function () {
            assert.strictEqual(called, 2);
            assert.ok(params);
            // Single parameter with json parameters
            assert.strictEqual(params[0], JSON.stringify(expectedParams));
            const p = client.executeGraph('g.V(id)', expectedParams, expectedOptions);
            helper.assertInstanceOf(p, Promise);
            return p;
          })
          .then(function () {
            assert.strictEqual(called, 3);
            assert.ok(params);
            assert.strictEqual(params[0], JSON.stringify(expectedParams));
            assert.ok(options);
            assert.strictEqual(options.getConsistency(), expectedOptions.consistency);
            return null;
          });
      });
    });
  });
});