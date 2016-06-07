/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var async = require('async');
var cassandra = require('cassandra-driver');
var DseClient = require('../../lib/dse-client');
var helper = require('../helper');

describe('DseClient', function () {
  describe('constructor', function () {
    it('should validate options', function () {
      assert.throws(function () {
        //noinspection JSCheckFunctionSignatures
        new DseClient();
      }, cassandra.errors.ArgumentError);
    });
  });
  describe('#executeGraph()', function () {
    it('should allow optional parameters', function () {
      var client = new DseClient({ contactPoints: ['host1']});
      var parameters = {};
      client.execute = function (query, params, options, callback) {
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
      assert.ok(parameters.options.customPayload);
      assert.strictEqual(typeof parameters.callback, 'function');
      client.executeGraph('QUERY2', helper.noop);
      assert.strictEqual(parameters.query, 'QUERY2');
      assert.strictEqual(parameters.params, null);
      assert.ok(parameters.options);
      assert.ok(parameters.options.customPayload);
      assert.strictEqual(typeof parameters.callback, 'function');
    });
    it('should not allow a namespace of type different than string', function () {
      var client = new DseClient({ contactPoints: ['host1']});
      client.execute = helper.noop;
      assert.doesNotThrow(function () {
        client.executeGraph('Q1', {}, { graphName: 'abc' }, helper.noop)
      });
      assert.throws(function () {
        client.executeGraph('Q1', {}, { graphName: 123 }, helper.noop)
      }, TypeError);
    });
    it('should not allow a array query parameters', function () {
      var client = new DseClient({ contactPoints: ['host1']});
      client.execute = helper.noop;
      client.executeGraph('Q1', [], {  }, function (err) {
        helper.assertInstanceOf(err, TypeError);
        assert.strictEqual(err.message, 'Parameters must be a Object instance as an associative array');
      });
    });
    it('should execute with query and callback parameters', function (done) {
      var client = new DseClient({ contactPoints: ['host1']});
      client.execute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q2');
        assert.strictEqual(params, null);
        assert.strictEqual(typeof options, 'object');
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q2', helper.noop);
    });
    it('should execute with query, parameters and callback parameters', function (done) {
      var client = new DseClient({ contactPoints: ['host1']});
      client.execute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q3');
        assert.deepEqual(params, [JSON.stringify({ a: 1})]);
        assert.strictEqual(typeof options, 'object');
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q3', { a: 1}, helper.throwOp);
    });
    it('should execute with all parameters defined', function (done) {
      var client = new DseClient({ contactPoints: ['host1']});
      var optionsParameter = { k: { } };
      client.execute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q4');
        assert.deepEqual(params, [JSON.stringify({ a: 2})]);
        assert.notStrictEqual(optionsParameter, options);
        assert.strictEqual(optionsParameter.k, options.k);
        assert.strictEqual(typeof callback, 'function');
        done();
      };
      client.executeGraph('Q4', { a: 2}, optionsParameter, helper.throwOp);
    });
    it('should set the same default options when not set', function (done) {
      var client = new DseClient({ contactPoints: ['host1']});
      var optionsArray = [];
      client.execute = function (query, params, options, callback) {
        assert.strictEqual(query, 'Q5');
        assert.deepEqual(params, [JSON.stringify({ z: 3})]);
        optionsArray.push(options);
        assert.strictEqual(typeof callback, 'function');
      };
      client.executeGraph('Q5', { z: 3 }, helper.noop);
      client.executeGraph('Q5', { z: 3 }, helper.noop);
      assert.strictEqual(optionsArray.length, 2);
      assert.strictEqual(optionsArray[0], optionsArray[1]);
      assert.ok(optionsArray[0].customPayload);
      done();
    });
    it('should set the default payload for the executions', function () {
      var client = new DseClient({
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name1',
          source: 'a1',
          readConsistency: cassandra.types.consistencies.localOne
        }
      });
      var optionsParameter = { anotherOption: { k: 'v'}};
      var actualOptions = null;
      client.execute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwOp);
      assert.notStrictEqual(optionsParameter, actualOptions);
      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.anotherOption);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'a1');
      helper.assertBufferString(actualOptions.customPayload['graph-name'], 'name1');
      helper.assertBufferString(actualOptions.customPayload['graph-read-consistency'], 'LOCAL_ONE');
      assert.strictEqual(actualOptions.customPayload['graph-write-consistency'], undefined);
    });
    it('should use the default readTimeout', function () {
      //noinspection JSCheckFunctionSignatures
      var client = new DseClient({
        contactPoints: ['host1'],
        graphOptions: {
          source: 'x',
          readTimeout: 12345,
          writeConsistency: cassandra.types.consistencies.two
        }
      });
      var actualOptions = null;
      client.execute = function (q, p, options) {
        actualOptions = options;
      };
      //with options defined
      client.executeGraph('Q10', { c: 0}, { }, helper.throwOp);
      assert.ok(actualOptions);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'x');
      assert.strictEqual(actualOptions.customPayload['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.customPayload['graph-write-consistency'], 'TWO');
      assert.strictEqual(actualOptions.readTimeout, 12345);
      //with payload defined
      client.executeGraph('Q10', { c: 0}, { customPayload: { 'z': new Buffer('zValue')} }, helper.throwOp);
      assert.ok(actualOptions);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'x');
      helper.assertBufferString(actualOptions.customPayload['z'], 'zValue');
      assert.strictEqual(actualOptions.readTimeout, 12345);
      //with timeout defined
      client.executeGraph('Q10', { c: 0}, { readTimeout: 9999 }, helper.throwOp);
      assert.ok(actualOptions);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'x');
      assert.strictEqual(actualOptions.customPayload['z'], undefined);
      assert.strictEqual(actualOptions.readTimeout, 9999);
      //without options defined
      client.executeGraph('Q10', { c: 0}, helper.throwOp);
      assert.ok(actualOptions);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'x');
      assert.strictEqual(actualOptions.readTimeout, 12345);
    });
    it('should set the read and write consistency levels', function () {
      var client = new DseClient({
        contactPoints: ['host1'],
        graphOptions: {
          name: 'name10'
        }
      });
      var actualOptions = null;
      client.execute = function (query, params, options) {
        actualOptions = options;
      };
      client.executeGraph('Q5', { c: 0}, helper.throwOp);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-source'], 'g');
      helper.assertBufferString(actualOptions.customPayload['graph-name'], 'name10');
      assert.strictEqual(actualOptions.customPayload['graph-read-consistency'], undefined);
      assert.strictEqual(actualOptions.customPayload['graph-write-consistency'], undefined);
      var optionsParameter = {
        graphReadConsistency: cassandra.types.consistencies.localQuorum
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwOp);
      assert.notStrictEqual(optionsParameter, actualOptions);
      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.anotherOption);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      helper.assertBufferString(actualOptions.customPayload['graph-read-consistency'], 'LOCAL_QUORUM');
      assert.strictEqual(actualOptions.customPayload['graph-write-consistency'], undefined);
      optionsParameter = {
        graphWriteConsistency: cassandra.types.consistencies.quorum
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwOp);
      assert.notStrictEqual(optionsParameter, actualOptions);
      //shallow copy the properties
      assert.strictEqual(optionsParameter.anotherOption, actualOptions.anotherOption);
      assert.ok(actualOptions.customPayload);
      helper.assertBufferString(actualOptions.customPayload['graph-language'], 'gremlin-groovy');
      assert.strictEqual(actualOptions.customPayload['graph-read-consistency'], undefined);
      helper.assertBufferString(actualOptions.customPayload['graph-write-consistency'], 'QUORUM');
    });
    it('should reuse the default payload for the executions', function (done) {
      var client = new DseClient({ contactPoints: ['host1'], graphOptions: { name: 'name1' }});
      var optionsParameter = { anotherOption: { k: 'v2'}};
      var actualOptions = [];
      client.execute = function (query, params, options) {
        //do not use the actual object
        assert.notStrictEqual(optionsParameter, options);
        //shallow copy the properties
        assert.strictEqual(optionsParameter.anotherOption, options.anotherOption);
        assert.ok(options.customPayload);
        actualOptions.push(options);
      };
      client.executeGraph('Q5', { a: 1}, optionsParameter, helper.throwOp);
      client.executeGraph('Q6', { b: 1}, optionsParameter, helper.throwOp);
      assert.strictEqual(actualOptions.length, 2);
      assert.notStrictEqual(actualOptions[0], actualOptions[1]);
      assert.strictEqual(actualOptions[0].customPayload, actualOptions[1].customPayload);
      done();
    });
    it('should set the payload with the options provided', function (done) {
      var client = new DseClient({ contactPoints: ['host1'], graphOptions: {
        language: 'groovy2',
        source: 'another-source',
        name: 'namespace2'
      }});
      var optionsParameter = { anotherOption: { k: 'v3'}};
      client.execute = function (query, params, options) {
        //do not use the actual object
        assert.notStrictEqual(optionsParameter, options);
        //shallow copy the properties
        assert.strictEqual(optionsParameter.anotherOption, options.anotherOption);
        assert.ok(options.customPayload);
        assert.deepEqual(options.customPayload['graph-language'], new Buffer('groovy2'));
        assert.deepEqual(options.customPayload['graph-source'], new Buffer('another-source'));
        assert.deepEqual(options.customPayload['graph-name'], new Buffer('namespace2'));
        done();
      };
      client.executeGraph('Q5', { 'x': 1 }, optionsParameter, helper.throwOp);
    });
    describe('with analytics queries', function () {
      it('should query for analytics master', function (done) {
        var client = new DseClient({ contactPoints: ['host1'], graphOptions: {
          source: 'a',
          name: 'name1'
        }});
        var actualOptions;
        client.execute = function (q, p, options, cb) {
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
          assert.ok(actualOptions.preferredHost);
          assert.ok(actualOptions.preferredHost.address, '10.10.10.10:9042');
          done();
        });
      });
      it('should call address translator', function (done) {
        var translatorCalled = 0;
        var translator = new cassandra.policies.addressResolution.AddressTranslator();
        translator.translate = function (ip, port, cb) {
          translatorCalled++;
          cb(ip + ':' + port);
        };
        var client = new DseClient({ 
          contactPoints: ['host1'], 
          graphOptions: { 
            source: 'a', 
            name: 'name1'
          },
          policies: {
            addressResolution: translator
          }
        });
        var actualOptions;
        client.execute = function (q, p, options, cb) {
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
          assert.ok(actualOptions.preferredHost);
          assert.strictEqual(actualOptions.preferredHost.address, '10.10.10.10:9042');
          assert.strictEqual(translatorCalled, 1);
          done();
        });
      });
      it('should set preferredHost to null when RPC errors', function (done) {
        var client = new DseClient({ contactPoints: ['host1'], graphOptions: {
          source: 'a',
          name: 'name1'
        }});
        var actualOptions;
        client.execute = function (q, p, options, cb) {
          if (q === 'CALL DseClientTool.getAnalyticsGraphServer()') {
            return cb(new Error('Test error'));
          }
          actualOptions = options;
          cb(null, { rows: []});
        };
        client.executeGraph('g.V()', function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          assert.strictEqual(actualOptions.preferredHost, null);
          done();
        });
      });
    });
  });
});