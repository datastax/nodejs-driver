'use strict';
var assert = require('assert');
var cassandra = require('cassandra-driver');
var DseClient = require('../../lib/dse-client');
var helper = require('../helper');

describe('DseClient', function () {
  describe('constructor', function () {
    it('should validate options', function () {
      assert.throws(function () {
        new DseClient();
      }, cassandra.errors.ArgumentError);
    });
  });
  describe('#executeGraph()', function () {
    it('should not allow a namespace of type different than string', function () {
      var client = new DseClient({ contactPoints: ['host1']});
      client.execute = helper.noop;
      assert.doesNotThrow(function () {
        client.executeGraph('Q1', [], { graphName: 'abc' }, helper.noop)
      });
      assert.throws(function () {
        //noinspection JSCheckFunctionSignatures
        client.executeGraph('Q1', [], { graphName: 123 }, helper.noop)
      }, TypeError);
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
    it('should set the default payload for the executions', function (done) {
      var client = new DseClient({ contactPoints: ['host1'], graphOptions: { name: 'name1' }});
      var optionsParameter = { anotherOption: { k: 'v'}};
      client.execute = function (query, params, options) {
        //do not use the actual object
        assert.notStrictEqual(optionsParameter, options);
        //shallow copy the properties
        assert.strictEqual(optionsParameter.anotherOption, options.anotherOption);
        assert.ok(options.customPayload);
        assert.deepEqual(options.customPayload['graph-language'], new Buffer('gremlin-groovy'));
        assert.deepEqual(options.customPayload['graph-source'], new Buffer('default'));
        assert.deepEqual(options.customPayload['graph-name'], new Buffer('name1'));
        done();
      };
      client.executeGraph('Q5', { c: 0}, optionsParameter, helper.throwOp);
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
  });
});