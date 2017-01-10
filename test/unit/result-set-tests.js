"use strict";

var assert = require('assert');
var helper = require('../test-helper');
var types = require('../../lib/types');
var ResultSet = types.ResultSet;

describe('ResultSet', function () {
  describe('constructor', function () {
    it('should set the properties', function () {
      var response = { rows: [ 0, 1, 2 ] };
      var result = new ResultSet(response, '192.168.1.100', {}, types.consistencies.three);
      assert.strictEqual(result.rowLength, 3);
      assert.ok(result.info);
      assert.strictEqual(result.info.queriedHost, '192.168.1.100');
      assert.strictEqual(result.info.achievedConsistency, types.consistencies.three);
      assert.strictEqual(result.rows, response.rows);
      assert.strictEqual(new ResultSet({ rowLength: 12 }).rowLength, 12);
    });
  });
  describe('#first()', function () {
    it('should return the first row', function () {
      var result = new ResultSet({ rows: [ 400, 420 ] }, null);
      assert.strictEqual(result.first(), 400);
    });
    it('should return null when rows is not defined', function () {
      var result = new ResultSet({ }, null);
      assert.strictEqual(result.first(), null);
    });
  });
  if (helper.iteratorSupport) {
    describe('#[@@iterator]()', function () {
      it('should return the rows iterator', function () {
        var result = new ResultSet({ rows: [ 100, 200, 300] }, null);
        // Equivalent of for..of result
        var iterator = result[Symbol.iterator]();
        var item = iterator.next();
        assert.strictEqual(item.done, false);
        assert.strictEqual(item.value, 100);
        item = iterator.next();
        assert.strictEqual(item.done, false);
        assert.strictEqual(item.value, 200);
        item = iterator.next();
        assert.strictEqual(item.done, false);
        assert.strictEqual(item.value, 300);
        assert.strictEqual(iterator.next().done, true);
      });
      it('should return an empty iterator when rows is not defined', function () {
        var result = new ResultSet({ }, null);
        // Equivalent of for..of result
        var iterator = result[Symbol.iterator]();
        var item = iterator.next();
        assert.ok(item);
        assert.strictEqual(item.done, true);
        assert.strictEqual(item.value, undefined);
      });
    });
  }
});