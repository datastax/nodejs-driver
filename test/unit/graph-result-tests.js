/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var cassandra = require('cassandra-driver');
var helper = require('../helper');
var GraphResultSet = require('../../lib/graph/result-set');

var resultVertex = {
  rows: [ {
    "gremlin": JSON.stringify({
      "result": {
        "id":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2},
        "label":"vertex",
        "type":"vertex",
        "properties":{
          "name":[{"id":{"local_id":"00000000-0000-8007-0000-000000000000","~type":"name","out_vertex":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2}},"value":"j"}],
          "age":[{"id":{"local_id":"00000000-0000-8008-0000-000000000000","~type":"age","out_vertex":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2}},"value":34}]}
      }})
  }]
};
var resultEdge = {
  rows: [ {
    "gremlin": JSON.stringify({
      "result":{
        "id":{
          "out_vertex":{"member_id":0,"community_id":680148,"~label":"vertex","group_id":3},
          "local_id":"4e78f871-c5c8-11e5-a449-130aecf8e504","in_vertex":{"member_id":0,"community_id":680148,"~label":"vertex","group_id":5},"~type":"knows"},
        "label":"knows",
        "type":"edge",
        "inVLabel":"vertex",
        "outVLabel":"vertex",
        "inV":{"member_id":0,"community_id":680148,"~label":"vertex","group_id":5},
        "outV":{"member_id":0,"community_id":680148,"~label":"vertex","group_id":3},
        "properties":{"weight":1.0}
      }})
  }]
};
var resultScalars = {
  rows: [
    { gremlin: JSON.stringify({ result: 'a'})},
    { gremlin: JSON.stringify({ result: 'b'})}
  ]
};

describe('GraphResultSet', function () {
  describe('#toArray()', function () {
    it('should return an Array with parsed values', function () {
      var result = new GraphResultSet(resultVertex);
      var arr = result.toArray();
      assert.strictEqual(arr.length, 1);
      assert.strictEqual(arr[0].type, 'vertex');
    });
  });
  describe('#forEach()', function () {
    it('should execute callback per each value', function () {
      var indexes = [];
      var values = [];
      var result = new GraphResultSet(resultScalars);
      result.forEach(function (val, i) {
        values.push(val);
        indexes.push(i);
      });
      assert.deepEqual(values, ['a', 'b']);
      assert.deepEqual(indexes, [0, 1]);
    });
  });
  describe('#values()', function () {
    it('should return an iterator', function () {
      var result = new GraphResultSet(resultScalars);
      var iterator = result.values();
      var item = iterator.next();
      assert.strictEqual(item.value, 'a');
      assert.strictEqual(item.done, false);
      item = iterator.next();
      assert.strictEqual(item.value, 'b');
      assert.strictEqual(item.done, false);
      item = iterator.next();
      assert.strictEqual(typeof item.value, 'undefined');
      assert.strictEqual(item.done, true);
    });
  });
  //noinspection JSUnresolvedVariable
  if (typeof Symbol !== 'undefined' && typeof Symbol.iterator === 'symbol') {
    console.log('hello symbol!');
    describe('@@iterator', function () {
      it('should be iterable', function () {
        var result = new GraphResultSet(resultEdge);
        //equivalent of for..of result
        //noinspection JSUnresolvedVariable
        var iterator = result[Symbol.iterator]();
        assert.ok(iterator);
        var item = iterator.next();
        assert.ok(item.value);
        assert.strictEqual(item.value.type, 'edge');
        assert.strictEqual(item.done, false);
        item = iterator.next();
        assert.strictEqual(typeof item.value, 'undefined');
        assert.strictEqual(item.done, true);
      });
    });
  }
});