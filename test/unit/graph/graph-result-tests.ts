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
import assert from "assert";
import utils from "../../../lib/utils";
import ResultSet from "../../../lib/types/result-set";
import GraphResultSet from "../../../lib/datastax/graph/result-set";

'use strict';
const resultVertex = getResultSet([ {
  "gremlin": JSON.stringify({
    "result": {
      "id":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2},
      "label":"vertex",
      "type":"vertex",
      "properties":{
        "name":[{"id":{"local_id":"00000000-0000-8007-0000-000000000000","~type":"name","out_vertex":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2}},"value":"j"}],
        "age":[{"id":{"local_id":"00000000-0000-8008-0000-000000000000","~type":"age","out_vertex":{"member_id":0,"community_id":586910,"~label":"vertex","group_id":2}},"value":34}]}
    }})
}]);
const resultEdge = getResultSet([ {
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
}]);
const resultScalars = getResultSet([
  { gremlin: JSON.stringify({ result: 'a'})},
  { gremlin: JSON.stringify({ result: 'b'})}
]);
const resultScalarsBulked1 = getResultSet([
  { gremlin: JSON.stringify({ result: 'a', bulk: 1 })},
  { gremlin: JSON.stringify({ result: 'b', bulk: 2 })},
  { gremlin: JSON.stringify({ result: 'c', bulk: 3 })},
]);
const resultScalarsBulked2 = getResultSet([
  { gremlin: JSON.stringify({ result: 'a', bulk: 3 })},
  { gremlin: JSON.stringify({ result: 'b', bulk: 2 })},
  { gremlin: JSON.stringify({ result: 'c', bulk: 1 })},
]);

describe('GraphResultSet', function () {
  describe('#toArray()', function () {
    it('should return an Array with parsed values', function () {
      const result = new GraphResultSet(resultVertex);
      const arr = result.toArray();
      assert.strictEqual(arr.length, 1);
      assert.strictEqual(arr[0].type, 'vertex');
    });
  });
  describe('#forEach()', function () {
    it('should execute callback per each value', function () {
      const indexes = [];
      const values = [];
      const result = new GraphResultSet(resultScalars);
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
      const result = new GraphResultSet(resultScalars);
      const iterator = result.values();
      let item = iterator.next();
      assert.strictEqual(item.value, 'a');
      assert.strictEqual(item.done, false);
      item = iterator.next();
      assert.strictEqual(item.value, 'b');
      assert.strictEqual(item.done, false);
      item = iterator.next();
      assert.strictEqual(typeof item.value, 'undefined');
      assert.strictEqual(item.done, true);
    });
    it('should return a iterator with no items when result set is empty', function () {
      const result = new GraphResultSet(getResultSet([]));
      const iterator = result.values();
      const item = iterator.next();
      assert.strictEqual(typeof item.value, 'undefined');
      assert.strictEqual(item.done, true);
    });
    it('should parse bulked results', function () {
      const result1 = new GraphResultSet(resultScalarsBulked1);
      assert.deepEqual(utils.iteratorToArray(result1.values()), [ 'a', 'b', 'b', 'c', 'c', 'c']);
      const result2 = new GraphResultSet(resultScalarsBulked2);
      assert.deepEqual(utils.iteratorToArray(result2.values()), [ 'a', 'a', 'a', 'b', 'b', 'c']);
    });
  });
  //noinspection JSUnresolvedVariable
  if (typeof Symbol !== 'undefined' && typeof Symbol.iterator === 'symbol') {
    describe('@@iterator', function () {
      it('should be iterable', function () {
        const result = new GraphResultSet(resultEdge);
        //equivalent of for..of result
        //noinspection JSUnresolvedVariable
        const iterator = result[Symbol.iterator]();
        assert.ok(iterator);
        let item = iterator.next();
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

/**
 * @param {Array} rows
 * @returns {ResultSet}
 */
function getResultSet(rows) {
  return new ResultSet({ rows: rows }, null, null, null);
}