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

"use strict";

const assert = require('assert');
const types = require('../../lib/types');
const ResultSet = types.ResultSet;

describe('ResultSet', function () {
  describe('constructor', function () {
    it('should set the properties', function () {
      const response = { rows: [ 0, 1, 2 ] };
      const result = new ResultSet(response, '192.168.1.100', {}, 1, types.consistencies.three, false);
      assert.strictEqual(result.rowLength, 3);
      assert.ok(result.info);
      assert.strictEqual(result.info.queriedHost, '192.168.1.100');
      assert.strictEqual(result.info.achievedConsistency, types.consistencies.three);
      assert.strictEqual(result.info.speculativeExecutions, 1);
      assert.strictEqual(result.info.isSchemaInAgreement, false);
      assert.strictEqual(result.rows, response.rows);

      assert.strictEqual(new ResultSet({ rowLength: 12 }).rowLength, 12);

      assert.strictEqual(
        new ResultSet({ rowLength: 0 }, '10.10.10.10', null, 1, 1, true).info.isSchemaInAgreement,
        true);
    });
  });
  describe('#first()', function () {
    it('should return the first row', function () {
      const result = new ResultSet({ rows: [ 400, 420 ] }, null);
      assert.strictEqual(result.first(), 400);
    });
    it('should return null when rows is not defined', function () {
      const result = new ResultSet({ }, null);
      assert.strictEqual(result.first(), null);
    });
  });
  describe('#[@@iterator]()', function () {
    it('should return the rows iterator', function () {
      const result = new ResultSet({ rows: [ 100, 200, 300] }, null);
      // Equivalent of for..of result
      const iterator = result[Symbol.iterator]();
      let item = iterator.next();
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
      const result = new ResultSet({ }, null);
      // Equivalent of for..of result
      const iterator = result[Symbol.iterator]();
      const item = iterator.next();
      assert.ok(item);
      assert.strictEqual(item.done, true);
      assert.strictEqual(item.value, undefined);
    });
  });
});