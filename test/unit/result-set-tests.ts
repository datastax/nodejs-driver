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
import { assert } from "chai";
import sinon from "sinon";
import utils from "../../lib/utils";
import types from "../../lib/types/index";
import helper from "../test-helper";


const { ResultSet } = types;

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

  if (Symbol.asyncIterator !== undefined) {
    describe('#[@@asyncIterator]()', function () {
      it('should return the first page when pageState is not set', async () => {
        const rows = [ 100, 200, 300 ];
        const rs = new ResultSet( { rows });
        const result = await helper.asyncIteratorToArray(rs);
        assert.deepStrictEqual(result, rows);
      });

      it('should reject when nextPageAsync is not set', async () => {
        const rs = new ResultSet( { rows: [ 100 ], meta: { pageState: utils.allocBuffer(1)} });
        const iterator = rs[Symbol.asyncIterator]();
        const item = await iterator.next();
        assert.deepEqual(item, { value: 100, done: false });
        await helper.assertThrowsAsync(iterator.next(), null, 'Property nextPageAsync');
      });

      it('should return the following pages', async () => {
        const firstPageState = utils.allocBuffer(1);
        const secondPageState = utils.allocBuffer(2).fill(0xff);
        const rs = new ResultSet( { rows: [ 100, 101, 102 ], meta: { pageState: firstPageState } });
        rs.nextPageAsync = sinon.spy(function (pageState) {
          if (pageState === firstPageState) {
            return Promise.resolve({ rows: [ 200, 201 ], rawPageState: secondPageState });
          }

          return Promise.resolve({ rows: [ 300 ] });
        });

        const result = await helper.asyncIteratorToArray(rs);
        assert.strictEqual(rs.isPaged(), true);
        assert.deepEqual(result, [ 100, 101, 102, 200, 201, 300 ]);
        assert.strictEqual(rs.nextPageAsync.callCount, 2);
        assert.ok(rs.nextPageAsync.firstCall.calledWithExactly(firstPageState));
        assert.ok(rs.nextPageAsync.secondCall.calledWithExactly(secondPageState));
        // After iterating, isPaged() should continue to be true
        assert.strictEqual(rs.isPaged(), true);
      });

      it('should support next page empty', async () => {
        const pageState = utils.allocBuffer(1);
        const rs = new ResultSet( { rows: [ 100, 101, 102 ], meta: { pageState } });
        rs.nextPageAsync = sinon.spy(function () {
          return Promise.resolve({ rows: [ ], rawPageState: undefined });
        });

        const result = await helper.asyncIteratorToArray(rs);
        assert.deepEqual(result, [ 100, 101, 102 ]);
        assert.strictEqual(rs.nextPageAsync.callCount, 1);
        assert.ok(rs.nextPageAsync.firstCall.calledWithExactly(pageState));
      });

      it('should reject when nextPageAsync rejects', async () => {
        const rs = new ResultSet( { rows: [ 100 ], meta: { pageState: utils.allocBuffer(1)} });
        const error = new Error('Test dummy error');
        rs.nextPageAsync = sinon.spy(function () {
          return Promise.reject(error);
        });
        const iterator = rs[Symbol.asyncIterator]();
        const item = await iterator.next();
        assert.deepEqual(item, { value: 100, done: false });
        await helper.assertThrowsAsync(iterator.next(), null, error.message);
        assert.strictEqual(rs.nextPageAsync.callCount, 1);
      });
    });

    describe('#isPaged()', () => {
      it('should return false when page state is not defined', () => {
        const rs = new ResultSet( { rows: [ 100 ] });
        assert.strictEqual(rs.isPaged(), false);
      });

      it('should return false when page state is undefined', () => {
        const rs = new ResultSet( { rows: [ 100 ], meta: { pageState: undefined } });
        assert.strictEqual(rs.isPaged(), false);
      });

      it('should return true when page state is set', () => {
        const rs = new ResultSet( { rows: [ 100 ], meta: { pageState: utils.allocBuffer(1) } });
        assert.strictEqual(rs.isPaged(), true);
      });
    });
  }
});