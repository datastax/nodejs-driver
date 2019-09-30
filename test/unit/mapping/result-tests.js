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
const Result = require('../../../lib/mapping/result');
const util = require('util');

const expected = [ { id: 1, name: 'name1', adapted: true }, { id: 2, name: 'name2', adapted: true }];

describe('Result', () => {
  describe('#toArray()', () => {
    it('should map to an array', () => {
      const result = getResult();
      const arr = result.toArray();
      assert.deepStrictEqual(arr, expected);
    });

    it('should return empty array when is a VOID result', () => {
      const result = new Result({ columns: [], rows: undefined, rowLength: undefined }, null, () => {});
      assert.deepStrictEqual(result.toArray(), []);
    });
  });

  describe('#forEach()', () => {
    it('should invoke the callback per each row', () => {
      const arr = new Array(2);
      getResult().forEach((item, index) => arr[index] = item);
      assert.deepStrictEqual(arr, expected);
    });
  });

  describe('@@iterator', () => {
    it('should support iteration', () => {
      assert.strictEqual(typeof Result.prototype[Symbol.iterator], 'function');
      const arr = [];
      for (const item of getResult()) {
        arr.push(item);
      }
      assert.deepStrictEqual(arr, expected);
    });
  });

  describe('[util.inspect.custom]()', () => {
    it('should provide the array representation', () => {
      const result = getResult();
      assert.strictEqual(util.inspect(result), util.inspect(expected));
      assert.strictEqual(util.inspect(result), util.inspect(result.toArray()));
    });

    it('should return empty array representation when is a VOID result', () => {
      const result = new Result({ columns: [], rows: undefined, rowLength: undefined }, null, () => {});
      assert.strictEqual(util.inspect(result), util.inspect([]));
    });

    it('should return empty array representation when is LWT result', () => {
      const result = new Result({ columns: [ { name: '[applied]' }], rows: [{ }], rowLength: 1 }, null, () => {});
      assert.strictEqual(util.inspect(result), util.inspect([]));
    });
  });
});

function getResult(columns, rows, rowAdapter) {
  rowAdapter = rowAdapter || (row => {
    row['adapted'] = true;
    return row;
  });

  if (!columns) {
    columns = [ 'id', 'name' ];
  }

  columns = columns.map(name => ({ name }));

  if (!rows) {
    rows = [ [ 1, 'name1' ], [ 2, 'name2' ] ];
  }

  rows = rows.map(item => {
    const row = {};
    for (let i = 0; i < columns.length; i++) {
      row[columns[i].name] = item[i];
    }
    return row;
  });

  return new Result({ columns, rows, rowLength: rows.length}, null, rowAdapter);
}