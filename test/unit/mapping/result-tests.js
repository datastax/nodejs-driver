/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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