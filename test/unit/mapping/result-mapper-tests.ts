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

const { assert } = require('chai');
const sinon = require('sinon');

const ResultMapper = require('../../../lib/mapping/result-mapper');

describe('ResultMapper', function () {
  describe('getSelectAdapter()', function () {
    it('should return a function that maps row values into object values', () => {
      const suffix = 'Prop';
      const rs = { columns: [{ name: 'col1' }, { name: 'col2' }] };
      const info = sinon.spy({
        newInstance: () => ({}),
        getPropertyName: name => name + suffix,
        getToModelFn: () => null
      });

      const fn = ResultMapper.getSelectAdapter(info, rs);
      assert.isFunction(fn);
      assert.equal(info.newInstance.callCount, 0);
      assert.equal(info.getPropertyName.callCount, rs.columns.length);

      // Two parameters: function rowAdapter(row, info) {}
      assert.equal(fn.length, 2);
      const row = { 'col1': 'a', 'col2': {} };

      // It should changed the name of the columns into props
      const obj = fn(row, info);
      assert.strictEqual(obj['col1' + suffix], row.col1);
      assert.strictEqual(obj['col2' + suffix], row.col2);
      // Should have called newInstance()
      assert.equal(info.newInstance.callCount, 1);
    });

    it('should use user defined mapping functions ', () => {
      const suffixProperty = 'Prop';
      const suffixValue = '_value';
      const rs = { columns: [{ name: 'col1' }, { name: 'col2' }] };
      const info = sinon.spy({
        newInstance: () => ({}),
        getPropertyName: name => name + suffixProperty,
        getToModelFn: columnName => (columnName === 'col2' ? (v => v + suffixValue) : null)
      });

      const fn = ResultMapper.getSelectAdapter(info, rs);
      const row = { 'col1': 'a', 'col2': 'b' };

      const obj = fn(row, info);
      // Mapping function adds a suffix
      assert.strictEqual(obj['col2' + suffixProperty], row.col2 + suffixValue);
      // No mapping function
      assert.strictEqual(obj['col1' + suffixProperty], row.col1);
    });
  });
});