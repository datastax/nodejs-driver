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
const tableMappingsModule = require('../../../lib/mapping/table-mappings');
const UnderscoreCqlToCamelCaseMappings = tableMappingsModule.UnderscoreCqlToCamelCaseMappings;

describe('UnderscoreCqlToCamelCaseMappings', () => {
  const instance = new UnderscoreCqlToCamelCaseMappings();

  describe('#getPropertyName()', () => {
    it('should convert to camel case', () => {
      [
        [ 'my_property_name', 'myPropertyName' ],
        [ '_my_property_name', '_myPropertyName' ],
        [ 'my__property_name', 'my_PropertyName' ],
        [ 'abc', 'abc' ],
        [ 'ABC', 'ABC' ]
      ].forEach(item => assert.strictEqual(instance.getPropertyName(item[0]), item[1]));
    });
  });

  describe('#getColumnName()', () => {
    it('should convert to snake case', () => {
      [
        [ 'myPropertyName', 'my_property_name' ],
        [ '_myPropertyName', '_my_property_name' ],
        [ 'my_PropertyName', 'my_property_name'],
        [ 'abc', 'abc' ],
        [ 'ABC', 'abc' ]
      ].forEach(item => assert.strictEqual(instance.getColumnName(item[0]), item[1]));
    });
  });
});