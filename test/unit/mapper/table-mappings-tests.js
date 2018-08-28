'use strict';

const assert = require('assert');
const tableMappingsModule = require('../../../lib/mapper/table-mappings');
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