'use strict';

const assert = require('assert');
const q = require('../../../lib/mapper/q').q;
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');

const emptyResponse = { meta: { columns: [] }, rows: [] };

describe('ModelMapper', () => {
  describe('#find()', () => {
    mapperTestHelper.testParameters('find', 'select');

    it('should support specifying fields');

    it('should support limit');

    it('should support relational operators', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ], 'ks1', emptyResponse);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const items = [
        {
          doc: { id1: 'value1', id2: q.gte('m') },
          query: 'SELECT * FROM table1 WHERE id1 = ? AND id2 >= ?'
        }];

      return Promise.all(items.map((item, index) => modelMapper.find(item.doc).then(() => {
        assert.strictEqual(clientInfo.executions.length, items.length);
        const execution = clientInfo.executions[index];
        assert.strictEqual(execution.query, item.query);
        assert.deepStrictEqual(execution.params, [ item.doc.id1, item.doc.id2.value ]);
        helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
      })));
    });
  });

  describe('#get()', () => {
    mapperTestHelper.testParameters('get', 'select');
  });
});