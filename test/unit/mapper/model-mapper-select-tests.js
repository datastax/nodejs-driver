'use strict';

const assert = require('assert');
const q = require('../../../lib/mapper/q').q;
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');

const emptyResponse = { meta: { columns: [] }, rows: [] };

describe('ModelMapper', () => {
  describe('#find()', () => {
    mapperTestHelper.testParameters('find', 'select');

    it('should support limit', () => testQueries([
      {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { limit: 100 },
        query: 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ? LIMIT ?',
        params: [ 'value1', 'e', 100 ]
      }]));

    it('should support fields to specify the selection columns', () => testQueries([
      {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { fields: ['name', 'description', 'locationType'] },
        query: 'SELECT name, description, location_type FROM ks1.table1 WHERE id1 = ? AND id2 >= ?',
        params: [ 'value1', 'e' ]
      }]));

    it('should support relational operators, orderBy, limit and fields', () => testQueries([
      {
        doc: {id1: 'value1', id2: q.gte('m')},
        query: 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ?',
        params: ['value1', 'm']
      }, {
        doc: {id1: 'value1', id2: q.and(q.gte('a'), q.lt('z'))},
        query: 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ? AND id2 < ?',
        params: ['value1', 'a', 'z']
      }, {
        doc: {id1: 'value1', id2: q.and(q.gte('a'), q.and(q.gte('e'), q.lt('z')))},
        query: 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ? AND id2 >= ? AND id2 < ?',
        params: ['value1', 'a', 'e', 'z']
      }]));

    it('should support orderBy in correct order', () => testQueries([
      {
        doc: { id1: 'value2' },
        docInfo: { orderBy: {'locationType': 'desc' }},
        query:
          'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY location_type DESC',
        params: [ 'value2' ]
      }, {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { fields: ['name'], limit: 20, orderBy: {'description': 'asc', 'locationType': 'desc' }},
        query:
          'SELECT name FROM ks1.table1 WHERE id1 = ? AND id2 >= ? ORDER BY description ASC, location_type DESC LIMIT ?',
        params: [ 'value1', 'e', 20 ]
      }]));

    it('should throw an error when filter, fields or orderBy are not valid', () =>
      Promise.all([
        {
          doc: { id1: 'x', notAValidProp: 'y' },
          message: 'No table matches the columns filter (PKs): [id1,notAValidProp]'
        }, {
          doc: { id1: 'x'},
          docInfo: { fields: ['notAValidProp'] },
          message: 'No table matches the columns filter (PKs): [id1]; fields: [notAValidProp]'
        }, {
          doc: { id1: 'x', name: 'y' },
          message: 'No table matches the columns filter (PKs): [id1,name]'
        }, {
          doc: { id1: 'x'},
          docInfo: { orderBy: { 'notAValidProp': 'asc' } },
          message: 'No table matches the columns filter (PKs): [id1]; orderBy: [notAValidProp]'
        }
      ].map(item => {
        const columns = [ 'id1', 'id2', 'name'];
        const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1', emptyResponse);
        const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

        let catchCalled = false;

        return modelMapper.find(item.doc, item.docInfo)
          .catch(err => {
            catchCalled = true;
            helper.assertInstanceOf(err, Error);
            assert.strictEqual(err.message, item.message);
          })
          .then(() => assert.strictEqual(catchCalled, true));
      }))
    );

    it('should use the initial keyspace', () => {
      const columns = [ 'id1', 'id2', 'name'];
      const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1', emptyResponse);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

      // Switch keyspace after Mapper instance is created
      clientInfo.client.keyspace = 'ks2';

      return modelMapper.find({ id1: 'x', id2: 'y'}).then(() => {
        const execution = clientInfo.executions[0];
        assert.strictEqual(execution.query, 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 = ?');
      });
    });
  });

  describe('#get()', () => {
    mapperTestHelper.testParameters('get', 'select');
  });
});

function testQueries(items) {
  const columns = [ 'id1', 'id2', 'name', 'description', 'location_type'];
  const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1', emptyResponse);
  const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

  return Promise.all(items.map((item, index) => modelMapper.find(item.doc, item.docInfo).then(() => {
    assert.strictEqual(clientInfo.executions.length, items.length);
    const execution = clientInfo.executions[index];
    assert.strictEqual(execution.query, item.query);
    assert.deepStrictEqual(execution.params, item.params);
    helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
  })));
}