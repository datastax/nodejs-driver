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
const q = require('../../../lib/mapping/q').q;
const dataTypes = require('../../../lib/types').dataTypes;
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');

const emptyResponse = { meta: { columns: [] }, rows: [] };

describe('ModelMapper', () => {
  describe('#find()', () => {
    mapperTestHelper.testParameters('find', 'select');

    it('should support limit', () => testQueries('find', [
      {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { limit: 100 },
        query: 'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ? LIMIT ?',
        params: [ 'value1', 'e', 100 ]
      }]));

    it('should support fields to specify the selection columns', () => testQueries('find', [
      {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { fields: ['name', 'description', 'locationType'] },
        query: 'SELECT name, description, location_type FROM ks1.table1 WHERE id1 = ? AND id2 >= ?',
        params: [ 'value1', 'e' ]
      }]));

    it('should support relational operators, orderBy, limit and fields', () => testQueries('find', [
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

    it('should support orderBy in correct order', () => testQueries('find', [
      {
        doc: { id1: 'value2' },
        docInfo: { orderBy: {'id2': 'desc' }},
        query:
          'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY id2 DESC',
        params: [ 'value2' ]
      }, {
        doc: { id1: 'value3' },
        docInfo: { orderBy: {'id2': 'asc' }},
        query:
          'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY id2 ASC',
        params: [ 'value3' ]
      }, {
        doc: { id1: 'value1', id2: q.gte('e') },
        docInfo: { fields: ['name'], limit: 20, orderBy: {'id2': 'asc' }},
        query:
          'SELECT name FROM ks1.table1 WHERE id1 = ? AND id2 >= ? ORDER BY id2 ASC LIMIT ?',
        params: [ 'value1', 'e', 20 ]
      }]));

    it('should support ordering by clustering key only providing a partition key', () => testQueries('find', [
      {
        doc: { id1: 'value2' },
        docInfo: { orderBy: {'id2': 'asc' }},
        query:
          'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY id2 ASC',
        params: [ 'value2' ]
      }]));

    it('should support model mapping functions', () => {
      // Create a closure
      const suffix = '_after_function';

      return testQueries({
        methodName: 'find',
        models: {
          'Sample': {
            tables: ['table1'],
            columns: {
              'id2': { fromModel: a => a + suffix },
              'location_type': 'locationType'
            }
          }
        },
        items: [
          {
            doc: { id1: 'value_id1', id2: 'value_id2' },
            query:
              'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 = ?',
            params: [ 'value_id1', 'value_id2' + suffix ]
          },
          {
            doc: { id1: 'value_id1', id2: q.gt('value_id2') },
            query:
              'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 > ?',
            params: [ 'value_id1', 'value_id2' + suffix ]
          },
          {
            doc: { id1: 'value_id1', id2: q.and(q.gte('a'), q.lt('z')) },
            query:
              'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 >= ? AND id2 < ?',
            params: [ 'value_id1', 'a' + suffix, 'z' + suffix ]
          }
        ]
      });
    });

    it('should support mapping IN values with a mapping function', () => testQueries({
      methodName: 'find',
      models: {
        'Sample': {
          tables: [ 'table1' ],
          columns: {
            'id2': { fromModel: a => a + '_mapped_value' }
          }
        }
      },
      items: [
        {
          doc: { id1: 'value_id1', id2: q.in_(['first', 'second']) },
          query:
            'SELECT * FROM ks1.table1 WHERE id1 = ? AND id2 IN ?',
          params: [ 'value_id1', ['first_mapped_value', 'second_mapped_value'] ]
        }
      ]
    }));

    it('should throw an error when filter, fields or orderBy are not valid', () =>
      Promise.all([
        {
          doc: { id1: 'x', notAValidProp: 'y' },
          message: 'No table matches the filter (PKs): [id1,notAValidProp]'
        }, {
          doc: { id1: 'x'},
          docInfo: { fields: ['notAValidProp'] },
          message: 'No table matches the filter (PKs): [id1]; fields: [notAValidProp]'
        }, {
          doc: { id1: 'x', name: 'y' },
          message: 'No table matches the filter (PKs): [id1,name]'
        }, {
          doc: { id1: 'x'},
          docInfo: { orderBy: { 'notAValidProp': 'asc' } },
          message: 'No table matches the filter (PKs): [id1]; orderBy: [notAValidProp]'
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

    it('should use the default table name based on the model name', () => {
      const columns = [ 'id1', 'id2', 'name'];
      const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1', emptyResponse);
      const mapper = mapperTestHelper.getMapper(clientInfo);
      const modelMapper = mapper.forModel('NoTableSpecified');

      return modelMapper.find({ id1: 'x', id2: 'y'}).then(() => {
        const execution = clientInfo.executions[0];
        assert.strictEqual(execution.query, 'SELECT * FROM ks1.NoTableSpecified WHERE id1 = ? AND id2 = ?');
      });
    });

    it('should use the correct table when order by a clustering key', () => {
      // Mock table metadata
      function getTableMetadata(ks, name) {
        // Equivalent of
        // CREATE TABLE table1 (id1 text, id2 text, id3 text, PRIMARY KEY (id1, id2, id3))
        // CREATE TABLE table2 (id1 text, id2 text, id3 text, PRIMARY KEY (id1, id3, id2))
        const columns = [ 'id1', 'id2', 'id3', 'value' ].map(c => ({ name: c, type: { code: dataTypes.text }}));
        const partitionKeys = [ columns[0] ];
        const clusteringKeys = name === 'table1' ? [ columns[1], columns[2] ] : [ columns[2], columns[1] ];

        const table = { name, partitionKeys, clusteringKeys, columnsByName: {}, columns };
        table.columns.forEach(c => table.columnsByName[c.name] = c);
        return Promise.resolve(table);
      }

      const models = { 'Sample': { tables: [ 'table1', 'table2' ] } };

      const clientInfo = mapperTestHelper.getClient(getTableMetadata, null, 'ks1', emptyResponse);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo, models);

      return modelMapper
        .find({ id1: 'a' }, { orderBy: { 'id3': 'asc' }})
        .then(() => modelMapper.find({ id1: 'a' }, { orderBy: { 'id2': 'desc', 'id3': 'desc' }}))
        .then(() => modelMapper.find({ id1: 'a' }, { orderBy: { 'id2': 'asc' }}))
        .then(() => {
          [
            // Selected "table2" for the first query
            'SELECT * FROM ks1.table2 WHERE id1 = ? ORDER BY id3 ASC',
            // Selected "table1" for the second query
            'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY id2 DESC, id3 DESC',
            // Selected "table1" for the third query
            'SELECT * FROM ks1.table1 WHERE id1 = ? ORDER BY id2 ASC'
          ].forEach((query, index) => {
            assert.strictEqual(clientInfo.executions[index].query, query);
            assert.deepStrictEqual(clientInfo.executions[index].params, [ 'a' ]);
          });
        });
    });
  });

  describe('#get()', () => {

    const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ], 'ks1', emptyResponse);
    const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

    mapperTestHelper.testParameters('get', 'select');

    it('should only execute the query if all PKs are defined', () => {
      let catchCalled = false;

      return modelMapper.get({ id1: 'abc' })
        .catch(err => {
          catchCalled = true;
          helper.assertInstanceOf(err, Error);
          assert.strictEqual(err.message, 'No table matches the filter (all PKs have to be specified): [id1]');
        })
        .then(() => assert.strictEqual(catchCalled, true));
    });
  });

  describe('#findAll()', () => {

    it('should support limit', () => testQueries('findAll', [
      {
        docInfo: { limit: 100 },
        query: 'SELECT * FROM ks1.table1 LIMIT ?',
        params: [ 100 ]
      }]));

    it('should support fields to specify the selection columns', () => testQueries('findAll', [
      {
        docInfo: { fields: ['name', 'description', 'locationType'] },
        query: 'SELECT name, description, location_type FROM ks1.table1',
        params: []
      }]));
  });

  describe('#mapWithQuery', () => {
    it('should warn when cache reaches 100 different queries', () => {
      const clientInfo = mapperTestHelper.getClient(['id1'], [ 1 ], 'ks1', emptyResponse);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

      const cacheHighWaterMark = 100;
      const promises = [];

      for (let i = 0; i < 2 * cacheHighWaterMark; i++) {
        const executor = modelMapper.mapWithQuery(`query-${i % (cacheHighWaterMark - 1)}`, () => []);
        promises.push(executor());
      }

      return Promise.all(promises)
        // No warnings logged when there are 99 different queries
        .then(() => assert.strictEqual(clientInfo.logMessages.length, 0))
        // One more query
        .then(() => modelMapper.mapWithQuery(`query-limit`, () => [])())
        .then(() => {
          assert.strictEqual(clientInfo.logMessages.length, 1);
          assert.strictEqual(clientInfo.logMessages[0].level, 'warning');
          assert.strictEqual(clientInfo.logMessages[0].message,
            `Custom queries cache reached ${cacheHighWaterMark} items, this could be caused by ` +
            `hard-coding parameter values inside the query, which should be avoided`);
        });
    });
  });
});

async function testQueries(methodName, items) {
  let models = null;
  const columns = [ 'id1', 'id2', 'name', 'description', 'location_type'];

  if (typeof methodName === 'object') {
    // Its an object with properties as parameters
    models = methodName.models;
    items = methodName.items;
    methodName = methodName.methodName;
  }

  const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1', emptyResponse);
  const modelMapper = mapperTestHelper.getModelMapper(clientInfo, models);

  for (const item of items) {
    if (methodName !== 'findAll') {
      await modelMapper[methodName](item.doc, item.docInfo, item.executionOptions);
    } else {
      await modelMapper[methodName](item.docInfo, item.executionOptions);
    }

    const execution = clientInfo.executions.pop();
    assert.strictEqual(execution.query, item.query);
    assert.deepStrictEqual(execution.params, item.params);
    helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
  }
}