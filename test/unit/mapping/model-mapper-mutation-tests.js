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
const types = require('../../../lib/types');
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');
const dataTypes = types.dataTypes;

describe('ModelMapper', () => {
  describe('#insert()', () => {
    mapperTestHelper.testParameters('insert');

    it('should retrieve the table that apply and make a single execution', () => {
      const clientInfo = mapperTestHelper.getClient([ 'partition1', 'clustering1', 'name'], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const doc = { clustering1: 'value2' , partition1: 'value1' };

      return modelMapper.insert(doc)
        .then(() => {
          assert.strictEqual(clientInfo.executions.length, 1);
          const execution = clientInfo.executions[0];
          assert.strictEqual(execution.query, 'INSERT INTO ks1.table1 ("clustering1", "partition1") VALUES (?, ?)');
          assert.deepStrictEqual(execution.params, Object.keys(doc).map(key => doc[key]));
          helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
        });
    });

    it('should mark LWT queries as non-idempotent', () => testQueries('insert', [
      {
        doc: { clustering1: 'value2' , partition1: 'value1', name: 'name1' },
        docInfo: { ifNotExists: true },
        query: 'INSERT INTO ks1.table1 ("clustering1", "partition1", "name") VALUES (?, ?, ?) IF NOT EXISTS',
        params: [ 'value2', 'value1', 'name1' ],
        isIdempotent: false
      }
    ]));

    it('should set TTL', () => testQueries('insert', [
      {
        doc: { clustering1: 'value2' , partition1: 'value1', name: 'name1' },
        docInfo: { ttl: 1000 },
        query: 'INSERT INTO ks1.table1 ("clustering1", "partition1", "name") VALUES (?, ?, ?) USING TTL ?',
        params: [ 'value2', 'value1', 'name1', 1000 ]
      }
    ]));

    it('should throw an error when the table metadata retrieval fails', () => {
      const error = new Error('test error');
      const client = {
        connect: () => Promise.resolve(),
        keyspace: 'ks1',
        log: () => {},
        metadata: {
          getTable: () => Promise.reject(error)
        }
      };
      const modelMapper = mapperTestHelper.getModelMapper({ client });

      let catchCalled = false;

      return modelMapper.insert({ partition1: 'value1' })
        .catch(err => {
          catchCalled = true;
          assert.strictEqual(err, error);
        })
        .then(() => assert.strictEqual(catchCalled, true));
    });

    it('should throw an error when filter or conditions are not valid', () => testErrors('insert', [
      {
        doc: { partition1: 'x', notAValidProp: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [partition1,notAValidProp]'
      }, {
        doc: { partition1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (all PKs have to be specified) fields: [notAValidProp]'
      }, {
        doc: { partition1: 'x', name: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [partition1,name]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }
    ]));

    it('should warn when cache reaches 100 different queries', async () => {
      const clientInfo = mapperTestHelper.getClient(['partition1'], [ 1 ], 'ks1');
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

      const cacheHighWaterMark = 100;
      const promises = [];

      for (let i = 0; i < 2 * cacheHighWaterMark; i++) {
        promises.push(modelMapper.insert({ partition1: 1, [`col${i % (cacheHighWaterMark-1)}`]: 1}));
      }

      await Promise.all(promises);

      // No warnings logged when there are 99 different queries
      assert.strictEqual(clientInfo.logMessages.filter(l => l.level === 'warning').length, 0);

      // One more query
      await modelMapper.insert({ partition1: 1, anotherColumn: 1 });

      const warnings = clientInfo.logMessages.filter(l => l.level === 'warning');
      assert.strictEqual(warnings.length, 1);
      helper.assertContains(warnings[0].message, `ModelMapper cache reached ${cacheHighWaterMark}`);
    });

    it('should use the mapping function in the insert values', () => testQueries({
      methodName: 'insert',
      models: {
        'Sample': {
          tables: ['table1'],
          columns: {
            'name': { fromModel: JSON.stringify },
          }
        }
      },
      items: [
        {
          doc: { partition1: 'value_partition1', clustering1: 'value_clustering1', name: { prop1: 1, prop2: 'two' } },
          query: 'INSERT INTO ks1.table1 ("partition1", "clustering1", "name") VALUES (?, ?, ?)',
          params: [ 'value_partition1', 'value_clustering1', '{"prop1":1,"prop2":"two"}']
        }
      ]
    }));
  });

  describe('#update()', () => {
    mapperTestHelper.testParameters('update');

    it('should retrieve the table that apply and make a single execution', () => testQueries('update', [
      {
        doc: { clustering1: 'value2' , partition1: 'value1', name: 'name1' },
        query: 'UPDATE ks1.table1 SET "name" = ? WHERE "clustering1" = ? AND "partition1" = ?',
        params: ['name1', 'value2', 'value1'],
        isIdempotent: true
      }]));

    it('should mark LWT queries as non-idempotent', () => testQueries('update', [
      {
        doc: {clustering1: 'value2', partition1: 'value1', name: 'name1'},
        docInfo: {when: {name: 'previous name'}},
        query: 'UPDATE ks1.table1 SET "name" = ? WHERE "clustering1" = ? AND "partition1" = ? IF "name" = ?',
        params: ['name1', 'value2', 'value1', 'previous name'],
        isIdempotent: false
      }]));

    it('should append/prepend to a list', () => testQueries('update', [
      {
        doc: { clustering1: 'value2' , partition1: 'value1', name: 'name1', list1: q.append(['a', 'b']) },
        query: 'UPDATE ks1.table1 SET "name" = ?, "list1" = "list1" + ? WHERE "clustering1" = ? AND "partition1" = ?',
        params: ['name1', ['a', 'b'], 'value2', 'value1'],
        isIdempotent: false
      }, {
        doc: { clustering1: 'value2' , partition1: 'value1', name: 'name1', list1: q.prepend(['a', 'b']) },
        query: 'UPDATE ks1.table1 SET "name" = ?, "list1" = ? + "list1" WHERE "clustering1" = ? AND "partition1" = ?',
        params: ['name1', ['a', 'b'], 'value2', 'value1'],
        isIdempotent: false
      }]));

    it('should increment/decrement a counter', () => {
      const clientInfo = mapperTestHelper.getClient([ 'partition1', 'clustering1', { name: 'c1', type: { code: dataTypes.counter }}], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const items = [
        {
          doc: { clustering1: 'value2' , partition1: 'value1', c1: q.incr(10) },
          query: 'UPDATE ks1.table1 SET "c1" = "c1" + ? WHERE "clustering1" = ? AND "partition1" = ?'
        }, {
          doc: { clustering1: 'another id 2' , partition1: 'another id 1', c1: q.decr(10) },
          query: 'UPDATE ks1.table1 SET "c1" = "c1" - ? WHERE "clustering1" = ? AND "partition1" = ?'
        }];

      return Promise.all(items.map((item, index) => modelMapper.update(item.doc).then(() => {
        assert.strictEqual(clientInfo.executions.length, items.length);
        const execution = clientInfo.executions[index];
        assert.strictEqual(execution.query, item.query);
        assert.deepStrictEqual(execution.params, [ 10, item.doc.clustering1, item.doc.partition1 ]);
        helper.assertProperties(execution.options, { prepare: true, isIdempotent: false });
      })));
    });

    it('should throw an error when filter or conditions are not valid', () => testErrors('update', [
      {
        doc: { partition1: 'x', notAValidProp: 'y' },
        message: 'No table matches (all PKs and columns to set have to be specified) fields: [partition1,notAValidProp]'
      }, {
        doc: { partition1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (all PKs and columns to set have to be specified) fields: [notAValidProp]'
      }, {
        doc: { partition1: 'x', name: 'y' },
        message: 'No table matches (all PKs and columns to set have to be specified) fields: [partition1,name]'
      }, {
        doc: { partition1: 'x', clustering1: 'y', name: 'z'},
        docInfo: { when: { notAValidProp: 'm'} },
        message: 'No table matches (all PKs and columns to set have to be specified) fields: [partition1,clustering1,name]; condition: [notAValidProp]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }, {
        doc: { partition1: 'x', clustering1: 'y' },
        message: 'No table matches (all PKs and columns to set have to be specified) fields: [partition1,clustering1]'
      }
    ]));

    it('should use fields when specified', () => testQueries('update', [
      {
        doc: { clustering1: 'value2', partition1: 'value1', name: 'name1', description: 'description1' },
        docInfo: { fields: [ 'partition1', 'clustering1', 'description' ] },
        query: 'UPDATE ks1.table1 SET "description" = ? WHERE "partition1" = ? AND "clustering1" = ?',
        params: ['description1', 'value1', 'value2']
      }]));

    it('should set TTL', () => testQueries('update', [
      {
        doc: { partition1: 'value_partition1', clustering1: 'value_clustering1', name: 'value_name1' },
        docInfo: { ttl: 360 },
        query: 'UPDATE ks1.table1 USING TTL ? SET "name" = ? WHERE "partition1" = ? AND "clustering1" = ?',
        params: [ 360, 'value_name1', 'value_partition1', 'value_clustering1' ]
      }
    ]));

    it('should use the mapping function in the SET WHERE and IF clauses', () => testQueries({
      methodName: 'update',
      models: {
        'Sample': {
          tables: ['table1'],
          columns: {
            'name': { fromModel: JSON.stringify },
            'clustering1': { fromModel: v => v + "_suffix" }
          }
        }
      },
      items: [
        {
          doc: { partition1: 'value_partition1', clustering1: 'value_clustering1', name: { prop1: 1, prop2: 'two' } },
          query: 'UPDATE ks1.table1 SET "name" = ? WHERE "partition1" = ? AND "clustering1" = ?',
          params: [ '{"prop1":1,"prop2":"two"}', 'value_partition1', 'value_clustering1_suffix' ]
        },
        {
          doc: { partition1: 'value_partition1', clustering1: 'value_clustering1', description: 'my description' },
          docInfo: { when: { name: { a: 'a', b: 2 } } },
          query: 'UPDATE ks1.table1 SET "description" = ? WHERE "partition1" = ? AND "clustering1" = ? IF "name" = ?',
          params: [ 'my description', 'value_partition1', 'value_clustering1_suffix', '{"a":"a","b":2}' ],
          isIdempotent: false
        }
      ]
    }));
  });

  describe('#remove()', () => {
    mapperTestHelper.testParameters('remove');

    it('should throw an error when filter or conditions are not valid', () => testErrors('remove', [
      {
        doc: { partition1: 'x', notAValidProp: 'y' },
        message: 'No table matches (must specify all partition key and top-level clustering columns) fields: [partition1,notAValidProp]'
      }, {
        doc: { partition1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (must specify all partition key and top-level clustering columns) fields: [notAValidProp]'
      }, {
        doc: { partition1: 'x', name: 'y' },
        message: 'No table matches (must specify all partition key and top-level clustering columns) fields: [partition1,name]'
      }, {
        doc: { partition1: 'x', clustering1: 'y'},
        docInfo: { when: { notAValidProp: 'm'} },
        message: 'No table matches (must specify all partition key and top-level clustering columns) fields: [partition1,clustering1]; condition: [notAValidProp]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }, {
        doc: { partition1: 'x', clustering2: 'y' },
        message: 'No table matches (must specify all partition key and top-level clustering columns) fields: [partition1,clustering2]'
      }
    ]));

    it('should generate the query, params and set the idempotency', () => testQueries('remove', [
      {
        doc: { partition1: 'x', clustering1: 'y' },
        query: 'DELETE FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ?',
        params: [ 'x', 'y' ]
      }, {
        doc: { partition1: 'x', clustering1: 'y' },
        docInfo: { when: { name: 'a' }},
        query: 'DELETE FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ? IF "name" = ?',
        params: [ 'x', 'y', 'a' ],
        isIdempotent: false
      }, {
        doc: { partition1: 'x', clustering1: 'y' },
        docInfo: { ifExists: true },
        query: 'DELETE FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ? IF EXISTS',
        params: [ 'x', 'y' ],
        isIdempotent: false
      }, {
        doc: { partition1: 'x', clustering1: 'y' },
        docInfo: { fields: [ 'partition1', 'clustering1', 'name' ], deleteOnlyColumns: true },
        query: 'DELETE "name" FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ?',
        params: [ 'x', 'y' ]
      }
    ]));

    it('should use the mapping function in the WHERE and IF clauses', () => testQueries({
      methodName: 'remove',
      models: {
        'Sample': {
          tables: ['table1'],
          columns: {
            'name': { fromModel: JSON.stringify },
            'clustering1': { fromModel: v => v + "_suffix" }
          }
        }
      },
      items: [
        {
          doc: { partition1: 'value_partition1', clustering1: 'value_clustering1' },
          query: 'DELETE FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ?',
          params: [ 'value_partition1', 'value_clustering1_suffix' ]
        },
        {
          doc: { partition1: 'value_partition1', clustering1: 'value_clustering1' },
          docInfo: { when: { name: { a: 1 } }},
          query: 'DELETE FROM ks1.table1 WHERE "partition1" = ? AND "clustering1" = ? IF "name" = ?',
          params: [ 'value_partition1', 'value_clustering1_suffix', '{"a":1}' ],
          isIdempotent: false
        },
      ]
    }));
  });
});

function testErrors(methodName, items) {
  return Promise.all(items.map(item => {
    const columns = [ 'partition1', 'clustering1', 'clustering2', 'name'];
    const clientInfo = mapperTestHelper.getClient(columns, [ 1, 2 ], 'ks1');
    const modelMapper = mapperTestHelper.getModelMapper(clientInfo);

    let catchCalled = false;

    return modelMapper[methodName](item.doc, item.docInfo)
      .catch(err => {
        catchCalled = true;
        helper.assertInstanceOf(err, Error);
        assert.strictEqual(err.message, item.message);
      })
      .then(() => assert.strictEqual(catchCalled, true));
  }));
}

async function testQueries(methodName, items) {
  let models = null;
  const columns = [ 'partition1', 'clustering1', 'name', { name: 'list1', type: { code: dataTypes.list }}, 'description'];

  if (typeof methodName === 'object') {
    // Its an object with properties as parameters
    models = methodName.models;
    items = methodName.items;
    methodName = methodName.methodName;
  }

  const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ]);
  const modelMapper = mapperTestHelper.getModelMapper(clientInfo, models);

  for (const item of items) {
    await modelMapper[methodName](item.doc, item.docInfo, item.executionOptions);
    const execution = clientInfo.executions.pop();
    assert.strictEqual(execution.query, item.query);
    assert.deepStrictEqual(execution.params, item.params);
    const expectedOptions = { prepare: true, isIdempotent: item.isIdempotent !== false };
    helper.assertProperties(execution.options, expectedOptions);
  }
}
