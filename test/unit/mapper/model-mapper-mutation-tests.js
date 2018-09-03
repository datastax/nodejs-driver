'use strict';

const assert = require('assert');
const q = require('../../../lib/mapper/q').q;
const types = require('../../../lib/types');
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');
const dataTypes = types.dataTypes;

describe('ModelMapper', () => {
  describe('#insert()', () => {
    mapperTestHelper.testParameters('insert');

    it('should retrieve the table that apply and make a single execution', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const doc = { id2: 'value2' , id1: 'value1' };

      return modelMapper.insert(doc)
        .then(() => {
          assert.strictEqual(clientInfo.executions.length, 1);
          const execution = clientInfo.executions[0];
          assert.strictEqual(execution.query, 'INSERT INTO table1 (id2, id1) VALUES (?, ?)');
          assert.deepStrictEqual(execution.params, Object.keys(doc).map(key => doc[key]));
          helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
        });
    });

    it('should mark LWT queries as non-idempotent', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const doc = { id2: 'value2' , id1: 'value1', name: 'name1' };

      return modelMapper.insert(doc, { ifNotExists: true })
        .then(() => {
          assert.strictEqual(clientInfo.executions.length, 1);
          const execution = clientInfo.executions[0];
          assert.strictEqual(execution.query, 'INSERT INTO table1 (id2, id1, name) VALUES (?, ?, ?) IF NOT EXISTS');
          assert.deepStrictEqual(execution.params, Object.keys(doc).map(key => doc[key]));
          helper.assertProperties(execution.options, { isIdempotent: false });
        });
    });

    it('should throw an error when the table metadata retrieval fails');

    it('should throw an error when filter or conditions are not valid', () => testErrors('insert', [
      {
        doc: { id1: 'x', notAValidProp: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,notAValidProp]'
      }, {
        doc: { id1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (all PKs have to be specified) fields: [notAValidProp]'
      }, {
        doc: { id1: 'x', name: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,name]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }
    ]));
  });

  describe('#update()', () => {
    mapperTestHelper.testParameters('update');

    it('should retrieve the table that apply and make a single execution', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const doc = { id2: 'value2' , id1: 'value1', name: 'name1' };

      return modelMapper.update(doc)
        .then(() => {
          assert.strictEqual(clientInfo.executions.length, 1);
          const execution = clientInfo.executions[0];
          assert.strictEqual(execution.query, 'UPDATE table1 SET name = ? WHERE id2 = ? AND id1 = ?');
          assert.deepStrictEqual(execution.params, [ doc.name, doc.id2, doc.id1 ]);
          helper.assertProperties(execution.options, { prepare: true, isIdempotent: true });
        });
    });

    it('should mark LWT queries as non-idempotent', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const doc = { id2: 'value2' , id1: 'value1', name: 'name1' };
      const docInfo = { when: { name: 'previous name' }};

      return modelMapper.update(doc, docInfo)
        .then(() => {
          assert.strictEqual(clientInfo.executions.length, 1);
          const execution = clientInfo.executions[0];
          assert.strictEqual(execution.query, 'UPDATE table1 SET name = ? WHERE id2 = ? AND id1 = ? IF name = ?');
          assert.deepStrictEqual(execution.params, [ doc.name, doc.id2, doc.id1, docInfo.when.name ]);
          helper.assertProperties(execution.options, { prepare: true, isIdempotent: false });
        });
    });

    it('should append/prepend to a list', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name', { name: 'list1', type: { code: dataTypes.list }}], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const items = [
        {
          doc: { id2: 'value2' , id1: 'value1', name: 'name1', list1: q.append(['a', 'b']) },
          query: 'UPDATE table1 SET name = ?, list1 = list1 + ? WHERE id2 = ? AND id1 = ?'
        }, {
          doc: { id2: 'value2' , id1: 'value1', name: 'name1', list1: q.prepend(['a', 'b']) },
          query: 'UPDATE table1 SET name = ?, list1 = ? + list1 WHERE id2 = ? AND id1 = ?'
        }];

      return Promise.all(items.map((item, index) => modelMapper.update(item.doc).then(() => {
        assert.strictEqual(clientInfo.executions.length, items.length);
        const execution = clientInfo.executions[index];
        assert.strictEqual(execution.query, item.query);
        assert.deepStrictEqual(execution.params, [ item.doc.name, ['a', 'b'], item.doc.id2, item.doc.id1 ]);
        helper.assertProperties(execution.options, { prepare: true, isIdempotent: false });
      })));
    });

    it('should increment/decrement a counter', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', { name: 'c1', type: { code: dataTypes.counter }}], [ 1, 1 ]);
      const modelMapper = mapperTestHelper.getModelMapper(clientInfo);
      const items = [
        {
          doc: { id2: 'value2' , id1: 'value1', c1: q.incr(10) },
          query: 'UPDATE table1 SET c1 = c1 + ? WHERE id2 = ? AND id1 = ?'
        }, {
          doc: { id2: 'another id 2' , id1: 'another id 1', c1: q.decr(10) },
          query: 'UPDATE table1 SET c1 = c1 - ? WHERE id2 = ? AND id1 = ?'
        }];

      return Promise.all(items.map((item, index) => modelMapper.update(item.doc).then(() => {
        assert.strictEqual(clientInfo.executions.length, items.length);
        const execution = clientInfo.executions[index];
        assert.strictEqual(execution.query, item.query);
        assert.deepStrictEqual(execution.params, [ 10, item.doc.id2, item.doc.id1 ]);
        helper.assertProperties(execution.options, { prepare: true, isIdempotent: false });
      })));
    });

    it('should throw an error when filter or conditions are not valid', () => testErrors('update', [
      {
        doc: { id1: 'x', notAValidProp: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,notAValidProp]'
      }, {
        doc: { id1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (all PKs have to be specified) fields: [notAValidProp]'
      }, {
        doc: { id1: 'x', name: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,name]'
      }, {
        doc: { id1: 'x', id2: 'y', name: 'z'},
        docInfo: { when: { notAValidProp: 'm'} },
        message: 'No table matches (all PKs have to be specified) fields: [id1,id2,name]; condition: [notAValidProp]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }
    ]));

    it('should use fields when specified');
  });

  describe('#remove()', () => {
    mapperTestHelper.testParameters('remove');

    it('should throw an error when filter or conditions are not valid', () => testErrors('remove', [
      {
        doc: { id1: 'x', notAValidProp: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,notAValidProp]'
      }, {
        doc: { id1: 'x'},
        docInfo: { fields: ['notAValidProp'] },
        message: 'No table matches (all PKs have to be specified) fields: [notAValidProp]'
      }, {
        doc: { id1: 'x', name: 'y' },
        message: 'No table matches (all PKs have to be specified) fields: [id1,name]'
      }, {
        doc: { id1: 'x', id2: 'y', name: 'z'},
        docInfo: { when: { notAValidProp: 'm'} },
        message: 'No table matches (all PKs have to be specified) fields: [id1,id2,name]; condition: [notAValidProp]'
      }, {
        doc: {},
        message: 'Expected object with keys'
      }
    ]));
  });
});

function testErrors(methodName, items) {
  return Promise.all(items.map(item => {
    const columns = [ 'id1', 'id2', 'name'];
    const clientInfo = mapperTestHelper.getClient(columns, [ 1, 1 ], 'ks1');
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

