'use strict';

const assert = require('assert');
const Mapper = require('../../../lib/mapping/mapper');
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');
const ModelBatchItem = require('../../../lib/mapping/model-batch-item');
const utils = require('../../../lib/utils');

describe('Mapper', () => {
  describe('constructor', () => {
    it('should validate that client is provided', () => {
      assert.throws(() => new Mapper(), /client must be defined/);
      assert.doesNotThrow(() => new Mapper({}));
    });

    it('should validate that the table name is specified', () => {
      assert.throws(() => new Mapper({ keyspace: 'ks1' }, {
        models: { 'Sample': {
          tables: [ { isView: false }]
        }}
      }), /Table name not specified for model/);

      assert.doesNotThrow(() => new Mapper({ keyspace: 'ks1' }, {
        models: { 'Sample': { tables: [ 't1' ] }}
      }));

      assert.doesNotThrow(() => new Mapper({ keyspace: 'ks1' }, {
        models: { 'Sample': { tables: [ { name: 't1' } ] }}
      }));

      assert.doesNotThrow(() => new Mapper({ keyspace: 'ks1' }, {
        models: { 'Sample': { tables: [ { name: 't1', isView: false } ] }}
      }));
    });

    it('should validate that client keyspace is set', () => {
      assert.throws(() => new Mapper({}, { models: { 'Sample': { table: 't1' }}}),
        /You should specify the keyspace of the model in the MappingOptions when the Client is not using a keyspace/);

      assert.doesNotThrow(() => new Mapper({}, { models: { 'Sample': { table: 't1', keyspace: 'ks1' }}}));
    });
  });

  describe('#forModel()', () => {
    it('should validate that the client keyspace is set when mapping options has not been specified', () => {
      const mapper = new Mapper({});
      assert.throws(() => mapper.forModel('Sample'),
        /You must set the Client keyspace or specify the keyspace of the model in the MappingOptions/);
    });
  });

  describe('#batch()', () => {
    it('should throw when the parameters are not valid', () => {
      const mapper = new Mapper({ keyspace: 'ks1' });
      const message1 = 'First parameter items should be an Array with 1 or more ModelBatchItem instances';
      const message2 = 'Batch items must be instances of ModelBatchItem, use modelMapper.batching object to create' +
        ' each item';

      return Promise.all([
        [ [], message1 ],
        [ null, message1 ],
        [ {}, message1 ],
        [ [ 'a' ], message2 ],
        [ [ 'a', 'b' ], message2 ]
      ].map(item => {
        let catchCalled = false;
        return mapper.batch(item[0])
          .catch(err => {
            catchCalled = true;
            helper.assertInstanceOf(err, Error);
            assert.strictEqual(err.message, item[1]);
          })
          .then(() => assert.strictEqual(catchCalled, true));
      }));
    });

    it('should set the execution options', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const mapper = mapperTestHelper.getMapper(clientInfo);
      const modelMapper = mapper.forModel('Sample');
      const items = [{
        isIdempotent: true,
        logged: true,
        timestamp: 0
      }, {
        isIdempotent: false
      }];

      return Promise.all(items.map((executionOptions, index) => {
        const queryItems = [ modelMapper.batching.insert({ id1: 'a', id2: 'b' }) ];

        return mapper.batch(queryItems, executionOptions).then(() => {
          helper.assertProperties(clientInfo.batchExecutions[index].options, executionOptions);
          assert.strictEqual(clientInfo.batchExecutions[index].options.prepare, true);
        });
      }));
    });

    it('should set idempotency based on the items idempotency', () =>
      Promise.all([ true, false ].map((isIdempotent) => {
        const clientInfo = mapperTestHelper.getClient([ 'id1'], [ 1 ]);
        const mapper = mapperTestHelper.getMapper(clientInfo);

        // 2 queries, one using the provided idempotency to test
        const items = [
          new ModelBatchItem(Promise.resolve([ { isIdempotent, paramsGetter: utils.noop }])),
          new ModelBatchItem(Promise.resolve([ { isIdempotent: true, paramsGetter: utils.noop }])),
        ];

        return mapper.batch(items)
          .then(() => {
            assert.strictEqual(clientInfo.batchExecutions[0].options.isIdempotent, isIdempotent);
          });
      })));

    it('should allow multiple calls using the same ModelBatchItem', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1'], [ 1 ]);
      const mapper = mapperTestHelper.getMapper(clientInfo);

      let resolve = null;

      // 2 queries, one using the provided idempotency to test
      const items = [
        new ModelBatchItem(new Promise(r => resolve = r)),
        new ModelBatchItem(Promise.resolve([ { isIdempotent: true, paramsGetter: utils.noop }])),
      ];

      // call it multiple times with the same queries
      const promises = [ mapper.batch(items), mapper.batch(items) ];

      resolve([ { isIdempotent: false, paramsGetter: utils.noop }]);

      return Promise.all(promises.map((p, index) => p.then(() => {
        assert.strictEqual(clientInfo.batchExecutions[index].options.isIdempotent, false);
      })));
    });
  });
});
