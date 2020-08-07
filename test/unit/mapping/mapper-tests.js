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
const Mapper = require('../../../lib/mapping/mapper');
const ModelMapper = require('../../../lib/mapping/model-mapper');
const helper = require('../../test-helper');
const mapperTestHelper = require('./mapper-unit-test-helper');

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
        /No mapping information found for model 'Sample'\. Mapper .+ without setting the keyspace$/);
    });

    it('should log when creating a ModelMapper instance using mapping information', () => {
      const client = sinon.spy({ log: () => {} });
      const mapper = new Mapper(client, { models: { 'T': { tables: ['t1', 't2'], keyspace: 'ks1' }}});

      // call forModel() multiple times
      for (let i = 0; i < 10; i++) {
        const m = mapper.forModel('T');
        assert.instanceOf(m, ModelMapper);
      }

      // Only should be logged once
      assert.ok(client.log.calledOnce);
      const args = client.log.getCall(0).args;
      assert.strictEqual(args[0], 'info');
      assert.match(args[1], /Creating model mapper for 'T' using mapping information. Keyspace: ks1; Tables: t1,t2.$/);
    });

    it('should log when creating a ModelMapper instance using defaults', () => {
      const client = sinon.spy({ log: () => {}, keyspace: 'abc' });
      const mapper = new Mapper(client);

      // call forModel() multiple times
      for (let i = 0; i < 10; i++) {
        const m = mapper.forModel('user');
        assert.instanceOf(m, ModelMapper);
      }

      assert.ok(client.log.calledOnce);
      const args = client.log.getCall(0).args;
      assert.strictEqual(args[0], 'info');
      assert.match(args[1], /Mapping information for model 'user' not found, creating .+Keyspace: abc; Table: user.$/);
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

    it('should not create unhandled promises', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const mapper = mapperTestHelper.getMapper(clientInfo);
      const modelMapper = mapper.forModel('Sample');
      const unhandled = [];

      function unhandledRejectionFn(reason, promise) {
        unhandled.push({ reason, promise });
      }

      process.on('unhandledRejection', unhandledRejectionFn);

      modelMapper.batching.insert({ z: 'this column does not exist' });
      modelMapper.batching.update({ z: 'this column does not exist' });
      modelMapper.batching.remove({ z: 'this column does not exist' });

      return new Promise(r => setImmediate(r))
        .then(() => process.removeListener('unhandledRejection', unhandledRejectionFn))
        .then(() => assert.strictEqual(unhandled.length, 0));
    });

    it('should throw error when one of the ModelBatchItem is invalid', () => {
      const clientInfo = mapperTestHelper.getClient([ 'id1', 'id2', 'name'], [ 1, 1 ]);
      const mapper = mapperTestHelper.getMapper(clientInfo);
      const modelMapper = mapper.forModel('Sample');
      const unhandled = [];

      function unhandledRejectionFn(reason, promise) {
        unhandled.push({ reason, promise });
      }

      process.on('unhandledRejection', unhandledRejectionFn);
      let error;

      return mapper
        .batch([
          modelMapper.batching.insert({ z: 'this column does not exist' }),
          modelMapper.batching.update({ z: 'this column does not exist' }),
          modelMapper.batching.remove({ z: 'this column does not exist' })
        ])
        .catch(err => error = err)
        .then(() => new Promise(r => setImmediate(r)))
        .then(() => {
          process.removeListener('unhandledRejection', unhandledRejectionFn);
          helper.assertInstanceOf(error, Error);
          helper.assertContains(error.message, 'No table matches');
          assert.strictEqual(unhandled.length, 0);
        });
    });

    it('should set idempotency based on the items idempotency', () =>
      Promise.all([ true, false ].map((isIdempotent) => {
        const clientInfo = mapperTestHelper.getClient([ 'id1'], [ 1 ]);
        const mapper = mapperTestHelper.getMapper(clientInfo);
        const modelMapper = mapper.forModel('Sample');

        // 2 queries, one using the provided idempotency to test
        const items = [
          modelMapper.batching.insert({ id1: 'value1' }, { ifNotExists: !isIdempotent }),
          modelMapper.batching.insert({ id1: 'value2' }),
        ];

        return mapper.batch(items)
          .then(() => {
            assert.strictEqual(clientInfo.batchExecutions[0].options.isIdempotent, isIdempotent);
          });
      })));
  });
});
