'use strict';

const assert = require('assert');
const Mapper = require('../../../lib/mapper/mapper');

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
});
