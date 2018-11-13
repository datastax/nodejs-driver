'use strict';

const assert = require('assert');
const types = require('../../../lib/types');
const ModelMapper = require('../../../lib/mapper/model-mapper');
const ResultSet = types.ResultSet;
const dataTypes = types.dataTypes;
const Mapper = require('../../../lib/mapper/mapper');

module.exports = {
  /**
   * Gets a fake client instance that returns metadata for a single table
   * @param {Array} columns
   * @param {Array} primaryKeys
   * @param {String} [keyspace]
   * @param {Object} [response]
   * @return {{executions: Array, client: Client}}
   */
  getClient: function (columns, primaryKeys, keyspace, response) {
    columns = columns.map(c => (typeof c === 'string' ? { name: c, type: { code: dataTypes.text }} : c));
    const partitionKeys = columns.slice(0, primaryKeys[0]);
    const clusteringKeys = primaryKeys[1] !== undefined
      ? columns.slice(primaryKeys[0], primaryKeys[1] + primaryKeys[0])
      : [];

    const result = {
      executions: [],
      client: {
        keyspace: keyspace === undefined ? 'ks1' : keyspace,
        metadata: {
          getTable: (ks, name) => {
            const table = { name, partitionKeys, clusteringKeys, columnsByName: {}, columns };
            table.columns.forEach(c => table.columnsByName[c.name] = c);
            return Promise.resolve(table);
          },
        },
        execute: function (query, params, options) {
          result.executions.push({ query, params, options });
          return Promise.resolve(new ResultSet(response || {}, '10.1.1.1:9042', {}, 1, 1));
        }
      }
    };
    return result;
  },

  getModelMapper: function (clientInfo) {
    const mapper = new Mapper(clientInfo.client, {
      models: {
        'Sample': {
          tables: [ 'table1' ],
          columns: {
            'location_type': 'locationType'
          }
        }
      }
    });

    return mapper.forModel('Sample');
  },

  testParameters: function (methodName, handlerMethodName) {
    const handlerParameters = {
      select: { executor: null, executorCall: null },
      insert: { executor: null, executorCall: null },
      update: { executor: null, executorCall: null }
    };

    const instance = new ModelMapper('abc', {
      getInsertExecutor: (doc, docInfo) => {
        handlerParameters.insert.executor = { doc, docInfo };
        return Promise.resolve((doc, docInfo, executionOptions) => {
          handlerParameters.insert.executorCall = { doc, docInfo, executionOptions};
          return {};
        });
      },
      getUpdateExecutor: (doc, docInfo) => {
        handlerParameters.update.executor = { doc, docInfo };
        return Promise.resolve((doc, docInfo, executionOptions) => {
          handlerParameters.update.executorCall = { doc, docInfo, executionOptions};
          return {};
        });
      },
      getSelectExecutor: (doc, docInfo) => {
        handlerParameters.select.executor = { doc, docInfo };
        return Promise.resolve((doc, docInfo, executionOptions) => {
          handlerParameters.select.executorCall = { doc, docInfo, executionOptions};
          return { first: () => null };
        });
      }
    });

    it('should call the handler to obtain the executor and invoke it', () => {
      const doc = { a: 1};
      const docInfo = { b: 2 };
      const executionOptions = { c : 3 };

      handlerMethodName = handlerMethodName || methodName;

      return instance[methodName](doc, docInfo, executionOptions)
        .then(() => {
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo });
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo, executionOptions });
        });
    });

    it('should set the executionOptions when the second parameter is a string', () => {
      const doc = { a: 100 };
      const executionOptions = 'exec-profile';

      return instance[methodName](doc, executionOptions)
        .then(() => {
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo: null });
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo: null, executionOptions });
        });
    });

    it('should set the executionOptions when the third parameter is a string', () => {
      const doc = { a: 10 };
      const docInfo = { b: 20 };
      const executionOptions = 'exec-profile2';

      return instance[methodName](doc, docInfo, executionOptions)
        .then(() => {
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executor, { doc, docInfo });
          assert.deepStrictEqual(handlerParameters[handlerMethodName].executorCall, { doc, docInfo, executionOptions });
        });
    });
  }
};

