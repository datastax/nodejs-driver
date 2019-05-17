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
const types = require('../../../lib/types');
const ModelMapper = require('../../../lib/mapping/model-mapper');
const ResultSet = types.ResultSet;
const dataTypes = types.dataTypes;
const Mapper = require('../../../lib/mapping/mapper');

const mapperHelper = module.exports = {
  /**
   * Gets a fake client instance that returns metadata for a single table
   * @param {Array} columns
   * @param {Array} primaryKeys
   * @param {String} [keyspace]
   * @param {Object} [response]
   * @return {{executions: Array, batchExecutions: Array, client: Client}}
   */
  getClient: function (columns, primaryKeys, keyspace, response) {
    columns = columns.map(c => (typeof c === 'string' ? { name: c, type: { code: dataTypes.text }} : c));
    const partitionKeys = columns.slice(0, primaryKeys[0]);
    const clusteringKeys = primaryKeys[1] !== undefined
      ? columns.slice(primaryKeys[0], primaryKeys[1] + primaryKeys[0])
      : [];

    const result = {
      executions: [],
      batchExecutions: [],
      logMessages: [],
      client: {
        connect: () => Promise.resolve(),
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
        },
        batch: function (queries, options) {
          result.batchExecutions.push({ queries, options });
          return Promise.resolve(new ResultSet(response || {}, '10.1.1.1:9042', {}, 1, 1));
        },
        log: function (level, message) {
          result.logMessages.push({ level, message });
        }
      }
    };

    return result;
  },

  getModelMapper: function (clientInfo) {
    const mapper = mapperHelper.getMapper(clientInfo);
    return mapper.forModel('Sample');
  },

  getMapper: function (clientInfo) {
    return new Mapper(clientInfo.client, {
      models: {
        'Sample': {
          tables: [ 'table1' ],
          columns: {
            'location_type': 'locationType'
          }
        }
      }
    });
  },

  testParameters: function (methodName, handlerMethodName) {
    const handlerParameters = {
      select: { executor: null, executorCall: null },
      insert: { executor: null, executorCall: null },
      update: { executor: null, executorCall: null },
      remove: { executor: null, executorCall: null }
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
      getDeleteExecutor: (doc, docInfo) => {
        handlerParameters.remove.executor = { doc, docInfo };
        return Promise.resolve((doc, docInfo, executionOptions) => {
          handlerParameters.remove.executorCall = { doc, docInfo, executionOptions};
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

