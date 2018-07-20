'use strict';

const utils = require('../utils');
const format = require('util').format;
const QueryGenerator = require('./query-generator');
const ResultMapper = require('./result-mapper');
const Result = require('./result');
const Cache = require('./cache');
const Tree = require('./tree');

class MappingHandler {
  /**
   * @param {Client} client
   * @param {TableMappingInfo} mappingInfo
   */
  constructor(client, mappingInfo) {
    this._client = client;
    this._info = mappingInfo;
    this._cache = {
      select: new Tree()
    };
  }

  /**
   * @param {Object} doc
   * @param {{fields, groupBy, orderBy, limit}} docInfo
   * @return {Promise<Function>}
   */
  getSelectExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getSelectKey(docKeys, doc, docInfo);
    // Cache the executor and the result mapper under the same key
    // That way, those can get evicted together
    const cacheItem = this._cache.select.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    let fields = utils.emptyArray;

    if (docInfo) {
      if (docInfo.fields) {
        fields = docInfo.fields;
      }
    }

    const columnKeys = docKeys.map(x => this._info.getColumnName(x));
    const columnFields = fields.map(x => this._info.getColumnName(x));

    return this._getTableForDoc(columnKeys, columnFields).then(tableName => {
      // Part of the closure
      const query = QueryGenerator.getSelect(tableName, columnKeys, columnFields);
      const paramsGetter = QueryGenerator.selectParamsGetter(docKeys, docInfo);
      const client = this._client;
      const self = this;

      //TODO: Parse execution options

      cacheItem.executor = function selectExecutor(doc, docInfo, executionOptions) {
        return client.execute(query, paramsGetter(doc, docInfo), { prepare: true }).then(rs => {
          if (cacheItem.resultAdapter === null) {
            cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
          }
          return new Result(rs, self._info, cacheItem.resultAdapter);
        });
      };

      return cacheItem.executor;
    });
  }


  /**
   * @param {Object} doc
   * @param {{when, ifExists, ttl, fields}} docInfo
   * @return {Promise<Function>}
   */
  getInsertExecutor(doc, docInfo) {
    // Check cache
    // Get all the tables affected
    // For each tables affected
    //  Generate query and parameter getters
    // if there is a single query
    //   create an executor using execute()
    // otherwise
    //   create an executor using logged batch()
  }

  _getTableForDoc(columnKeys, columnFields) {
    if (this._info.tables.length === 1) {
      return this._info.tables[0].name;
    }

    return Promise.all(
      this._info.tables.map(t => {
        if (t.isView) {
          return this._client.metadata.getMaterializedView(this._info.keyspace, t.name);
        }
        return this._client.metadata.getTable(this._info.keyspace, t.name);
      }))
      .then(tables => {
        for (let i = 0; i < tables.length; i++) {
          const table = tables[i];
          if (table === null) {
            throw new Error(format('Table %s could not be retrieved', this._info.tables[i].name));
          }

          let allPartitionKeysAreIncluded = true;

          const partitionKeys = new Map(table.partitionKeys.map(k => {
            allPartitionKeysAreIncluded = allPartitionKeysAreIncluded && columnKeys.indexOf(k.name) >= 0;
            return [ k.name, true ];
          }));

          if (!allPartitionKeysAreIncluded) {
            // Not all the partition keys are covered
            continue;
          }

          if (columnKeys.length > table.partitionKeys.length) {
            // Validate that all keys are clustered keys
            const notCoveredByClusteringKeys = columnKeys
              .filter(name => !partitionKeys.get(name) && table.clusteringKeys.indexOf(name) === -1).length > 0;
            if (notCoveredByClusteringKeys) {
              continue;
            }
          }

          // TODO: All fields must be contained
          // TODO: orderBy fields must be contained in the clustering keys
          return table.name;
        }

        throw new Error(format('No configured table matches the columns: %j', columnKeys));
      });
  }
}

// class TableMappings {
//   //TODO: ConventionBasedTableMappings
//   //TODO: DefaultTableMappings
// }


module.exports = MappingHandler;