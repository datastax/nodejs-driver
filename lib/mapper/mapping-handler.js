'use strict';

const utils = require('../utils');
const QueryGenerator = require('./query-generator');
const ResultMapper = require('./result-mapper');
const Result = require('./result');
const Cache = require('./cache');
const Tree = require('./tree');
const ObjectSelector = require('./object-selector');
const DocInfoAdapter = require('./doc-info-adapter');

class MappingHandler {
  /**
   * @param {Client} client
   * @param {TableMappingInfo} mappingInfo
   */
  constructor(client, mappingInfo) {
    this._client = client;
    this._info = mappingInfo;
    this._cache = {
      select: new Tree(),
      insert: new Tree(),
      update: new Tree()
    };
  }

  /**
   * Gets a function to be used to execute SELECT the query using the document.
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

    const columnKeys = docKeys.map(x => this._info.getColumnName(x));
    const fieldColumns = (!docInfo || !docInfo.fields || docInfo.fields.length === 0)
      ? utils.emptyArray
      : docInfo.fields.map(x => this._info.getColumnName(x));

    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this._info);

    return ObjectSelector.getForSelect(this._client, this._info, columnKeys, fieldColumns, orderByColumns)
      .then(tableName => {
        // Part of the closure
        const query = QueryGenerator.getSelect(tableName, columnKeys, fieldColumns, orderByColumns);
        const paramsGetter = QueryGenerator.selectParamsGetter(docKeys, docInfo);
        const self = this;

        //TODO: Parse execution options

        cacheItem.executor = function selectExecutor(doc, docInfo, executionOptions) {
          return self._client.execute(query, paramsGetter(doc, docInfo), { prepare: true }).then(rs => {
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
   * Gets a function to be used to execute INSERT the query using the document.
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}} docInfo
   * @return {Promise<Function>}
   */
  getInsertExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getInsertKey(docKeys, docInfo);

    const cacheItem = this._cache.insert.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this._info);
    const ifNotExists = docInfo && docInfo.ifNotExists;
    const ttl = docInfo && docInfo.ttl;

    // Get all the tables affected
    return ObjectSelector.getForInsert(this._client, this._info, propertiesInfo)
      .then(tables => {
        // For each tables affected, Generate query and parameter getters
        // Part of the closure
        const queries = tables.map(table => {
          // Not all columns are contained in the table
          const filteredPropertiesInfo = propertiesInfo
            .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);

          return ({
            query: QueryGenerator.getInsert(table.name, filteredPropertiesInfo, ifNotExists, ttl),
            paramsGetter: QueryGenerator.insertParamsGetter(filteredPropertiesInfo, docInfo)
          });
        });

        const self = this;

        //TODO: Parse execution options

        if (queries.length === 1) {
          cacheItem.executor = function insertSingleExecutor(doc, docInfo, executionOptions) {
            return self._client.execute(queries[0].query, queries[0].paramsGetter(doc, docInfo), { prepare: true })
              .then(rs => {
                if (cacheItem.resultAdapter === null) {
                  cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
                }
                return new Result(rs, self._info, cacheItem.resultAdapter);
              });
          };
        }
        else {
          cacheItem.executor = function insertBatchExecutor(doc, docInfo, executionOptions) {
            // Use the params getter function to obtain the parameters each time
            const queryAndParams = queries.map(q => ({
              query: q.query,
              params: q.paramsGetter(doc, docInfo)
            }));

            // Execute using a Batch
            return self._client.batch(queryAndParams, { prepare: true })
              .then(rs => {
                if (cacheItem.resultAdapter === null) {
                  cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
                }
                return new Result(rs, self._info, cacheItem.resultAdapter);
              });
          };
        }

        return cacheItem.executor;
      });
  }

  /**
   * Gets a function to be used to execute UPDATE the query using the document.
   * @param {Object} doc
   * @param {{ifExists, when, ttl, fields}} docInfo
   * @return {Promise<Function>}
   */
  getUpdateExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getUpdateKey(docKeys, doc, docInfo);

    const cacheItem = this._cache.update.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this._info);
    const ifExists = docInfo && docInfo.ifExists;
    const ttl = docInfo && docInfo.ttl;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this._info)
      : utils.emptyArray;

    // Get all the tables affected
    return ObjectSelector.getForUpdate(this._client, this._info, propertiesInfo, when)
      .then(tables => {
        // For each tables affected, Generate query and parameter getters
        // Part of the closure
        const queries = tables.map(table => {
          // Not all columns are contained in the table
          const filteredPropertiesInfo = propertiesInfo
            .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);

          return ({
            query: QueryGenerator.getUpdate(table, filteredPropertiesInfo, when, ifExists, ttl),
            paramsGetter: QueryGenerator.updateParamsGetter(filteredPropertiesInfo, when, docInfo)
          });
        });

        const self = this;

        //TODO: Parse execution options

        if (queries.length === 1) {
          cacheItem.executor = function updateSingleExecutor(doc, docInfo, executionOptions) {
            return self._client.execute(queries[0].query, queries[0].paramsGetter(doc, docInfo), { prepare: true })
              .then(rs => {
                if (cacheItem.resultAdapter === null) {
                  cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
                }
                return new Result(rs, self._info, cacheItem.resultAdapter);
              });
          };
        }
        else {
          cacheItem.executor = function updateBatchExecutor(doc, docInfo, executionOptions) {
            // Use the params getter function to obtain the parameters each time
            const queryAndParams = queries.map(q => ({
              query: q.query,
              params: q.paramsGetter(doc, docInfo)
            }));

            // Execute using a Batch
            return self._client.batch(queryAndParams, { prepare: true })
              .then(rs => {
                if (cacheItem.resultAdapter === null) {
                  cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
                }
                return new Result(rs, self._info, cacheItem.resultAdapter);
              });
          };
        }

        return cacheItem.executor;
      });
  }
}

// class TableMappings {
//   //TODO: ConventionBasedTableMappings
//   //TODO: DefaultTableMappings
// }

module.exports = MappingHandler;