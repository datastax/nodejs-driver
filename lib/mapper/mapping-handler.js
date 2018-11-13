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
      update: new Tree(),
      remove: new Tree()
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

    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, null, doc, this._info);
    const fieldsInfo = DocInfoAdapter.getPropertiesInfo(utils.emptyArray, docInfo, doc, this._info);
    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this._info);

    return ObjectSelector.getForSelect(this._client, this._info, propertiesInfo, fieldsInfo, orderByColumns)
      .then(tableName => {
        // Part of the closure
        const query = QueryGenerator.getSelect(tableName, propertiesInfo, fieldsInfo, orderByColumns);
        const paramsGetter = QueryGenerator.selectParamsGetter(propertiesInfo, docInfo);
        const self = this;

        cacheItem.executor = function selectExecutor(doc, docInfo, executionOptions) {
          const options = DocInfoAdapter.adaptSelectOptions(executionOptions);

          return self._client.execute(query, paramsGetter(doc, docInfo), options).then(rs => {
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

        if (tables.length > 1 && ifNotExists) {
          throw new Error('Batch with ifNotExists conditions cannot span multiple tables');
        }

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

        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  /**
   * Gets a function to be used to execute the UPDATE queries with the provided document.
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

    return this.createUpdateQueries(docKeys, doc, docInfo)
      .then(queries => {
        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  /**
   * Creates an Array containing the query and the params getter function for each table affected by the UPDATE.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {Object} docInfo
   * @returns {Promise<Array<{query, paramsGetter}>>}
   */
  createUpdateQueries(docKeys, doc, docInfo) {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this._info);
    const ifExists = docInfo && docInfo.ifExists;
    const ttl = docInfo && docInfo.ttl;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this._info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return ObjectSelector.getForUpdate(this._client, this._info, propertiesInfo, when)
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each table affected, Generate query and parameter getters
        return tables.map(table => {
          // Not all columns are contained in the table
          const filteredPropertiesInfo = propertiesInfo
            .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);

          const primaryKeys = new Set(table.partitionKeys.concat(table.clusteringKeys).map(c => c.name));

          return ({
            query: QueryGenerator.getUpdate(table.name, primaryKeys, filteredPropertiesInfo, when, ifExists, ttl),
            paramsGetter: QueryGenerator.updateParamsGetter(primaryKeys, filteredPropertiesInfo, when, docInfo)
          });
        });
      });
  }

  /**
   * Gets a function to be used to execute the DELETE queries with the provided document.
   * @param {Object} doc
   * @param {{when, ifExists, fields}} docInfo
   * @return {Promise<Function>}
   */
  getDeleteExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    const cacheKey = Cache.getRemoveKey(docKeys, doc, docInfo);

    const cacheItem = this._cache.remove.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this._info);
    const ifExists = docInfo && docInfo.ifExists;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this._info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return ObjectSelector.getForDelete(this._client, this._info, propertiesInfo, when)
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each tables affected, Generate query and parameter getters
        // Part of the closure
        const queries = tables.map(table => {
          // Not all columns are contained in the table
          const filteredPropertiesInfo = propertiesInfo
            .filter(pInfo => table.columnsByName[pInfo.columnName] !== undefined);

          const primaryKeys = new Set(table.partitionKeys.concat(table.clusteringKeys).map(c => c.name));

          return ({
            query: QueryGenerator.getDelete(table.name, primaryKeys, filteredPropertiesInfo, when, ifExists),
            paramsGetter: QueryGenerator.deleteParamsGetter(primaryKeys, filteredPropertiesInfo, when)
          });
        });

        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  _setSingleExecutor(cacheItem, queries) {
    // Parameters and this instance are part of the closure
    const self = this;

    // Set the function to execute the request in the cache
    cacheItem.executor = function singleExecutor(doc, docInfo, executionOptions) {
      const options = DocInfoAdapter.adaptOptions(executionOptions);

      return self._client.execute(queries[0].query, queries[0].paramsGetter(doc, docInfo), options)
        .then(rs => {
          if (cacheItem.resultAdapter === null) {
            cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
          }
          return new Result(rs, self._info, cacheItem.resultAdapter);
        });
    };

    return cacheItem.executor;
  }

  _setBatchExecutor(cacheItem, queries) {
    // Parameters and this instance are part of the closure
    const self = this;

    // Set the function to execute the batch request in the cache
    cacheItem.executor = function batchExecutor(doc, docInfo, executionOptions) {
      // Use the params getter function to obtain the parameters each time
      const queryAndParams = queries.map(q => ({
        query: q.query,
        params: q.paramsGetter(doc, docInfo)
      }));

      const options = DocInfoAdapter.adaptOptions(executionOptions);

      // Execute using a Batch
      return self._client.batch(queryAndParams, options)
        .then(rs => {
          if (cacheItem.resultAdapter === null) {
            cacheItem.resultAdapter = ResultMapper.getAdapter(self._info, rs);
          }
          return new Result(rs, self._info, cacheItem.resultAdapter);
        });
    };

    return cacheItem.executor;
  }
}

// class TableMappings {
//   //TODO: ConventionBasedTableMappings
//   //TODO: DefaultTableMappings
// }

module.exports = MappingHandler;