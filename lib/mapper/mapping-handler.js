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
   * @param {ModelMappingInfo} mappingInfo
   */
  constructor(client, mappingInfo) {
    this._client = client;
    this._cache = {
      select: new Tree(),
      selectAll: new Tree(),
      insert: new Tree(),
      update: new Tree(),
      remove: new Tree(),
      customQueries: new Map()
    };

    /**
     * Gets the mapping information of the document.
     * @type {ModelMappingInfo}
     */
    this.info = mappingInfo;
  }

  /**
   * Gets a function to be used to execute SELECT the query using the document.
   * @param {Object} doc
   * @param {{fields, orderBy, limit}} docInfo
   * @param {Boolean} allPKsDefined Determines whether all primary keys must be defined in the doc for the query to
   * be valid.
   * @return {Promise<Function>}
   */
  getSelectExecutor(doc, docInfo, allPKsDefined) {
    const docKeys = Object.keys(doc);
    if (docKeys.length === 0) {
      return Promise.reject(new Error('Expected object with keys'));
    }

    const cacheKey = Cache.getSelectKey(docKeys, doc, docInfo);
    // Cache the executor and the result mapper under the same key
    // That way, those can get evicted together
    const cacheItem = this._cache.select.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, null, doc, this.info);
    const fieldsInfo = DocInfoAdapter.getPropertiesInfo(utils.emptyArray, docInfo, doc, this.info);
    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this.info);
    const limit = docInfo && docInfo.limit;

    return ObjectSelector
      .getForSelect(this._client, this.info, allPKsDefined, propertiesInfo, fieldsInfo, orderByColumns)
      .then(tableName => {
        // Part of the closure
        const query = QueryGenerator.getSelect(tableName, this.info.keyspace, propertiesInfo, fieldsInfo,
          orderByColumns, limit);
        const paramsGetter = QueryGenerator.selectParamsGetter(propertiesInfo, limit);
        const self = this;

        cacheItem.executor = function selectExecutor(doc, docInfo, executionOptions) {
          const options = DocInfoAdapter.adaptAllOptions(executionOptions, true);

          return self._client.execute(query, paramsGetter(doc, docInfo), options).then(rs => {
            if (cacheItem.resultAdapter === null) {
              cacheItem.resultAdapter = ResultMapper.getSelectAdapter(self.info, rs);
            }
            return new Result(rs, self.info, cacheItem.resultAdapter);
          });
        };

        return cacheItem.executor;
      });
  }

  getSelectAllExecutor(docInfo) {
    const cacheKey = Cache.getSelectAllKey(docInfo);
    const cacheItem = this._cache.selectAll.getOrCreate(cacheKey, () => ({ executor: null, resultAdapter: null }));

    if (cacheItem.executor !== null) {
      return cacheItem.executor;
    }

    const fieldsInfo = DocInfoAdapter.getPropertiesInfo(utils.emptyArray, docInfo, utils.emptyObject, this.info);
    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this.info);
    const limit = docInfo && docInfo.limit;

    const tableName = ObjectSelector.getForSelectAll(this.info);

    // Part of the closure
    const query = QueryGenerator.getSelect(
      tableName, this.info.keyspace, utils.emptyArray, fieldsInfo, orderByColumns, limit);
    const paramsGetter = QueryGenerator.selectParamsGetter(utils.emptyArray, limit);
    const self = this;

    cacheItem.executor = function selectAllExecutor(docInfo, executionOptions) {
      const options = DocInfoAdapter.adaptAllOptions(executionOptions, true);

      return self._client.execute(query, paramsGetter(null, docInfo), options).then(rs => {
        if (cacheItem.resultAdapter === null) {
          cacheItem.resultAdapter = ResultMapper.getSelectAdapter(self.info, rs);
        }
        return new Result(rs, self.info, cacheItem.resultAdapter);
      });
    };

    return cacheItem.executor;
  }

  /**
   * Gets a function to be used to execute INSERT the query using the document.
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}} docInfo
   * @return {Promise<Function>}
   */
  getInsertExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    if (docKeys.length === 0) {
      return Promise.reject(new Error('Expected object with keys'));
    }

    const cacheKey = Cache.getInsertKey(docKeys, docInfo);
    const cacheItem = this._cache.insert.getOrCreate(cacheKey, () => ({ executor: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    return this.createInsertQueries(docKeys, doc, docInfo)
      .then(queries => {
        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries[0]);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  /**
   * Creates an Array containing the query and the params getter function for each table affected by the INSERT.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}} docInfo
   * @returns {Promise<Array<{query, paramsGetter}>>}
   */
  createInsertQueries(docKeys, doc, docInfo) {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifNotExists = docInfo && docInfo.ifNotExists;

    // Get all the tables affected
    return ObjectSelector.getForInsert(this._client, this.info, propertiesInfo)
      .then(tables => {

        if (tables.length > 1 && ifNotExists) {
          throw new Error('Batch with ifNotExists conditions cannot span multiple tables');
        }

        // For each tables affected, Generate query and parameter getters
        return tables.map(table =>
          QueryGenerator.getInsert(table, this.info.keyspace, propertiesInfo, docInfo,ifNotExists));
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
    if (docKeys.length === 0) {
      return Promise.reject(new Error('Expected object with keys'));
    }

    const cacheKey = Cache.getUpdateKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.update.getOrCreate(cacheKey, () => ({ executor: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    return this.createUpdateQueries(docKeys, doc, docInfo)
      .then(queries => {
        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries[0]);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  /**
   * Creates an Array containing the query and the params getter function for each table affected by the UPDATE.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {Object} docInfo
   * @returns {Promise<Array<{query, paramsGetter, isIdempotent}>>}
   */
  createUpdateQueries(docKeys, doc, docInfo) {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifExists = docInfo && docInfo.ifExists;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this.info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return ObjectSelector.getForUpdate(this._client, this.info, propertiesInfo, when)
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each table affected, Generate query and parameter getters
        return tables.map(table =>
          QueryGenerator.getUpdate(table, this.info.keyspace, propertiesInfo, docInfo, when, ifExists));
      });
  }

  /**
   * Gets a function to be used to execute the DELETE queries with the provided document.
   * @param {Object} doc
   * @param {{when, ifExists, fields, deleteOnlyColumns}} docInfo
   * @return {Promise<Function>}
   */
  getDeleteExecutor(doc, docInfo) {
    const docKeys = Object.keys(doc);
    if (docKeys.length === 0) {
      return Promise.reject(new Error('Expected object with keys'));
    }

    const cacheKey = Cache.getRemoveKey(docKeys, doc, docInfo);
    const cacheItem = this._cache.remove.getOrCreate(cacheKey, () => ({ executor: null }));

    if (cacheItem.executor !== null) {
      return Promise.resolve(cacheItem.executor);
    }

    return this.createDeleteQueries(docKeys, doc, docInfo)
      .then(queries => {
        if (queries.length === 1) {
          return this._setSingleExecutor(cacheItem, queries[0]);
        }

        return this._setBatchExecutor(cacheItem, queries);
      });
  }

  /**
   * Creates an Array containing the query and the params getter function for each table affected by the DELETE.
   * @param {Array<String>} docKeys
   * @param {Object} doc
   * @param {{when, ifExists, fields, deleteOnlyColumns}} docInfo
   * @returns {Promise<Array<{query, paramsGetter}>>}
   */
  createDeleteQueries(docKeys, doc, docInfo) {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifExists = docInfo && docInfo.ifExists;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this.info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return ObjectSelector.getForDelete(this._client, this.info, propertiesInfo, when)
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each tables affected, Generate query and parameter getters
        return tables.map(table =>
          QueryGenerator.getDelete(table, this.info.keyspace, propertiesInfo, docInfo, when, ifExists));
      });
  }

  getExecutorFromQuery(query, paramsHandler, commonExecutionOptions) {
    // Use the current instance in the closure
    // as there is no guarantee of how the returned function will be invoked
    const self = this;
    const commonOptions = commonExecutionOptions ? DocInfoAdapter.adaptAllOptions(commonExecutionOptions) : null;

    return (function queryMappedExecutor(doc, executionOptions) {
      // When the executionOptions were already specified,
      // use it and skip the ones provided in each invocation
      const options = commonOptions
        ? commonOptions
        : DocInfoAdapter.adaptAllOptions(executionOptions);

      return self._client.execute(query, paramsHandler(doc), options).then(rs => {
        // Cache the resultAdapter based on the query
        let resultAdapter = self._cache.customQueries.get(query);

        if (resultAdapter === undefined) {
          const resultAdapterInfo = ResultMapper.getCustomQueryAdapter(self.info, rs);
          resultAdapter = resultAdapterInfo.fn;
          if (resultAdapterInfo.canCache) {
            // Avoid caching conditional updates results as the amount of columns change
            // depending on the parameter values.
            self._cache.customQueries.set(query, resultAdapter);
          }
        }

        return new Result(rs, self.info, resultAdapter);
      });
    });
  }

  _setSingleExecutor(cacheItem, queryInfo) {
    // Parameters and this instance are part of the closure
    const self = this;

    // Set the function to execute the request in the cache
    cacheItem.executor = function singleExecutor(doc, docInfo, executionOptions) {
      const options = DocInfoAdapter.adaptOptions(executionOptions, queryInfo.isIdempotent);

      return self._client.execute(queryInfo.query, queryInfo.paramsGetter(doc, docInfo), options)
        .then(rs => new Result(rs, self.info, ResultMapper.getMutationAdapter(rs)));
    };

    return cacheItem.executor;
  }

  _setBatchExecutor(cacheItem, queries) {
    // Parameters and the following fields are part of the closure
    const self = this;
    const isIdempotent = queries.reduce((acc, q) => acc && q.isIdempotent, true);

    // Set the function to execute the batch request in the cache
    cacheItem.executor = function batchExecutor(doc, docInfo, executionOptions) {
      // Use the params getter function to obtain the parameters each time
      const queryAndParams = queries.map(q => ({
        query: q.query,
        params: q.paramsGetter(doc, docInfo)
      }));

      const options = DocInfoAdapter.adaptOptions(executionOptions, isIdempotent);

      // Execute using a Batch
      return self._client.batch(queryAndParams, options)
        .then(rs => new Result(rs, self.info, ResultMapper.getMutationAdapter(rs)));
    };

    return cacheItem.executor;
  }
}

module.exports = MappingHandler;