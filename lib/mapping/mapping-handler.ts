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
import type { FindDocInfo, InsertDocInfo, RemoveDocInfo, UpdateDocInfo } from ".";
import type Client from "../client";
import utils from "../utils";
import Cache from "./cache";
import DocInfoAdapter from "./doc-info-adapter";
import type ModelMappingInfo from "./model-mapping-info";
import ObjectSelector from "./object-selector";
import QueryGenerator from "./query-generator";
import Result from "./result";
import ResultMapper from "./result-mapper";
import Tree from "./tree";


const cacheHighWaterMark = 100;

/**
 * @ignore @internal
 */
class MappingHandler {
  private _client: Client;
  private _cache: { select: Tree; selectAll: Tree; insert: Tree; update: Tree; remove: Tree; customQueries: Map<any, any>; };
  info: ModelMappingInfo;
  /**
   * @param {Client} client
   * @param {ModelMappingInfo} mappingInfo
   */
  constructor(client: Client, mappingInfo: ModelMappingInfo) {
    this._client = client;
    this._cache = {
      select: new Tree().on('add', length => this._validateCacheLength(length)),
      selectAll: new Tree().on('add', length => this._validateCacheLength(length)),
      insert: new Tree().on('add', length => this._validateCacheLength(length)),
      update: new Tree().on('add', length => this._validateCacheLength(length)),
      remove: new Tree().on('add', length => this._validateCacheLength(length)),
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
  getSelectExecutor(doc: object, docInfo: FindDocInfo, allPKsDefined: boolean): Promise<Function> {
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
    const fieldsInfo = DocInfoAdapter.getPropertiesInfo(utils.emptyArray as any[], docInfo, doc, this.info);
    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this.info);
    const limit = docInfo && docInfo.limit;

    return this._client.connect()
      .then(() =>
        ObjectSelector.getForSelect(this._client, this.info, allPKsDefined, propertiesInfo, fieldsInfo, orderByColumns))
      .then(tableName => {
        // Part of the closure
        const query = QueryGenerator.getSelect(tableName, this.info.keyspace, propertiesInfo, fieldsInfo,
          orderByColumns, limit);
        const paramsGetter = QueryGenerator.selectParamsGetter(propertiesInfo, limit);
        const self = this;

        cacheItem.executor = function selectExecutor(doc, docInfo, executionOptions) {
          return self._executeSelect(query, paramsGetter, doc, docInfo, executionOptions, cacheItem);
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

    const fieldsInfo = DocInfoAdapter.getPropertiesInfo(utils.emptyArray as any[], docInfo, utils.emptyObject, this.info);
    const orderByColumns = DocInfoAdapter.adaptOrderBy(docInfo, this.info);
    const limit = docInfo && docInfo.limit;

    const tableName = ObjectSelector.getForSelectAll(this.info);

    // Part of the closure
    const query = QueryGenerator.getSelect(
      tableName, this.info.keyspace, utils.emptyArray as any[], fieldsInfo, orderByColumns, limit);
    const paramsGetter = QueryGenerator.selectParamsGetter(utils.emptyArray, limit);
    const self = this;

    cacheItem.executor = function selectAllExecutor(docInfo, executionOptions) {
      return self._executeSelect(query, paramsGetter, null, docInfo, executionOptions, cacheItem);
    };

    return cacheItem.executor;
  }

  /**
   * Executes a SELECT query and returns the adapted results.
   * When a result adapter is not yet created, it gets a new one and caches it.
   * @private
   */
  private _executeSelect(query, paramsGetter, doc, docInfo, executionOptions, cacheItem) {
    const options = DocInfoAdapter.adaptAllOptions(executionOptions, true);

    return this._client.execute(query, paramsGetter(doc, docInfo, this.info), options)
      .then(rs => {
        if (cacheItem.resultAdapter === null) {
          cacheItem.resultAdapter = ResultMapper.getSelectAdapter(this.info, rs);
        }
        return new Result(rs, this.info, cacheItem.resultAdapter);
      });
  }

  /**
   * Gets a function to be used to execute INSERT the query using the document.
   * @param {Object} doc
   * @param {{ifNotExists, ttl, fields}} docInfo
   * @return {Promise<Function>}
   */
  getInsertExecutor(doc: object, docInfo: InsertDocInfo): Promise<Function> {
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
  createInsertQueries(docKeys: Array<string>, doc: object, docInfo: InsertDocInfo): Promise<Array<{ query; paramsGetter; }>> {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifNotExists = docInfo && docInfo.ifNotExists;

    // Get all the tables affected
    return this._client.connect()
      .then(() => ObjectSelector.getForInsert(this._client, this.info, propertiesInfo))
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
  getUpdateExecutor(doc: object, docInfo: UpdateDocInfo): Promise<Function> {
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
  createUpdateQueries(docKeys: Array<string>, doc: object, docInfo: UpdateDocInfo): Promise<Array<{ query; paramsGetter; isIdempotent; }>> {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifExists = docInfo && docInfo.ifExists;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this.info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return this._client.connect()
      .then(() => ObjectSelector.getForUpdate(this._client, this.info, propertiesInfo, when as any[]))
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each table affected, Generate query and parameter getters
        return tables.map(table =>
          QueryGenerator.getUpdate(table, this.info.keyspace, propertiesInfo, docInfo, when as any[], ifExists));
      });
  }

  /**
   * Gets a function to be used to execute the DELETE queries with the provided document.
   * @param {Object} doc
   * @param {{when, ifExists, fields, deleteOnlyColumns}} docInfo
   * @return {Promise<Function>}
   */
  getDeleteExecutor(doc: object, docInfo: RemoveDocInfo): Promise<Function> {
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
  createDeleteQueries(docKeys: Array<string>, doc: object, docInfo: RemoveDocInfo): Promise<Array<{ query; paramsGetter; }>> {
    const propertiesInfo = DocInfoAdapter.getPropertiesInfo(docKeys, docInfo, doc, this.info);
    const ifExists = docInfo && docInfo.ifExists;
    const when = docInfo && docInfo.when
      ? DocInfoAdapter.getPropertiesInfo(Object.keys(docInfo.when), null, docInfo.when, this.info)
      : utils.emptyArray;

    if (when.length > 0 && ifExists) {
      throw new Error('Both when and ifExists conditions can not be applied to the same statement');
    }

    // Get all the tables affected
    return this._client.connect()
      .then(() => ObjectSelector.getForDelete(this._client, this.info, propertiesInfo, when as any[]))
      .then(tables => {

        if (tables.length > 1 && (when.length > 0 || ifExists)) {
          throw new Error('Batch with when or ifExists conditions cannot span multiple tables');
        }

        // For each tables affected, Generate query and parameter getters
        return tables.map(table =>
          QueryGenerator.getDelete(table, this.info.keyspace, propertiesInfo, docInfo, when as any[], ifExists));
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

            if (self._cache.customQueries.size === cacheHighWaterMark) {
              self._client.log('warning',
                `Custom queries cache reached ${cacheHighWaterMark} items, this could be caused by ` +
                `hard-coding parameter values inside the query, which should be avoided`);
            }
          }
        }

        return new Result(rs, self.info, resultAdapter);
      });
    });
  }

  private _setSingleExecutor(cacheItem, queryInfo) {
    // Parameters and this instance are part of the closure
    const self = this;

    // Set the function to execute the request in the cache
    cacheItem.executor = function singleExecutor(doc, docInfo, executionOptions) {
      const options = DocInfoAdapter.adaptOptions(executionOptions, queryInfo.isIdempotent);

      return self._client.execute(queryInfo.query, queryInfo.paramsGetter(doc, docInfo, self.info), options)
        .then(rs => new Result(rs, self.info, ResultMapper.getMutationAdapter(rs)));
    };

    return cacheItem.executor;
  }

  private _setBatchExecutor(cacheItem, queries) {
    // Parameters and the following fields are part of the closure
    const self = this;
    const isIdempotent = queries.reduce((acc, q) => acc && q.isIdempotent, true);

    // Set the function to execute the batch request in the cache
    cacheItem.executor = function batchExecutor(doc, docInfo, executionOptions) {
      // Use the params getter function to obtain the parameters each time
      const queryAndParams = queries.map(q => ({
        query: q.query,
        params: q.paramsGetter(doc, docInfo, self.info)
      }));

      const options = DocInfoAdapter.adaptOptions(executionOptions, isIdempotent);

      // Execute using a Batch
      return self._client.batch(queryAndParams, options)
        .then(rs => new Result(rs, self.info, ResultMapper.getMutationAdapter(rs)));
    };

    return cacheItem.executor;
  }

  private _validateCacheLength(length) {
    if (length !== cacheHighWaterMark) {
      return;
    }

    this._client.log('warning', `ModelMapper cache reached ${cacheHighWaterMark} items, this could be caused by ` +
      `building the object to map in different ways (with different shapes) each time. Use the same or few object ` +
      `structures for a model and represent unset values with undefined or types.unset`);
  }
}

export default MappingHandler;