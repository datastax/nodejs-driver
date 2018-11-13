'use strict';

const ModelBatchMapper = require('./model-batch-mapper');

/**
 * Represents an object mapper for a specific model.
 * @alias module:mapping~ModelMapper
 */
class ModelMapper {
  constructor(name, handler) {
    /**
     * Gets the name identifier of the model.
     * @type {String}
     */
    this.name = name;
    this._handler = handler;
    /**
     * Gets a [ModelBatchMapper]{@link module:mapping~ModelBatchMapper} instance containing utility methods to group
     * multiple doc mutations in a single batch.
     * @type {ModelBatchMapper}
     */
    this.batching = new ModelBatchMapper(this._handler);
  }

  /**
   * Gets the first document matching the provided filter or null when not found.
   * <p>
   *   Note that all partition and clustering keys must be defined in order to use this method.
   * </p>
   * @param {Object} doc The object containing the properties that map to the primary keys.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @return {Promise<Object>}
   * @example <caption>Get a video by id</caption>
   * videoMapper.get({ id })
   * @example <caption>Get a video by id, selecting specific columns</caption>
   * videoMapper.get({ id }, fields: ['name', 'description'])
   */
  get(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getSelectExecutor(doc, docInfo, true)
      .then(executor => executor(doc, docInfo, executionOptions))
      .then(result => result.first());
  }

  /**
   * Executes a SELECT query based on the filter and returns the result as an iterable of documents.
   * @param {Object} doc An object containing the properties that map to the primary keys to filter.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
   * @param {Object<String, String>} [docInfo.orderBy] An associative array containing the column names as key and
   * the order string (asc or desc) as value used to set the order of the results server-side.
   * @param {Number} [docInfo.limit] Restricts the result of the query to a maximum number of rows on the
   * server.
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Number} [executionOptions.fetchSize] The amount of rows to retrieve per page.
   * @param {Number} [executionOptions.pageState] A Buffer instance or a string token representing the paging state.
   * <p>When provided, the query will be executed starting from a given paging state.</p>
   * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
   * @example <caption>Get user's videos</caption>
   * const result = await videoMapper.find({ userId });
   * for (let video of result) {
   *   console.log(video.name);
   * }
   * @example <caption>Get user's videos from a certain date</caption>
   * videoMapper.find({ userId, addedDate: q.gte(date)});
   * @example <caption>Get user's videos in reverse order</caption>
   * videoMapper.find({ userId }, { orderBy: { addedDate: 'desc' }});
   */
  find(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getSelectExecutor(doc, docInfo, false)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Executes a SELECT query without a filter and returns the result as an iterable of documents.
   * <p>
   *   This is only recommended to be used for tables with a limited amount of results. Otherwise, breaking up the
   *   token ranges on the client side should be used.
   * </p>
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
   * @param {Object<String, String>} [docInfo.orderBy] An associative array containing the column names as key and
   * the order string (asc or desc) as value used to set the order of the results server-side.
   * @param {Number} [docInfo.limit] Restricts the result of the query to a maximum number of rows on the
   * server.
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Number} [executionOptions.fetchSize] The mount of rows to retrieve per page.
   * @param {Number} [executionOptions.pageState] A Buffer instance or a string token representing the paging state.
   * <p>When provided, the query will be executed starting from a given paging state.</p>
   * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
   */
  findAll(docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    const executor = this._handler.getSelectAllExecutor(docInfo);
    return executor(docInfo, executionOptions);
  }

  /**
   * Inserts a document.
   * <p>
   *   When the model is mapped to multiple tables, it will insert a row in each table when all the primary keys
   *   are specified.
   * </p>
   * @param {Object} doc An object containing the properties to insert.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * INSERT cql statements generated. If specified, it must include the columns to insert and the primary keys.
   * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
   * @param {Boolean} [docInfo.ifNotExists] When set, it only inserts if the row does not exist prior to the insertion.
   * <p>Please note that using IF NOT EXISTS will incur a non negligible performance cost so this should be used
   * sparingly.</p>
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   By default all generated INSERT statements are considered idempotent, except in the case of lightweight
   *   transactions. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
   * @example <caption>Insert a video</caption>
   * videoMapper.insert({ id, name });
   */
  insert(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getInsertExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Updates a document.
   * <p>
   *   When the model is mapped to multiple tables, it will update a row in each table when all the primary keys
   *   are specified.
   * </p>
   * @param {Object} doc An object containing the properties to update.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * UPDATE cql statements generated. If specified, it must include the columns to update and the primary keys.
   * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
   * @param {Boolean} [docInfo.ifExists] When set, it only updates if the row already exists on the server.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the UPDATE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   The mapper uses the generated queries to determine the default value. When an UPDATE is generated with a
   *   counter column or appending/prepending to a list column, the execution is marked as not idempotent.
   * </p>
   * <p>
   *   Additionally, the mapper uses the safest approach for queries with lightweight transactions (Compare and
   *   Set) by considering them as non-idempotent. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
   * @example <caption>Update the name of a video</caption>
   * videoMapper.update({ id, name });
   */
  update(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getUpdateExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Deletes a document.
   * @param {Object} doc A document containing the primary keys values of the document to delete.
   * @param {Object} [docInfo] An object containing the additional doc information.
   * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
   * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
   * When the CQL query is generated, this would be used to generate the `IF` clause.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Boolean} [docInfo.ifExists] When set, it only issues the DELETE command if the row already exists on the
   * server.
   * <p>
   *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
   *   should be used sparingly.
   * </p>
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
   * @param {Boolean} [docInfo.deleteOnlyColumns] Determines that, when more document properties are specified
   * besides the primary keys, the generated DELETE statement should be used to delete some column values but leave
   * the row. When this is enabled and more properties are specified, a DELETE statement will have the following form:
   * "DELETE col1, col2 FROM table1 WHERE pk1 = ? AND pk2 = ?"
   * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
   * execution or a string representing the name of the execution profile.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
   * changing the result beyond the initial application.
   * <p>
   *   By default all generated DELETE statements are considered idempotent, except in the case of lightweight
   *   transactions. Lightweight transactions at client level with transparent retries can
   *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
   * </p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
   * @example <caption>Delete a video</caption>
   * videoMapper.remove({ id });
   */
  remove(doc, docInfo, executionOptions) {
    if (executionOptions === undefined && typeof docInfo === 'string') {
      executionOptions = docInfo;
      docInfo = null;
    }

    return this._handler.getDeleteExecutor(doc, docInfo)
      .then(executor => executor(doc, docInfo, executionOptions));
  }

  /**
   * Uses the provided query and param getter function to execute a query and map the results.
   * Gets a function that takes the document, executes the query and returns the mapped results.
   * @param {String} query The query to execute.
   * @param {Function} paramsHandler The function to execute to extract the parameters of a document.
   * @param {Object|String} [executionOptions] When provided, the options for all executions generated with this
   * method will use the provided options and it will not consider the executionOptions per call.
   * @param {String} [executionOptions.executionProfile] The name of the execution profile.
   * @param {Number} [executionOptions.fetchSize] Amount of rows to retrieve per page.
   * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times
   * without changing the result beyond the initial application.
   * @param {Number} [executionOptions.pageState] Buffer or string token representing the paging state.
   * <p>When provided, the query will be executed starting from a given paging state.</p>
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
   * @return {Function} Returns a function that takes the document and execution options as parameters and returns a
   * Promise the resolves to a [Result]{@link module:mapping~Result} instance.
   */
  mapWithQuery(query, paramsHandler, executionOptions) {
    return this._handler.getExecutorFromQuery(query, paramsHandler, executionOptions);
  }
}

module.exports = ModelMapper;