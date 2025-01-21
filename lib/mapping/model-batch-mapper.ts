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

const Tree = require('./tree');
const moduleBatchItemModule = require('./model-batch-item');
const InsertModelBatchItem = moduleBatchItemModule.InsertModelBatchItem;
const UpdateModelBatchItem = moduleBatchItemModule.UpdateModelBatchItem;
const RemoveModelBatchItem = moduleBatchItemModule.RemoveModelBatchItem;

/**
 * Provides utility methods to group multiple mutations on a single batch.
 * @alias module:mapping~ModelBatchMapper
 */
class ModelBatchMapper {
  /**
   * Creates a new instance of model batch mapper.
   * <p>
   *   An instance of this class is exposed as a singleton in the <code>batching</code> field of the
   *   [ModelMapper]{@link module:mapping~ModelMapper}. Note that new instances should not be create with this
   *   constructor.
   * </p>
   * @param {MappingHandler} handler
   * @ignore
   */
  constructor(handler) {
    this._handler = handler;
    this._cache = {
      insert: new Tree(),
      update: new Tree(),
      remove: new Tree()
    };
  }

  /**
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the INSERT mutation to be
   * used in a batch execution.
   * @param {Object} doc An object containing the properties to insert.
   * @param {Object} [docInfo] An object containing the additional document information.
   * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
   * INSERT cql statements generated. If specified, it must include the columns to insert and the primary keys.
   * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
   * @param {Boolean} [docInfo.ifNotExists] When set, it only inserts if the row does not exist prior to the insertion.
   * <p>Please note that using IF NOT EXISTS will incur a non negligible performance cost so this should be used
   * sparingly.</p>
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  insert(doc, docInfo) {
    return new InsertModelBatchItem(doc, docInfo, this._handler, this._cache.insert);
  }

  /**
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the UPDATE mutation to be
   * used in a batch execution.
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
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  update(doc, docInfo) {
    return new UpdateModelBatchItem(doc, docInfo, this._handler, this._cache.update);
  }

  /**
   * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem}  containing the queries for the DELETE mutation to be
   * used in a batch execution.
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
   * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
   * or a set of queries to be included in a batch.
   */
  remove(doc, docInfo) {
    return new RemoveModelBatchItem(doc, docInfo, this._handler, this._cache.update);
  }
}

module.exports = ModelBatchMapper;