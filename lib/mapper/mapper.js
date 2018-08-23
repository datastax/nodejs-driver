'use strict';

const ModelMapper = require('./model-mapper');
const MappingHandler = require('./mapping-handler');
const DocInfoAdapter = require('./doc-info-adapter');
const errors = require('../errors');

class Mapper {
  /**
   * Creates a new instance of Mapper.
   * @param {Client} client
   */
  constructor(client) {
    /**
     * The Client instance used to create this Mapper instance.
     * @type {Client}
     */
    this.client = client;
  }

  /**
   * Gets a <code>ModelMapper</code> that is able to map documents of a certain model into CQL rows.
   * @param {String} name
   * @param {TableMappingInfo} tableMappingInfo
   * @return {ModelMapper}
   */
  forModel(name, tableMappingInfo) {
    // TODO: singleton
    // TODO: remove parameter tableMappingInfo
    return new ModelMapper(name, new MappingHandler(this.client, tableMappingInfo));
  }

  /**
   * Executes a batch of queries represented in the items.
   * @param {Array<ModelBatchItem>} items
   * @param {String|{executionProfile, executeAs, timestamp}} [executionOptions]
   * @returns {Promise<Result>}
   */
  batch(items, executionOptions) {
    if (!Array.isArray(items) || !(items.length > 0)) {
      return Promise.reject(
        new errors.ArgumentError('First parameter items should be an Array with 1 or more ModelBatchItem instances'));
    }

    const queries = [];
    return Promise.all(items.map(item => item.pushQueries(queries)))
      .then(() => this.client.batch(queries, DocInfoAdapter.adaptOptions(executionOptions)));
  }
}

module.exports = Mapper;