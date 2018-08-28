'use strict';

const ModelMapper = require('./model-mapper');
const MappingHandler = require('./mapping-handler');
const DocInfoAdapter = require('./doc-info-adapter');
const errors = require('../errors');
const Result = require('./result');
const ResultMapper = require('./result-mapper');
const ModelMappingInfo = require('./model-mapping-info');

/**
 * Represents object mapper for Apache Cassandra and DataStax Enterprise.
 */
class Mapper {
  /**
   * Creates a new instance of Mapper.
   * @param {Client} client
   * @param {MappingOptions} [options]
   */
  constructor(client, options) {
    /**
     * The Client instance used to create this Mapper instance.
     * @type {Client}
     */
    this.client = client;

    this._modelMappingInfos = ModelMappingInfo.parse(options, client.keyspace);
    this._modelMappers = new Map();
  }

  /**
   * Gets a <code>ModelMapper</code> that is able to map documents of a certain model into CQL rows.
   * @param {String} name
   * @return {ModelMapper}
   */
  forModel(name) {
    let modelMapper = this._modelMappers.get(name);

    if (modelMapper === undefined) {
      let mappingInfo = this._modelMappingInfos.get(name);

      if (mappingInfo === undefined) {
        if (!this.client.keyspace) {
          throw new Error(
            'You must set the Client keyspace or specify the keyspace of the model in the MappingOptions');
        }

        mappingInfo = ModelMappingInfo.createDefault(name, this.client.keyspace);
      }

      modelMapper = new ModelMapper(name, new MappingHandler(this.client, mappingInfo));
      this._modelMappers.set(name, modelMapper);
    }

    return modelMapper;
  }

  /**
   * Executes a batch of queries represented in the items.
   * @param {Array<ModelBatchItem>} items
   * @param {String|{executionProfile, executeAs, timestamp, logged}} [executionOptions]
   * @returns {Promise<Result>}
   */
  batch(items, executionOptions) {
    if (!Array.isArray(items) || !(items.length > 0)) {
      return Promise.reject(
        new errors.ArgumentError('First parameter items should be an Array with 1 or more ModelBatchItem instances'));
    }

    const queries = [];
    return Promise.all(items.map(item => item.pushQueries(queries)))
      .then(() => this.client.batch(queries, DocInfoAdapter.adaptBatchOptions(executionOptions)))
      .then(rs => {
        // Results should only be adapted when the batch contains LWT (single table)
        const info = items[0].getMappingInfo();
        return new Result(rs, info, ResultMapper.getMutationAdapter(rs));
      });
  }
}

/**
 * Represents the mapping options.
 * @typedef {Object} MappingOptions
 * @property {Object.<String, {tables, mappings, columnNames}>} models An associative array containing the name of the model as
 * key and
 * @example <caption>Example containing all the possible options for a model</caption>
 * const mappingOptions = {
 *   models: {
 *     'Video': {
 *       tables: ['videos', 'user_videos', 'latest_videos', { name: 'my_videos_view', isView: true }],
 *       mappings: new UnderscoreCqlToCamelCaseMappings(),
 *       columnNames: {
 *         'videoid': 'id'
 *       }
 *     }
 *   }
 * };
 *
 * const mapper = new Mapper(client, mappingOptions);
 */

module.exports = Mapper;