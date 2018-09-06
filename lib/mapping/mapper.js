'use strict';

const ModelMapper = require('./model-mapper');
const MappingHandler = require('./mapping-handler');
const DocInfoAdapter = require('./doc-info-adapter');
const errors = require('../errors');
const Result = require('./result');
const ResultMapper = require('./result-mapper');
const ModelMappingInfo = require('./model-mapping-info');
const ModelBatchItem = require('./model-batch-item');

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
    if (!client) {
      throw new Error('client must be defined');
    }

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
   * @param {Boolean} [executionOptions.logged=true] Determines whether the batch should be written to the batchlog.
   * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
   * unix epoch (00:00:00, January 1st, 1970).
   * @returns {Promise<Result>}
   */
  batch(items, executionOptions) {
    if (!Array.isArray(items) || !(items.length > 0)) {
      return Promise.reject(
        new errors.ArgumentError('First parameter items should be an Array with 1 or more ModelBatchItem instances'));
    }

    const queries = [];
    let isIdempotent = true;

    return Promise
      .all(items
        .map(item => {
          if (!(item instanceof ModelBatchItem)) {
            return Promise.reject(new Error(
              'Batch items must be instances of ModelBatchItem, use modelMapper.batching object to create each item'));
          }
          return item.pushQueries(queries).then(isIdempotent = isIdempotent && item.isIdempotent);
        }))
      .then(() => {
        // For batched counter updates, all queries should be updating counters
        // check the first one should be enough
        // It will fail at server level if there is a mix of counter and normal mutations
        const isCounter = items[0].isCounter;

        return this.client.batch(queries, DocInfoAdapter.adaptBatchOptions(executionOptions, isIdempotent, isCounter));
      })
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
 * @property {Object.<String, ModelOptions>} models An associative array containing the
 * name of the model as key and the table and column information as value.
 * @example <caption>Example containing all the possible options for a model</caption>
 * const mappingOptions = {
 *   models: {
 *     'Video': {
 *       tables: ['videos', 'user_videos', 'latest_videos', { name: 'my_videos_view', isView: true }],
 *       mappings: new UnderscoreCqlToCamelCaseMappings(),
 *       columnNames: {
 *         'videoid': 'id'
 *       },
 *       keyspace: 'ks1'
 *     }
 *   }
 * };
 * const mapper = new Mapper(client, mappingOptions);
 * @example <caption>Example containing fewer options for a model</caption>
 * const mappingOptions = {
 *   models: {
 *     'User': {
 *       tables: ['users'],
 *       mappings: new UnderscoreCqlToCamelCaseMappings(),
 *       columnNames: {
 *         'userid': 'id'
 *       }
 *     }
 *   }
 * };
 * const mapper = new Mapper(client, mappingOptions);
 */

/**
 * Represents a set of options that applies to a certain model.
 * @typedef {Object} ModelOptions
 * @property {Array<String>|Array<{name, isView}>} tables An Array containing the name of the tables or An Array
 * containing the name and isView property to describe the table.
 * @property {TableMappings} mappings The TableMappings implementation instance that is used to convert from column
 * names to property names and the other way around.
 * @property {Object.<String, String>} [columnNames] An associative array containing the name of the columns and
 * properties that doesn't follow the convention defined in the <code>TableMappings</code>.
 * @property {String} [keyspace] The name of the keyspace. Only mandatory when the Client is not using a keyspace.
 */

module.exports = Mapper;