'use strict';

const ModelMapper = require('./model-mapper');
const MappingHandler = require('./mapping-handler');

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
   * @param {String} name
   * @param {TableMappingInfo} tableMappingInfo
   * @return {ModelMapper}
   */
  forModel(name, tableMappingInfo) {
    // TODO: singleton
    // TODO: remove parameter tableMappingInfo
    return new ModelMapper(name, new MappingHandler(this.client, tableMappingInfo));
  }
}

module.exports = Mapper;