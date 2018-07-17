'use strict';

const ModelMapper = require('./model-mapper');
const MappingHandler = require('./mapping-handler');

class Mapper {
  /**
   * @param {Client} client
   */
  constructor(client) {
    this._client = client;
  }

  /**
   * @param {String} name
   * @param {TableMappingInfo} tableMappingInfo
   * @return {ModelMapper}
   */
  forModel(name, tableMappingInfo) {
    // TODO: singleton
    // TODO: remove parameter
    return new ModelMapper(name, new MappingHandler(this._client, tableMappingInfo));
  }
}

module.exports = Mapper;