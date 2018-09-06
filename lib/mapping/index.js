'use strict';

/**
 * Module containing classes and fields related to the Mapper.
 * @module mapping
 */

exports.Mapper = require('./mapper');
exports.ModelMapper = require('./model-mapper');
exports.ModelBatchMapper = require('./model-batch-mapper');
exports.ModelBatchItem = require('./model-batch-item');
exports.Result = require('./result');
const tableMappingsModule = require('./table-mappings');
exports.TableMappings = tableMappingsModule.TableMappings;
exports.DefaultTableMappings = tableMappingsModule.DefaultTableMappings;
exports.UnderscoreCqlToCamelCaseMappings = tableMappingsModule.UnderscoreCqlToCamelCaseMappings;
exports.q = require('./q').q;