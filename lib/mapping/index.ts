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

/**
 * Module containing classes and fields related to the Mapper.
 * @module mapping
 */

exports.Mapper = require('./mapper');
exports.ModelMapper = require('./model-mapper');
exports.ModelBatchMapper = require('./model-batch-mapper');
exports.ModelBatchItem = require('./model-batch-item').ModelBatchItem;
exports.Result = require('./result');
const tableMappingsModule = require('./table-mappings');
exports.TableMappings = tableMappingsModule.TableMappings;
exports.DefaultTableMappings = tableMappingsModule.DefaultTableMappings;
exports.UnderscoreCqlToCamelCaseMappings = tableMappingsModule.UnderscoreCqlToCamelCaseMappings;
exports.q = require('./q').q;