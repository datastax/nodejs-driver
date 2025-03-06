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
import Mapper from './mapper';
import ModelMapper from "./model-mapper";
import ModelBatchMapper from "./model-batch-mapper";
import { ModelBatchItem } from './model-batch-item';
import Result from "./result";
import { q } from "./q";
import {TableMappings, DefaultTableMappings, UnderscoreCqlToCamelCaseMappings} from "./table-mappings";

'use strict';

/**
 * Module containing classes and fields related to the Mapper.
 * @module mapping
 */

export {
    Mapper,
    ModelMapper,
    ModelBatchMapper,
    ModelBatchItem,
    Result,
    TableMappings,
    DefaultTableMappings,
    UnderscoreCqlToCamelCaseMappings,
    q
}

export default {
    Mapper,
    ModelMapper,
    ModelBatchMapper,
    ModelBatchItem,
    Result,
    TableMappings,
    DefaultTableMappings,
    UnderscoreCqlToCamelCaseMappings,
    q
}