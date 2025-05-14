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
import Long from 'long';
import Mapper from './mapper';
import { ModelBatchItem } from './model-batch-item';
import ModelBatchMapper from "./model-batch-mapper";
import ModelMapper from "./model-mapper";
import { q } from "./q";
import Result from "./result";
import { DefaultTableMappings, TableMappings, UnderscoreCqlToCamelCaseMappings } from "./table-mappings";

type MappingOptions = {
  models: { [key: string]: ModelOptions };
}

type ModelOptions = {
  tables?: string[] | ModelTables[];
  mappings?: TableMappings;
  columns?: { [key: string]: string|ModelColumnOptions };
  keyspace?: string;
}

type ModelColumnOptions = {
  name: string;
  toModel?: (columnValue: any) => any;
  fromModel?: (modelValue: any) => any;
};

interface ModelTables {
  name: string;
  isView: boolean;
}

type MappingExecutionOptions = {
  executionProfile?: string;
  isIdempotent?: boolean;
  logged?: boolean;
  timestamp?: number | Long;
  fetchSize?: number;
  pageState?: number;
}

type FindDocInfo = {
  fields?: string[];
  orderBy?: { [key: string]: string };
  limit?: number;
}

type InsertDocInfo = {
  fields?: string[];
  ttl?: number;
  ifNotExists?: boolean;
}

type UpdateDocInfo = {
  fields?: string[];
  ttl?: number;
  ifExists?: boolean;
  when?: { [key: string]: any };
  orderBy?: { [key: string]: string };
  limit?: number;
  deleteOnlyColumns?: boolean;
}

type RemoveDocInfo = {
  fields?: string[];
  ttl?: number;
  ifExists?: boolean;
  when?: { [key: string]: any };
  deleteOnlyColumns?: boolean;
}


/**
 * Module containing classes and fields related to the Mapper.
 * @module mapping
 */

export {
  DefaultTableMappings, Mapper, ModelBatchItem, ModelBatchMapper, ModelColumnOptions, ModelMapper, ModelTables, q, Result,
  TableMappings, UnderscoreCqlToCamelCaseMappings, type FindDocInfo,
  type InsertDocInfo, type MappingExecutionOptions, type MappingOptions,
  type ModelOptions, type RemoveDocInfo, type UpdateDocInfo
};

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
};