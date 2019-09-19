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

import { types } from '../types';
import { Client } from '../../';
import Long = types.Long;

export namespace mapping {
  interface TableMappings {
    getColumnName(propName: string): string;

    getPropertyName(columnName: string): string;

    newObjectInstance(): any;
  }

  class DefaultTableMappings implements TableMappings {
    getColumnName(propName: string): string;

    getPropertyName(columnName: string): string;

    newObjectInstance(): any;
  }

  class UnderscoreCqlToCamelCaseMappings implements TableMappings {
    getColumnName(propName: string): string;

    getPropertyName(columnName: string): string;

    newObjectInstance(): any;
  }

  interface Result extends Iterator<any> {
    wasApplied(): boolean;

    first(): any;

    forEach(callback: (currentValue: any, index: number) => void, thisArg?: any): void;

    toArray(): any[];
  }

  type MappingExecutionOptions = {
    executionProfile?: string;
    isIdempotent?: boolean;
    logged?: boolean;
    timestamp?: number | Long;
    fetchSize?: number;
    pageState?: number;
  }

  interface ModelTables {
    name: string;
    isView: boolean;
  }

  class Mapper {
    constructor(client: Client, options?: MappingOptions);

    batch(items: ModelBatchItem[], executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    forModel(name: string): ModelMapper;
  }

  type MappingOptions = {
    models: { [key: string]: ModelOptions };
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

  type ModelOptions = {
    tables?: string[] | ModelTables[];
    mappings?: TableMappings;
    columns?: { [key: string]: string };
    keyspace?: string;
  }

  interface ModelBatchItem {

  }

  interface ModelBatchMapper {
    insert(doc, docInfo?: InsertDocInfo): ModelBatchItem;

    remove(doc, docInfo?: RemoveDocInfo): ModelBatchItem;

    update(doc, docInfo?: UpdateDocInfo): ModelBatchItem;
  }

  interface ModelMapper {
    name: string;
    batching: ModelBatchMapper;

    get(doc, docInfo?: { fields?: string[] }, executionOptions?: string | MappingExecutionOptions): Promise<any>;

    find(doc, docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    findAll(docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    insert(doc, docInfo?: InsertDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    update(doc, docInfo?: UpdateDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    remove(doc, docInfo?: RemoveDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    mapWithQuery(
      query: string,
      paramsHandler: (doc) => any[],
      executionOptions?: string | MappingExecutionOptions
    ): (doc, executionOptions?: string | MappingExecutionOptions) => Promise<Result>;
  }

  namespace q {
    interface QueryOperator {

    }

    function in_(arr): QueryOperator;

    function gt(value): QueryOperator;

    function gte(value): QueryOperator;

    function lt(value): QueryOperator;

    function lte(value): QueryOperator;

    function notEq(value): QueryOperator;

    function and(condition1, condition2): QueryOperator;

    function incr(value): QueryOperator;

    function decr(value): QueryOperator;

    function append(value): QueryOperator;

    function prepend(value): QueryOperator;

    function remove(value): QueryOperator;
  }
}