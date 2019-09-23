/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    insert(doc: any, docInfo?: InsertDocInfo): ModelBatchItem;

    remove(doc: any, docInfo?: RemoveDocInfo): ModelBatchItem;

    update(doc: any, docInfo?: UpdateDocInfo): ModelBatchItem;
  }

  interface ModelMapper {
    name: string;
    batching: ModelBatchMapper;

    get(doc: any, docInfo?: { fields?: string[] }, executionOptions?: string | MappingExecutionOptions): Promise<any>;

    find(doc: any, docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    findAll(docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    insert(doc: any, docInfo?: InsertDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    update(doc: any, docInfo?: UpdateDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    remove(doc: any, docInfo?: RemoveDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result>;

    mapWithQuery(
      query: string,
      paramsHandler: (doc: any) => any[],
      executionOptions?: string | MappingExecutionOptions
    ): (doc: any, executionOptions?: string | MappingExecutionOptions) => Promise<Result>;
  }

  namespace q {
    interface QueryOperator {

    }

    function in_(arr: any): QueryOperator;

    function gt(value: any): QueryOperator;

    function gte(value: any): QueryOperator;

    function lt(value: any): QueryOperator;

    function lte(value: any): QueryOperator;

    function notEq(value: any): QueryOperator;

    function and(condition1: any, condition2: any): QueryOperator;

    function incr(value: any): QueryOperator;

    function decr(value: any): QueryOperator;

    function append(value: any): QueryOperator;

    function prepend(value: any): QueryOperator;

    function remove(value: any): QueryOperator;
  }
}