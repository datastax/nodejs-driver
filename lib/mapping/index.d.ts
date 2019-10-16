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

  interface Result<T = any> extends Iterator<T> {
    wasApplied(): boolean;

    first(): T | null;

    forEach(callback: (currentValue: T, index: number) => void, thisArg?: any): void;

    toArray(): T[];
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

    forModel<T = any>(name: string): ModelMapper<T>;
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

  interface ModelMapper<T = any> {
    name: string;
    batching: ModelBatchMapper;

    get(doc: { [key: string]: any }, docInfo?: { fields?: string[] }, executionOptions?: string | MappingExecutionOptions): Promise<null | T>;

    find(doc: { [key: string]: any }, docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;

    findAll(docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;

    insert(doc: { [key: string]: any }, docInfo?: InsertDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;

    update(doc: { [key: string]: any }, docInfo?: UpdateDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;

    remove(doc: { [key: string]: any }, docInfo?: RemoveDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;

    mapWithQuery(
      query: string,
      paramsHandler: (doc: any) => any[],
      executionOptions?: string | MappingExecutionOptions
    ): (doc: any, executionOptions?: string | MappingExecutionOptions) => Promise<Result<T>>;
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