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
import { EmptyCallback, Host, token, ValueCallback } from '../../';
import dataTypes = types.dataTypes;
import Uuid = types.Uuid;
import InetAddress = types.InetAddress;

export namespace metadata {

  interface Aggregate {
    argumentTypes: Array<{ code: dataTypes, info: any }>;
    finalFunction: string;
    initCondition: string;
    keyspaceName: string;
    returnType: string;
    signature: string[];
    stateFunction: string;
    stateType: string;
  }

  interface ClientState {
    getConnectedHosts(): Host[];

    getInFlightQueries(host: Host): number;

    getOpenConnections(host: Host): number;

    toString(): string;
  }

  interface DataTypeInfo {
    code: dataTypes;
    info: string | DataTypeInfo | DataTypeInfo[];
    options: {
      frozen: boolean;
      reversed: boolean;
    };
  }

  interface ColumnInfo {
    name: string;
    type: DataTypeInfo;
  }

  enum IndexKind {
    custom = 0,
    keys,
    composites
  }

  interface Index {
    kind: IndexKind;
    name: string;
    options: object;
    target: string;

    isCompositesKind(): boolean;

    isCustomKind(): boolean;

    isKeysKind(): boolean;
  }

  interface DataCollection {
    bloomFilterFalsePositiveChance: number;
    caching: string;
    clusteringKeys: ColumnInfo[];
    clusteringOrder: string[];
    columns: ColumnInfo[];
    columnsByName: { [key: string]: ColumnInfo };
    comment: string;
    compactionClass: string;
    compactionOptions: { [option: string]: any; };
    compression: {
      class?: string;
      [option: string]: any;
    };
    crcCheckChange?: number;
    defaultTtl: number;
    extensions: { [option: string]: any; };
    gcGraceSeconds: number;
    localReadRepairChance: number;
    maxIndexInterval?: number;
    minIndexInterval?: number;
    name: string;
    partitionKeys: ColumnInfo[];
    populateCacheOnFlush: boolean;
    readRepairChance: number;
    speculativeRetry: string;
  }

  interface MaterializedView extends DataCollection {
    tableName: string;
    whereClause: string;
    includeAllColumns: boolean;
  }

  interface TableMetadata extends DataCollection {
    indexes: Index[];
    indexInterval?: number;
    isCompact: boolean;
    memtableFlushPeriod: number;
    replicateOnWrite: boolean;
    cdc?: boolean;
    virtual: boolean;
  }

  interface QueryTrace {
    requestType: string;
    coordinator: InetAddress;
    parameters: { [key: string]: any };
    startedAt: number | types.Long;
    duration: number;
    clientAddress: string;
    events: Array<{ id: Uuid; activity: any; source: any; elapsed: any; thread: any }>;
  }

  interface SchemaFunction {
    argumentNames: string[];
    argumentTypes: Array<{ code: dataTypes, info: any }>;
    body: string;
    calledOnNullInput: boolean;
    keyspaceName: string;
    language: string;
    name: string;
    returnType: string;
    signature: string[];
  }

  interface Udt {
    name: string;
    fields: ColumnInfo[]
  }

  interface Metadata {
    keyspaces: { [name: string]: { name: string, strategy: string }};

    clearPrepared(): void;

    getAggregate(keyspaceName: string, name: string, signature: string[] | Array<{ code: number, info: any }>, callback: ValueCallback<Aggregate>): void;

    getAggregate(keyspaceName: string, name: string, signature: string[] | Array<{ code: number, info: any }>): Promise<Aggregate>;

    getAggregates(keyspaceName: string, name: string, callback: ValueCallback<Aggregate[]>): void;

    getAggregates(keyspaceName: string, name: string): Promise<Aggregate[]>;

    getFunction(keyspaceName: string, name: string, signature: string[] | Array<{ code: number, info: any }>, callback: ValueCallback<SchemaFunction>): void;

    getFunction(keyspaceName: string, name: string, signature: string[] | Array<{ code: number, info: any }>): Promise<SchemaFunction>;

    getFunctions(keyspaceName: string, name: string, callback: ValueCallback<SchemaFunction[]>): void;

    getFunctions(keyspaceName: string, name: string): Promise<SchemaFunction[]>;

    getMaterializedView(keyspaceName: string, name: string, callback: ValueCallback<MaterializedView>): void;

    getMaterializedView(keyspaceName: string, name: string, callback: EmptyCallback): Promise<MaterializedView>;

    getReplicas(keyspaceName: string, token: Buffer | token.Token | token.TokenRange): Host[];

    getTable(keyspaceName: string, name: string, callback: ValueCallback<TableMetadata>): void;

    getTable(keyspaceName: string, name: string): Promise<TableMetadata>;

    getTokenRanges(): Set<token.TokenRange>;

    getTokenRangesForHost(keyspaceName: string, host: Host): Set<token.TokenRange> | null;

    getTrace(traceId: Uuid, consistency: types.consistencies, callback: ValueCallback<QueryTrace>): void;

    getTrace(traceId: Uuid, consistency: types.consistencies): Promise<QueryTrace>;

    getTrace(traceId: Uuid, callback: ValueCallback<QueryTrace>): void;

    getTrace(traceId: Uuid): Promise<QueryTrace>;

    getUdt(keyspaceName: string, name: string, callback: ValueCallback<Udt>): void;

    getUdt(keyspaceName: string, name: string): Promise<Udt>;

    newToken(components: Buffer[] | Buffer | string): token.Token;

    newTokenRange(start: token.Token, end: token.Token): token.TokenRange;

    refreshKeyspace(name: string, callback: EmptyCallback): void;

    refreshKeyspace(name: string): Promise<void>;

    refreshKeyspaces(waitReconnect: boolean, callback: EmptyCallback): void;

    refreshKeyspaces(waitReconnect?: boolean): Promise<void>;

    refreshKeyspaces(callback: EmptyCallback): void;
  }
}