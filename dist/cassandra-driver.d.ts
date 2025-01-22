import * as events from 'events';
import _Long = require('long');
import { Readable } from 'stream';
import * as stream from 'stream';
import * as tls from 'tls';
import { URL as URL_2 } from 'url';

export declare type ArrayOrObject = any[]|{[key: string]: any};

export declare namespace auth {
    export interface Authenticator {
        initialResponse(callback: Function): void;

        evaluateChallenge(challenge: Buffer, callback: Function): void;

        onAuthenticationSuccess(token?: Buffer): void;
    }

    export interface AuthProvider {
        newAuthenticator(endpoint: string, name: string): Authenticator;
    }

    export class PlainTextAuthProvider implements AuthProvider {
        constructor(username: string, password: string);

        newAuthenticator(endpoint: string, name: string): Authenticator;
    }

    export class DsePlainTextAuthProvider implements AuthProvider {
        constructor(username: string, password: string, authorizationId?: string);

        newAuthenticator(endpoint: string, name: string): Authenticator;
    }

    export class DseGssapiAuthProvider implements AuthProvider {
        constructor(gssOptions?: { authorizationId?: string, service?: string, hostNameResolver?: Function });

        newAuthenticator(endpoint: string, name: string): Authenticator;
    }
}

export declare class Client extends events.EventEmitter {
    hosts: HostMap;
    keyspace: string;
    metadata: metadata.Metadata;
    metrics: metrics.ClientMetrics;

    constructor(options: DseClientOptions);

    connect(): Promise<void>;

    connect(callback: EmptyCallback): void;

    execute(query: string, params?: ArrayOrObject, options?: QueryOptions): Promise<types.ResultSet>;

    execute(query: string, params: ArrayOrObject, options: QueryOptions, callback: ValueCallback<types.ResultSet>): void;

    execute(query: string, params: ArrayOrObject, callback: ValueCallback<types.ResultSet>): void;

    execute(query: string, callback: ValueCallback<types.ResultSet>): void;

    executeGraph(
    traversal: string,
    parameters: { [name: string]: any } | undefined,
    options: GraphQueryOptions,
    callback: ValueCallback<graph.GraphResultSet>): void;

    executeGraph(
    traversal: string,
    parameters: { [name: string]: any } | undefined,
    callback: ValueCallback<graph.GraphResultSet>): void;

    executeGraph(traversal: string, callback: ValueCallback<graph.GraphResultSet>): void;

    executeGraph(
    traversal: string,
    parameters?: { [name: string]: any },
    options?: GraphQueryOptions): Promise<graph.GraphResultSet>;

    eachRow(query: string,
    params: ArrayOrObject,
    options: QueryOptions,
    rowCallback: (n: number, row: types.Row) => void,
    callback?: ValueCallback<types.ResultSet>): void;

    eachRow(query: string,
    params: ArrayOrObject,
    rowCallback: (n: number, row: types.Row) => void,
    callback?: ValueCallback<types.ResultSet>): void;

    eachRow(query: string,
    rowCallback: (n: number, row: types.Row) => void): void;

    stream(query: string, params?: ArrayOrObject, options?: QueryOptions, callback?: EmptyCallback): events.EventEmitter;

    batch(
    queries: Array<string|{query: string, params?: ArrayOrObject}>,
    options?: QueryOptions): Promise<types.ResultSet>;

    batch(
    queries: Array<string|{query: string, params?: ArrayOrObject}>,
    options: QueryOptions,
    callback: ValueCallback<types.ResultSet>): void;

    batch(
    queries: Array<string|{query: string, params?: ArrayOrObject}>,
    callback: ValueCallback<types.ResultSet>): void;

    shutdown(): Promise<void>;

    shutdown(callback: EmptyCallback): void;

    getReplicas(keyspace: string, token: Buffer): Host[];

    getState(): metadata.ClientState;
}

export declare interface ClientOptions {
    contactPoints?: string[];
    localDataCenter?: string;
    keyspace?: string;
    authProvider?: auth.AuthProvider;
    credentials?: {
        username: string;
        password: string;
    }

    cloud?: {
        secureConnectBundle: string | URL_2;
    };

    encoding?: {
        map?: Function;
        set?: Function;
        copyBuffer?: boolean;
        useUndefinedAsUnset?: boolean;
        useBigIntAsLong?: boolean;
        useBigIntAsVarint?: boolean;
    };
    isMetadataSyncEnabled?: boolean;
    maxPrepared?: number;
    metrics?: metrics.ClientMetrics;
    policies?: {
        addressResolution?: policies.addressResolution.AddressTranslator;
        loadBalancing?: policies.loadBalancing.LoadBalancingPolicy;
        reconnection?: policies.reconnection.ReconnectionPolicy;
        retry?: policies.retry.RetryPolicy;
        speculativeExecution?: policies.speculativeExecution.SpeculativeExecutionPolicy;
        timestampGeneration?: policies.timestampGeneration.TimestampGenerator;
    };
    pooling?: {
        coreConnectionsPerHost?: { [key: number]: number; };
        heartBeatInterval?: number;
        maxRequestsPerConnection?: number;
        warmup?: boolean;
    };
    prepareOnAllHosts?: boolean;
    profiles?: ExecutionProfile[];
    protocolOptions?: {
        maxSchemaAgreementWaitSeconds?: number;
        maxVersion?: number;
        noCompact?: boolean;
        port?: number;
    };
    promiseFactory?: (handler: (callback: (err: Error, result?: any) => void) => void) => Promise<any>;
    queryOptions?: QueryOptions;
    refreshSchemaDelay?: number;
    rePrepareOnUp?: boolean;
    requestTracker?: tracker.RequestTracker;
    socketOptions?: {
        coalescingThreshold?: number;
        connectTimeout?: number;
        defunctReadTimeoutThreshold?: number;
        keepAlive?: boolean;
        keepAliveDelay?: number;
        readTimeout?: number;
        tcpNoDelay?: boolean;
    };
    sslOptions?: tls.ConnectionOptions;
}

export declare namespace concurrent {
    export interface ResultSetGroup {
        errors: Error[];
        resultItems: any[];
        totalExecuted: number;
    }

    export type Options = {
        collectResults?: boolean;
        concurrencyLevel?: number;
        executionProfile?: string;
        maxErrors?: number;
        raiseOnFirstError?: boolean;
    }

    export function executeConcurrent(
    client: Client,
    query: string,
    parameters: any[][]|Readable,
    options?: Options): Promise<ResultSetGroup>;

    export function executeConcurrent(
    client: Client,
    queries: Array<{query: string, params: any[]}>,
    options?: Options): Promise<ResultSetGroup>;
}

export declare namespace datastax {
    import graph = graphModule.graph;

    import search = searchModule.search;
}

export declare function defaultOptions(): ClientOptions;

export declare interface DseClientOptions extends ClientOptions {
    id?: Uuid;
    applicationName?: string;
    applicationVersion?: string;
    monitorReporting?: { enabled?: boolean };
    graphOptions?: GraphOptions;
}

export declare type EmptyCallback = (err: Error) => void;

export declare namespace errors {
    export class ArgumentError extends DriverError {
        constructor(message: string);
    }

    export class AuthenticationError extends DriverError {
        constructor(message: string);
    }

    export class BusyConnectionError extends DriverError {
        constructor(address: string, maxRequestsPerConnection: number, connectionLength: number);
    }

    export abstract class DriverError extends Error {
        info: string;

        constructor(message: string, constructor?: any);
    }

    export class DriverInternalError extends DriverError {
        constructor(message: string);
    }

    export class NoHostAvailableError extends DriverError {
        innerErrors: any;

        constructor(innerErrors: any, message?: string);
    }

    export class NotSupportedError extends DriverError {
        constructor(message: string);
    }

    export class OperationTimedOutError extends DriverError {
        host?: string;

        constructor(message: string, host?: string);
    }

    export class ResponseError extends DriverError {
        code: number;

        constructor(code: number, message: string);
    }
}

export declare interface ExecutionOptions {
    getCaptureStackTrace(): boolean;

    getConsistency(): types.consistencies;

    getCustomPayload(): { [key: string]: any };

    getFetchSize(): number;

    getFixedHost(): Host;

    getHints(): string[] | string[][];

    isAutoPage(): boolean;

    isBatchCounter(): boolean;

    isBatchLogged(): boolean;

    isIdempotent(): boolean;

    isPrepared(): boolean;

    isQueryTracing(): boolean;

    getKeyspace(): string;

    getLoadBalancingPolicy(): policies.loadBalancing.LoadBalancingPolicy;

    getPageState(): Buffer;

    getRawQueryOptions(): QueryOptions;

    getReadTimeout(): number;

    getRetryPolicy(): policies.retry.RetryPolicy;

    getRoutingKey(): Buffer | Buffer[];

    getSerialConsistency(): types.consistencies;

    getTimestamp(): number | Long | undefined | null;

    setHints(hints: string[]): void;
}

export declare class ExecutionProfile {
    consistency?: types.consistencies;
    loadBalancing?: policies.loadBalancing.LoadBalancingPolicy;
    name: string;
    readTimeout?: number;
    retry?: policies.retry.RetryPolicy;
    serialConsistency?: types.consistencies;
    graphOptions?: {
        name?: string;
        language?: string;
        source?: string;
        readConsistency?: types.consistencies;
        writeConsistency?: types.consistencies;
    };

    constructor(name: string, options: {
        consistency?: types.consistencies;
        loadBalancing?: policies.loadBalancing.LoadBalancingPolicy;
        readTimeout?: number;
        retry?: policies.retry.RetryPolicy;
        serialConsistency?: types.consistencies;
        graphOptions?: {
            name?: string;
            language?: string;
            source?: string;
            readConsistency?: types.consistencies;
            writeConsistency?: types.consistencies;
        };
    });
}

export declare namespace geometry {
    export class LineString {
        constructor(...args: Point[]);

        static fromBuffer(buffer: Buffer): LineString;

        static fromString(textValue: string): LineString;

        equals(other: LineString): boolean;

        toBuffer(): Buffer;

        toJSON(): string;

        toString(): string;

    }

    export class Point {
        constructor(x: number, y: number);

        static fromBuffer(buffer: Buffer): Point;

        static fromString(textValue: string): Point;

        equals(other: Point): boolean;

        toBuffer(): Buffer;

        toJSON(): string;

        toString(): string;

    }

    export class Polygon {
        constructor(...args: Point[]);

        static fromBuffer(buffer: Buffer): Polygon;

        static fromString(textValue: string): Polygon;

        equals(other: Polygon): boolean;

        toBuffer(): Buffer;

        toJSON(): string;

        toString(): string;
    }
}

declare namespace graph {
    interface Edge extends Element {
        outV?: Vertex;
        outVLabel?: string;
        inV?: Vertex;
        inVLabel?: string;
        properties?: object;
    }

    interface Element {
        id: any;
        label: string;
    }

    class GraphResultSet implements Iterator<any> {
        constructor(rs: types.ResultSet);

        first(): any;

        toArray(): any[];

        values(): Iterator<any>;

        next(value?: any): IteratorResult<any>;
    }

    interface Path {
        labels: any[];
        objects: any[];
    }

    interface Property {
        value: any
        key: any
    }

    interface Vertex extends Element {
        properties?: { [key: string]: any[] }
    }

    interface VertexProperty extends Element {
        value: any
        key: string
        properties?: any
    }

    function asDouble(value: number): object;

    function asFloat(value: number): object;

    function asInt(value: number): object;

    function asTimestamp(value: Date): object;

    function asUdt(value: object): object;

    interface EnumValue {
        toString(): string
    }

    namespace t {
        const id: EnumValue;
        const key: EnumValue;
        const label: EnumValue;
        const value: EnumValue;
    }

    namespace direction {
        // `in` is a reserved word
        const in_: EnumValue;
        const out: EnumValue;
        const both: EnumValue;
    }
}

export declare type GraphOptions = {
    language?: string;
    name?: string;
    readConsistency?: types.consistencies;
    readTimeout?: number;
    source?: string;
    writeConsistency?: types.consistencies;
};

export declare interface GraphQueryOptions extends QueryOptions {
    graphLanguage?: string;
    graphName?: string;
    graphReadConsistency?: types.consistencies;
    graphSource?: string;
    graphWriteConsistency?: types.consistencies;
}

export declare interface Host extends events.EventEmitter {
    address: string;
    cassandraVersion: string;
    datacenter: string;
    rack: string;
    tokens: string[];
    hostId: types.Uuid;

    canBeConsideredAsUp(): boolean;

    getCassandraVersion(): number[];

    isUp(): boolean;
}

export declare interface HostMap extends events.EventEmitter {
    length: number;

    forEach(callback: (value: Host, key: string) => void): void;

    get(key: string): Host;

    keys(): string[];

    values(): Host[];
}

export declare namespace mapping {
    export interface TableMappings {
        getColumnName(propName: string): string;

        getPropertyName(columnName: string): string;

        newObjectInstance(): any;
    }

    export class DefaultTableMappings implements TableMappings {
        getColumnName(propName: string): string;

        getPropertyName(columnName: string): string;

        newObjectInstance(): any;
    }

    export class UnderscoreCqlToCamelCaseMappings implements TableMappings {
        getColumnName(propName: string): string;

        getPropertyName(columnName: string): string;

        newObjectInstance(): any;
    }

    export interface Result<T = any> extends Iterator<T> {
        wasApplied(): boolean;

        first(): T | null;

        forEach(callback: (currentValue: T, index: number) => void, thisArg?: any): void;

        toArray(): T[];
    }

    export type MappingExecutionOptions = {
        executionProfile?: string;
        isIdempotent?: boolean;
        logged?: boolean;
        timestamp?: number | Long;
        fetchSize?: number;
        pageState?: number;
    }

    export interface ModelTables {
        name: string;
        isView: boolean;
    }

    export class Mapper {
        constructor(client: Client, options?: MappingOptions);

        batch(items: ModelBatchItem[], executionOptions?: string | MappingExecutionOptions): Promise<Result>;

        forModel<T = any>(name: string): ModelMapper<T>;
    }

    export type MappingOptions = {
        models: { [key: string]: ModelOptions };
    }

    export type FindDocInfo = {
        fields?: string[];
        orderBy?: { [key: string]: string };
        limit?: number;
    }

    export type InsertDocInfo = {
        fields?: string[];
        ttl?: number;
        ifNotExists?: boolean;
    }

    export type UpdateDocInfo = {
        fields?: string[];
        ttl?: number;
        ifExists?: boolean;
        when?: { [key: string]: any };
        orderBy?: { [key: string]: string };
        limit?: number;
        deleteOnlyColumns?: boolean;
    }

    export type RemoveDocInfo = {
        fields?: string[];
        ttl?: number;
        ifExists?: boolean;
        when?: { [key: string]: any };
        deleteOnlyColumns?: boolean;
    }

    export type ModelOptions = {
        tables?: string[] | ModelTables[];
        mappings?: TableMappings;
        columns?: { [key: string]: string|ModelColumnOptions };
        keyspace?: string;
    }

    export type ModelColumnOptions = {
        name: string;
        toModel?: (columnValue: any) => any;
        fromModel?: (modelValue: any) => any;
    };

    export interface ModelBatchItem {

    }

    export interface ModelBatchMapper {
        insert(doc: any, docInfo?: InsertDocInfo): ModelBatchItem;

        remove(doc: any, docInfo?: RemoveDocInfo): ModelBatchItem;

        update(doc: any, docInfo?: UpdateDocInfo): ModelBatchItem;
    }

    export interface ModelMapper<T = any> {
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

    export namespace q {
        export interface QueryOperator {

        }

        export function in_(arr: any): QueryOperator;

        export function gt(value: any): QueryOperator;

        export function gte(value: any): QueryOperator;

        export function lt(value: any): QueryOperator;

        export function lte(value: any): QueryOperator;

        export function notEq(value: any): QueryOperator;

        export function and(condition1: any, condition2: any): QueryOperator;

        export function incr(value: any): QueryOperator;

        export function decr(value: any): QueryOperator;

        export function append(value: any): QueryOperator;

        export function prepend(value: any): QueryOperator;

        export function remove(value: any): QueryOperator;
    }
}

export declare namespace metadata {

    export interface Aggregate {
        argumentTypes: Array<{ code: dataTypes, info: any }>;
        finalFunction: string;
        initCondition: string;
        keyspaceName: string;
        returnType: string;
        signature: string[];
        stateFunction: string;
        stateType: string;
    }

    export interface ClientState {
        getConnectedHosts(): Host[];

        getInFlightQueries(host: Host): number;

        getOpenConnections(host: Host): number;

        toString(): string;
    }

    export interface DataTypeInfo {
        code: dataTypes;
        info: string | DataTypeInfo | DataTypeInfo[];
        options: {
            frozen: boolean;
            reversed: boolean;
        };
    }

    export interface ColumnInfo {
        name: string;
        type: DataTypeInfo;
    }

    export enum IndexKind {
        custom = 0,
        keys,
        composites
    }

    export interface Index {
        kind: IndexKind;
        name: string;
        options: object;
        target: string;

        isCompositesKind(): boolean;

        isCustomKind(): boolean;

        isKeysKind(): boolean;
    }

    export interface DataCollection {
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

    export interface MaterializedView extends DataCollection {
        tableName: string;
        whereClause: string;
        includeAllColumns: boolean;
    }

    export interface TableMetadata extends DataCollection {
        indexes: Index[];
        indexInterval?: number;
        isCompact: boolean;
        memtableFlushPeriod: number;
        replicateOnWrite: boolean;
        cdc?: boolean;
        virtual: boolean;
    }

    export interface QueryTrace {
        requestType: string;
        coordinator: InetAddress;
        parameters: { [key: string]: any };
        startedAt: number | types.Long;
        duration: number;
        clientAddress: string;
        events: Array<{ id: Uuid; activity: any; source: any; elapsed: any; thread: any }>;
    }

    export interface SchemaFunction {
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

    export interface Udt {
        name: string;
        fields: ColumnInfo[]
    }

    export interface Metadata {
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

export declare namespace metrics {
    export interface ClientMetrics {
        onAuthenticationError(e: Error | errors.AuthenticationError): void;

        onClientTimeoutError(e: errors.OperationTimedOutError): void;

        onClientTimeoutRetry(e: Error): void;

        onConnectionError(e: Error): void;

        onIgnoreError(e: Error): void;

        onOtherError(e: Error): void;

        onOtherErrorRetry(e: Error): void;

        onReadTimeoutError(e: errors.ResponseError): void;

        onReadTimeoutRetry(e: Error): void;

        onResponse(latency: number[]): void;

        onSpeculativeExecution(): void;

        onSuccessfulResponse(latency: number[]): void;

        onUnavailableError(e: errors.ResponseError): void;

        onUnavailableRetry(e: Error): void;

        onWriteTimeoutError(e: errors.ResponseError): void;

        onWriteTimeoutRetry(e: Error): void;
    }

    export class DefaultMetrics implements ClientMetrics {
        constructor();

        onAuthenticationError(e: Error | errors.AuthenticationError): void;

        onClientTimeoutError(e: errors.OperationTimedOutError): void;

        onClientTimeoutRetry(e: Error): void;

        onConnectionError(e: Error): void;

        onIgnoreError(e: Error): void;

        onOtherError(e: Error): void;

        onOtherErrorRetry(e: Error): void;

        onReadTimeoutError(e: errors.ResponseError): void;

        onReadTimeoutRetry(e: Error): void;

        onResponse(latency: number[]): void;

        onSpeculativeExecution(): void;

        onSuccessfulResponse(latency: number[]): void;

        onUnavailableError(e: errors.ResponseError): void;

        onUnavailableRetry(e: Error): void;

        onWriteTimeoutError(e: errors.ResponseError): void;

        onWriteTimeoutRetry(e: Error): void;
    }
}

export declare namespace policies {
    export function defaultAddressTranslator(): addressResolution.AddressTranslator;

    export function defaultLoadBalancingPolicy(localDc?: string): loadBalancing.LoadBalancingPolicy;

    export function defaultReconnectionPolicy(): reconnection.ReconnectionPolicy;

    export function defaultRetryPolicy(): retry.RetryPolicy;

    export function defaultSpeculativeExecutionPolicy(): speculativeExecution.SpeculativeExecutionPolicy;

    export function defaultTimestampGenerator(): timestampGeneration.TimestampGenerator;

    export namespace addressResolution {
        export interface AddressTranslator {
            translate(address: string, port: number, callback: Function): void;
        }

        export class EC2MultiRegionTranslator implements AddressTranslator {
            translate(address: string, port: number, callback: Function): void;
        }
    }

    export namespace loadBalancing {
        export abstract class LoadBalancingPolicy {
            init(client: Client, hosts: HostMap, callback: EmptyCallback): void;

            getDistance(host: Host): types.distance;

            newQueryPlan(
            keyspace: string,
            executionOptions: ExecutionOptions,
            callback: (error: Error, iterator: Iterator<Host>) => void): void;

            getOptions(): Map<string, object>;
        }

        export class DCAwareRoundRobinPolicy extends LoadBalancingPolicy {
            constructor(localDc: string);
        }

        export class TokenAwarePolicy extends LoadBalancingPolicy {
            constructor(childPolicy: LoadBalancingPolicy);
        }

        export class AllowListPolicy extends LoadBalancingPolicy {
            constructor(childPolicy: LoadBalancingPolicy, allowList: string[]);
        }

        export class WhiteListPolicy extends AllowListPolicy {
        }

        export class RoundRobinPolicy extends LoadBalancingPolicy {
            constructor();
        }

        export class DefaultLoadBalancingPolicy extends LoadBalancingPolicy {
            constructor(options?: { localDc?: string, filter?: (host: Host) => boolean });
        }
    }

    export namespace reconnection {
        export class ConstantReconnectionPolicy implements ReconnectionPolicy {
            constructor(delay: number);

            getOptions(): Map<string, object>;

            newSchedule(): Iterator<number>;

        }

        export class ExponentialReconnectionPolicy implements ReconnectionPolicy {
            constructor(baseDelay: number, maxDelay: number, startWithNoDelay?: boolean);

            getOptions(): Map<string, object>;

            newSchedule(): Iterator<number>;
        }

        export interface ReconnectionPolicy {
            getOptions(): Map<string, object>;

            newSchedule(): Iterator<number>;
        }
    }

    export namespace retry {
        export class DecisionInfo {
            decision: number;
            consistency: types.consistencies;
        }

        export class OperationInfo {
            query: string;
            executionOptions: ExecutionOptions;
            nbRetry: number;
        }

        export class IdempotenceAwareRetryPolicy extends RetryPolicy {
            constructor(childPolicy: RetryPolicy);
        }

        export class FallthroughRetryPolicy extends RetryPolicy {
            constructor();
        }

        export class RetryPolicy {
            onReadTimeout(
            info: OperationInfo,
            consistency: types.consistencies,
            received: number,
            blockFor: number,
            isDataPresent: boolean): DecisionInfo;

            onRequestError(info: OperationInfo, consistency: types.consistencies, err: Error): DecisionInfo;

            onUnavailable(
            info: OperationInfo, consistency: types.consistencies, required: number, alive: boolean): DecisionInfo;

            onWriteTimeout(
            info: OperationInfo,
            consistency: types.consistencies,
            received: number,
            blockFor: number,
            writeType: string): DecisionInfo;

            rethrowResult(): DecisionInfo;

            retryResult(consistency: types.consistencies, useCurrentHost?: boolean): DecisionInfo;
        }

        export namespace RetryDecision {
            export enum retryDecision {
                ignore,
                rethrow,
                retry
            }
        }
    }

    export namespace speculativeExecution {
        export class ConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
            constructor(delay: number, maxSpeculativeExecutions: number);

            getOptions(): Map<string, object>;

            init(client: Client): void;

            newPlan(keyspace: string, queryInfo: string | Array<object>): { nextExecution: Function };

            shutdown(): void;
        }

        export class NoSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
            constructor();

            getOptions(): Map<string, object>;

            init(client: Client): void;

            newPlan(keyspace: string, queryInfo: string | Array<object>): { nextExecution: Function };

            shutdown(): void;
        }

        export interface SpeculativeExecutionPolicy {
            getOptions(): Map<string, object>;

            init(client: Client): void;

            newPlan(keyspace: string, queryInfo: string|Array<object>): { nextExecution: Function };

            shutdown(): void;
        }
    }

    export namespace timestampGeneration {
        export class MonotonicTimestampGenerator implements TimestampGenerator {
            constructor(warningThreshold: number, minLogInterval: number);

            getDate(): number;

            next(client: Client): types.Long | number;
        }

        export interface TimestampGenerator {
            next(client: Client): types.Long|number;
        }
    }
}

export declare interface QueryOptions {
    autoPage?: boolean;
    captureStackTrace?: boolean;
    consistency?: number;
    counter?: boolean;
    customPayload?: any;
    executionProfile?: string | ExecutionProfile;
    fetchSize?: number;
    hints?: string[] | string[][];
    host?: Host;
    isIdempotent?: boolean;
    keyspace?: string;
    logged?: boolean;
    pageState?: Buffer | string;
    prepare?: boolean;
    readTimeout?: number;
    retry?: policies.retry.RetryPolicy;
    routingIndexes?: number[];
    routingKey?: Buffer | Buffer[];
    routingNames?: string[];
    serialConsistency?: number;
    timestamp?: number | Long;
    traceQuery?: boolean;
}

export declare namespace token {
    export interface Token {
        compare(other: Token): number;

        equals(other: Token): boolean;

        getType(): { code: types.dataTypes, info: any };

        getValue(): any;
    }

    export interface TokenRange {
        start: Token;
        end: Token;

        compare(other: TokenRange): number;

        contains(token: Token): boolean;

        equals(other: TokenRange): boolean;

        isEmpty(): boolean;

        isWrappedAround(): boolean;

        splitEvenly(numberOfSplits: number): TokenRange[];

        unwrap(): TokenRange[];
    }
}

export declare namespace tracker {
    export interface RequestTracker {
        onError(
        host: Host,
        query: string | Array<{ query: string, params?: any }>,
        parameters: any[] | { [key: string]: any } | null,
        executionOptions: ExecutionOptions,
        requestLength: number,
        err: Error,
        latency: number[]): void;

        onSuccess(
        host: Host,
        query: string | Array<{ query: string, params?: any }>,
        parameters: any[] | { [key: string]: any } | null,
        executionOptions: ExecutionOptions,
        requestLength: number,
        responseLength: number,
        latency: number[]): void;

        shutdown(): void;
    }

    export class RequestLogger implements RequestTracker {
        constructor(options: {
            slowThreshold?: number;
            logNormalRequests?: boolean;
            logErroredRequests?: boolean;
            messageMaxQueryLength?: number;
            messageMaxParameterValueLength?: number;
            messageMaxErrorStackTraceLength?: number;
        });

        onError(host: Host, query: string | Array<{ query: string; params?: any }>, parameters: any[] | { [p: string]: any } | null, executionOptions: ExecutionOptions, requestLength: number, err: Error, latency: number[]): void;

        onSuccess(host: Host, query: string | Array<{ query: string; params?: any }>, parameters: any[] | { [p: string]: any } | null, executionOptions: ExecutionOptions, requestLength: number, responseLength: number, latency: number[]): void;

        shutdown(): void;
    }
}

export declare namespace types {
    export class Long extends _Long {

    }

    export enum consistencies {
        any = 0x00,
        one = 0x01,
        two = 0x02,
        three = 0x03,
        quorum = 0x04,
        all = 0x05,
        localQuorum = 0x06,
        eachQuorum = 0x07,
        serial = 0x08,
        localSerial = 0x09,
        localOne = 0x0a
    }

    export enum dataTypes {
        custom = 0x0000,
        ascii = 0x0001,
        bigint = 0x0002,
        blob = 0x0003,
        boolean = 0x0004,
        counter = 0x0005,
        decimal = 0x0006,
        double = 0x0007,
        float = 0x0008,
        int = 0x0009,
        text = 0x000a,
        timestamp = 0x000b,
        uuid = 0x000c,
        varchar = 0x000d,
        varint = 0x000e,
        timeuuid = 0x000f,
        inet = 0x0010,
        date = 0x0011,
        time = 0x0012,
        smallint = 0x0013,
        tinyint = 0x0014,
        duration = 0x0015,
        list = 0x0020,
        map = 0x0021,
        set = 0x0022,
        udt = 0x0030,
        tuple = 0x0031,
    }

    export enum distance {
        local = 0,
        remote,
        ignored
    }

    export enum responseErrorCodes {
        serverError = 0x0000,
        protocolError = 0x000A,
        badCredentials = 0x0100,
        unavailableException = 0x1000,
        overloaded = 0x1001,
        isBootstrapping = 0x1002,
        truncateError = 0x1003,
        writeTimeout = 0x1100,
        readTimeout = 0x1200,
        readFailure = 0x1300,
        functionFailure = 0x1400,
        writeFailure = 0x1500,
        syntaxError = 0x2000,
        unauthorized = 0x2100,
        invalid = 0x2200,
        configError = 0x2300,
        alreadyExists = 0x2400,
        unprepared = 0x2500,
        clientWriteFailure = 0x8000
    }

    export enum protocolVersion {
        v1 = 0x01,
        v2 = 0x02,
        v3 = 0x03,
        v4 = 0x04,
        v5 = 0x05,
        v6 = 0x06,
        dseV1 = 0x41,
        dseV2 = 0x42,
        maxSupported = dseV2,
        minSupported = v1
    }

    export namespace protocolVersion {
        export function isSupported(version: protocolVersion): boolean;
    }

    const unset: object;

    export class BigDecimal {
        constructor(unscaledValue: number, scale: number);

        static fromBuffer(buf: Buffer): BigDecimal;

        static fromString(value: string): BigDecimal;

        static toBuffer(value: BigDecimal): Buffer;

        static fromNumber(value: number): BigDecimal;

        add(other: BigDecimal): BigDecimal;

        compare(other: BigDecimal): number;

        equals(other: BigDecimal): boolean;

        greaterThan(other: BigDecimal): boolean;

        isNegative(): boolean;

        isZero(): boolean;

        notEquals(other: BigDecimal): boolean;

        subtract(other: BigDecimal): BigDecimal;

        toNumber(): number;

        toString(): string;

        toJSON(): string;
    }

    export class Duration {
        constructor(month: number, days: number, nanoseconds: number | Long);

        static fromBuffer(buffer: Buffer): Duration;

        static fromString(input: string): Duration;

        equals(other: Duration): boolean;

        toBuffer(): Buffer;

        toString(): string;
    }

    export class InetAddress {
        length: number;

        version: number;

        constructor(buffer: Buffer);

        static fromString(value: string): InetAddress;

        equals(other: InetAddress): boolean;

        getBuffer(): Buffer;

        toString(): string;

        toJSON(): string;
    }

    export class Integer {
        static ONE: Integer;
        static ZERO: Integer;

        constructor(bits: Array<number>, sign: number);

        static fromBits(bits: Array<number>): Integer;

        static fromBuffer(bits: Buffer): Integer;

        static fromInt(value: number): Integer;

        static fromNumber(value: number): Integer;

        static fromString(str: string, opt_radix?: number): Integer;

        static toBuffer(value: Integer): Buffer;

        abs(): Integer;

        add(other: Integer): Integer;

        compare(other: Integer): number;

        divide(other: Integer): Integer;

        equals(other: Integer): boolean;

        getBits(index: number): number;

        getBitsUnsigned(index: number): number;

        getSign(): number;

        greaterThan(other: Integer): boolean;

        greaterThanOrEqual(other: Integer): boolean;

        isNegative(): boolean;

        isOdd(): boolean;

        isZero(): boolean;

        lessThan(other: Integer): boolean;

        lessThanOrEqual(other: Integer): boolean;

        modulo(other: Integer): Integer;

        multiply(other: Integer): Integer;

        negate(): Integer;

        not(): Integer;

        notEquals(other: Integer): boolean;

        or(other: Integer): Integer;

        shiftLeft(numBits: number): Integer;

        shiftRight(numBits: number): Integer;

        shorten(numBits: number): Integer;

        subtract(other: Integer): Integer;

        toInt(): number;

        toJSON(): string;

        toNumber(): number;

        toString(opt_radix?: number): string;

        xor(other: Integer): Integer;
    }

    export class LocalDate {
        year: number;
        month: number;
        day: number;

        constructor(year: number, month: number, day: number);

        static fromDate(date: Date): LocalDate;

        static fromString(value: string): LocalDate;

        static fromBuffer(buffer: Buffer): LocalDate;

        static now(): LocalDate;

        static utcNow(): LocalDate;

        equals(other: LocalDate): boolean;

        inspect(): string;

        toBuffer(): Buffer;

        toJSON(): string;

        toString(): string;
    }

    export class LocalTime {
        hour: number;
        minute: number;
        nanosecond: number;
        second: number;

        constructor(totalNanoseconds: Long);

        static fromBuffer(value: Buffer): LocalTime;

        static fromDate(date: Date, nanoseconds: number): LocalTime;

        static fromMilliseconds(milliseconds: number, nanoseconds?: number): LocalTime;

        static fromString(value: string): LocalTime;

        static now(nanoseconds?: number): LocalTime;

        compare(other: LocalTime): boolean;

        equals(other: LocalTime): boolean;

        getTotalNanoseconds(): Long;

        inspect(): string;

        toBuffer(): Buffer;

        toJSON(): string;

        toString(): string;
    }

    export interface ResultSet extends Iterable<Row>, AsyncIterable<Row> {
        info: {
            queriedHost: string,
            triedHosts: { [key: string]: any; },
            speculativeExecutions: number,
            achievedConsistency: consistencies,
            traceId: Uuid,
            warnings: string[],
            customPayload: any
        };

        columns: Array<{ name: string, type: { code: dataTypes, info: any } }>;
        nextPage: (() => void) | null;
        pageState: string;
        rowLength: number;
        rows: Row[];

        first(): Row;

        wasApplied(): boolean;
    }

    export interface ResultStream extends stream.Readable {
        buffer: Buffer;
        paused: boolean;

        add(chunk: Buffer): void;
    }

    export interface Row {
        get(columnName: string | number): any;

        keys(): string[];

        forEach(callback: (row: Row) => void): void;

        values(): any[];

        [key: string]: any;
    }

    export class TimeUuid extends Uuid {
        static now(): TimeUuid;

        static now(nodeId: string | Buffer, clockId?: string | Buffer): TimeUuid;

        static now(nodeId: string | Buffer, clockId: string | Buffer, callback: ValueCallback<TimeUuid>): void;

        static now(callback: ValueCallback<TimeUuid>): void;

        static fromDate(date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer): TimeUuid;

        static fromDate(
        date: Date,
        ticks: number,
        nodeId: string | Buffer,
        clockId: string | Buffer,
        callback: ValueCallback<TimeUuid>): void;

        static fromString(value: string): TimeUuid;

        static max(date: Date, ticks: number): TimeUuid;

        static min(date: Date, ticks: number): TimeUuid;

        getDatePrecision(): { date: Date, ticks: number };

        getDate(): Date;
    }

    export class Tuple {
        elements: any[];
        length: number;

        constructor(...args: any[]);

        static fromArray(elements: any[]): Tuple;

        get(index: number): any;

        toString(): string;

        toJSON(): string;

        values(): any[];
    }

    export class Uuid {
        constructor(buffer: Buffer);

        static fromString(value: string): Uuid;

        static random(callback: ValueCallback<Uuid>): void;

        static random(): Uuid;

        equals(other: Uuid): boolean;

        getBuffer(): Buffer;

        toString(): string;

        toJSON(): string;
    }
}

export declare type ValueCallback<T> = (err: Error, val: T) => void;

export declare const version: number;

export { }
