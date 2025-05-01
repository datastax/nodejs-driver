import { ConnectionOptions } from 'tls';
import EventEmitter from 'events';
import { EventEmitter as EventEmitter_2 } from 'stream';
import { default as Long } from 'long';
import * as Long_2 from 'long';
import Long__default from 'long';
import { Readable } from 'stream';
import { Socket } from 'net';

export declare const addressResolution: {
    AddressTranslator: typeof AddressTranslator;
    EC2MultiRegionTranslator: typeof EC2MultiRegionTranslator;
};

/* Excluded from this release type: AddressResolver */

/**
 * @class
 * @classdesc
 * Translates IP addresses received from Cassandra nodes into locally queryable
 * addresses.
 * <p>
 * The driver auto-detects new Cassandra nodes added to the cluster through server
 * side pushed notifications and through checking the system tables. For each
 * node, the address received will correspond to the address set as
 * <code>rpc_address</code> in the node yaml file. In most case, this is the correct
 * address to use by the driver and that is what is used by default. However,
 * sometimes the addresses received through this mechanism will either not be
 * reachable directly by the driver or should not be the preferred address to use
 * to reach the node (for instance, the <code>rpc_address</code> set on Cassandra nodes
 * might be a private IP, but some clients  may have to use a public IP, or
 * pass by a router to reach that node). This interface allows to deal with
 * such cases, by allowing to translate an address as sent by a Cassandra node
 * to another address to be used by the driver for connection.
 * <p>
 * Please note that the contact points addresses provided while creating the
 * {@link Client} instance are not "translated", only IP address retrieve from or sent
 * by Cassandra nodes to the driver are.
 */
export declare class AddressTranslator {
    /**
     * Translates a Cassandra <code>rpc_address</code> to another address if necessary.
     * @param {String} address the address of a node as returned by Cassandra.
     * <p>
     * Note that if the <code>rpc_address</code> of a node has been configured to <code>0.0.0.0</code>
     * server side, then the provided address will be the node <code>listen_address</code>,
     * *not* <code>0.0.0.0</code>.
     * </p>
     * @param {Number} port The port number, as specified in the [protocolOptions]{@link ClientOptions} at Client instance creation (9042 by default).
     * @param {Function} callback Callback to invoke with endpoint as first parameter.
     * The endpoint is an string composed of the IP address and the port number in the format <code>ipAddress:port</code>.
     */
    translate(address: string, port: number, callback: Function): void;
}

/**
 * Creates a new Aggregate.
 * @classdesc Describes a CQL aggregate.
 * @alias module:metadata~Aggregate
 * @constructor
 */
export declare class Aggregate {
    /* Excluded from this release type: name */
    /**
     * Name of the keyspace where the aggregate is declared.
     */
    keyspaceName: string;
    /**
     * Signature of the aggregate.
     * @type {Array.<String>}
     */
    signature: Array<string>;
    /**
     * List of the CQL aggregate argument types.
     * @type {Array.<{code, info}>}
     */
    argumentTypes: Array<{
        code: number;
        info?: (object | Array<any> | string);
    }>;
    /**
     * State Function.
     * @type {String}
     */
    stateFunction: string;
    /**
     * State Type.
     * @type {{code, info}}
     */
    stateType: {
        code: number;
        info?: (object | Array<any> | string);
    };
    /**
     * Final Function.
     * @type {String}
     */
    finalFunction: string;
    /* Excluded from this release type: initConditionRaw */
    /**
     * Initial state value of this aggregate.
     * @type {String}
     */
    initCondition: string;
    /**
     * Type of the return value.
     * @type {{code: number, info: (Object|Array|null)}}
     */
    returnType: {
        code: number;
        info?: (object | Array<any> | string);
    };
    /**
     * Indicates whether or not this aggregate is deterministic.  This means that
     * given a particular input, the aggregate will always produce the same output.
     * @type {Boolean}
     */
    deterministic: boolean;
    /* Excluded from this release type: __constructor */
}

/**
 * @class
 * @classdesc
 * A load balancing policy wrapper that ensure that only hosts from a provided
 * allow list will ever be returned.
 * <p>
 * This policy wraps another load balancing policy and will delegate the choice
 * of hosts to the wrapped policy with the exception that only hosts contained
 * in the allow list provided when constructing this policy will ever be
 * returned. Any host not in the while list will be considered ignored
 * and thus will not be connected to.
 * <p>
 * This policy can be useful to ensure that the driver only connects to a
 * predefined set of hosts. Keep in mind however that this policy defeats
 * somewhat the host auto-detection of the driver. As such, this policy is only
 * useful in a few special cases or for testing, but is not optimal in general.
 * If all you want to do is limiting connections to hosts of the local
 * data-center then you should use DCAwareRoundRobinPolicy and *not* this policy
 * in particular.
 * @extends LoadBalancingPolicy
 */
export declare class AllowListPolicy extends LoadBalancingPolicy {
    private childPolicy;
    private allowList;
    /**
     * Create a new policy that wraps the provided child policy but only "allow" hosts
     * from the provided list.
     * @class
     * @classdesc
     * A load balancing policy wrapper that ensure that only hosts from a provided
     * allow list will ever be returned.
     * <p>
     * This policy wraps another load balancing policy and will delegate the choice
     * of hosts to the wrapped policy with the exception that only hosts contained
     * in the allow list provided when constructing this policy will ever be
     * returned. Any host not in the while list will be considered ignored
     * and thus will not be connected to.
     * <p>
     * This policy can be useful to ensure that the driver only connects to a
     * predefined set of hosts. Keep in mind however that this policy defeats
     * somewhat the host auto-detection of the driver. As such, this policy is only
     * useful in a few special cases or for testing, but is not optimal in general.
     * If all you want to do is limiting connections to hosts of the local
     * data-center then you should use DCAwareRoundRobinPolicy and *not* this policy
     * in particular.
     * @param {LoadBalancingPolicy} childPolicy the wrapped policy.
     * @param {Array.<string>}  allowList The hosts address in the format ipAddress:port.
     * Only hosts from this list may get connected
     * to (whether they will get connected to or not depends on the child policy).
     * @constructor
     */
    constructor(childPolicy: LoadBalancingPolicy, allowList: Array<string>);
    init(client: Client, hosts: HostMap, callback: EmptyCallback): void;
    /**
     * Uses the child policy to return the distance to the host if included in the allow list.
     * Any host not in the while list will be considered ignored.
     * @param host
     */
    getDistance(host: Host): distance;
    /**
     * Checks if the host is in the allow list.
     * @param {Host} host
     * @returns {boolean}
     * @private
     */
    private _contains;
    /* Excluded from this release type: newQueryPlan */
    private _filter;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * Represents an error that is raised when one of the arguments provided to a method is not valid
 */
export declare class ArgumentError extends DriverError {
    /**
     * Represents an error that is raised when one of the arguments provided to a method is not valid
     * @param {String} message
     * @constructor
     */
    constructor(message: string);
}

declare type ArrayOrObject = any[] | {
    [key: string]: any;
} | null;

/**
 * Wraps a number or null value to hint the client driver that the data type of the value is a double
 * @memberOf module:datastax/graph
 */
declare function asDouble(value: number): object;

/**
 * Wraps a number or null value to hint the client driver that the data type of the value is a double
 * @memberOf module:datastax/graph
 */
declare function asFloat(value: number): object;

/**
 * Wraps a number or null value to hint the client driver that the data type of the value is an int
 * @memberOf module:datastax/graph
 */
declare function asInt(value: number): object;

/**
 * Wraps a Date or null value to hint the client driver that the data type of the value is a timestamp
 * @memberOf module:datastax/graph
 */
declare function asTimestamp(value: Date): object;

/**
 * Wraps an Object or null value to hint the client driver that the data type of the value is a user-defined type.
 * @memberOf module:datastax/graph
 * @param {object} value The object representing the UDT.
 * @param {{name: string, keyspace: string, fields: Array}} udtInfo The UDT metadata as defined by the driver.
 */
declare function asUdt(value: object, udtInfo: {
    name: string;
    keyspace: string;
    fields: Array<any>;
}): object;

/**
 * DSE Authentication module.
 * <p>
 *   Contains the classes used for connecting to a DSE cluster secured with DseAuthenticator.
 * </p>
 * @module auth
 */
export declare const auth: {
    Authenticator: typeof Authenticator;
    AuthProvider: typeof AuthProvider;
    DseGssapiAuthProvider: typeof DseGssapiAuthProvider;
    DsePlainTextAuthProvider: typeof DsePlainTextAuthProvider;
    /* Excluded from this release type: NoAuthProvider */
    PlainTextAuthProvider: typeof PlainTextAuthProvider;
};

/**
 * Represents an error when trying to authenticate with auth-enabled host
 */
export declare class AuthenticationError extends DriverError {
    additionalInfo: ResponseError;
    /**
     * Represents an error when trying to authenticate with auth-enabled host
     * @param {String} message
     * @constructor
     */
    constructor(message: string);
}

/**
 * Handles SASL authentication with Cassandra servers.
 * Each time a new connection is created and the server requires authentication,
 * a new instance of this class will be created by the corresponding.
 * @alias module:auth~Authenticator
 */
export declare class Authenticator {
    /**
     * Obtain an initial response token for initializing the SASL handshake.
     * @param {Function} callback
     */
    initialResponse(callback: Function): void;
    /**
     * Evaluates a challenge received from the Server. Generally, this method should callback with
     * no error and no additional params when authentication is complete from the client perspective.
     * @param {Buffer} challenge
     * @param {Function} callback
     */
    evaluateChallenge(challenge: Buffer, callback: Function): void;
    /**
     * Called when authentication is successful with the last information
     * optionally sent by the server.
     * @param {Buffer} [token]
     */
    onAuthenticationSuccess(token?: Buffer): void;
}

/**
 * Provides [Authenticator]{@link module:auth~Authenticator} instances to be used when connecting to a host.
 * @abstract
 * @alias module:auth~AuthProvider
 */
export declare class AuthProvider {
    /**
     * Returns an [Authenticator]{@link module:auth~Authenticator} instance to be used when connecting to a host.
     * @param {String} endpoint The ip address and port number in the format ip:port
     * @param {String} name Authenticator name
     * @abstract
     * @returns {Authenticator}
     */
    newAuthenticator(endpoint: string, name: string): Authenticator;
}

/** @module types */
/**
 * A <code>BigDecimal</code> consists of an [arbitrary precision integer]{@link module:types~Integer}
 * <i>unscaled value</i> and a 32-bit integer <i>scale</i>.  If zero
 * or positive, the scale is the number of digits to the right of the
 * decimal point.  If negative, the unscaled value of the number is
 * multiplied by ten to the power of the negation of the scale.  The
 * value of the number represented by the <code>BigDecimal</code> is
 * therefore <tt>(unscaledValue &times; 10<sup>-scale</sup>)</tt>.
 * @class
 * @classdesc The <code>BigDecimal</code> class provides operations for
 * arithmetic, scale manipulation, rounding, comparison and
 * format conversion.  The {@link #toString} method provides a
 * canonical representation of a <code>BigDecimal</code>.
 */
export declare class BigDecimal {
    private _intVal;
    private _scale;
    /**
     * Constructs an immutable arbitrary-precision signed decimal number.
     * A <code>BigDecimal</code> consists of an [arbitrary precision integer]{@link module:types~Integer}
     * <i>unscaled value</i> and a 32-bit integer <i>scale</i>.  If zero
     * or positive, the scale is the number of digits to the right of the
     * decimal point.  If negative, the unscaled value of the number is
     * multiplied by ten to the power of the negation of the scale.  The
     * value of the number represented by the <code>BigDecimal</code> is
     * therefore <tt>(unscaledValue &times; 10<sup>-scale</sup>)</tt>.
     * @param {Integer|Number} unscaledValue The integer part of the decimal.
     * @param {Number} scale The scale of the decimal.
     * @constructor
     */
    constructor(unscaledValue: Integer | number, scale: number);
    /**
     * Returns the BigDecimal representation of a buffer composed of the scale (int32BE) and the unsigned value (varint BE)
     * @param {Buffer} buf
     * @returns {BigDecimal}
     */
    static fromBuffer(buf: Buffer): BigDecimal;
    /**
     * Returns a buffer representation composed of the scale as a BE int 32 and the unsigned value as a BE varint
     * @param {BigDecimal} value
     * @returns {Buffer}
     */
    static toBuffer(value: BigDecimal): Buffer;
    /**
     * Returns a BigDecimal representation of the string
     * @param {String} value
     * @returns {BigDecimal}
     */
    static fromString(value: string): BigDecimal;
    /**
     * Returns a BigDecimal representation of the Number
     * @param {Number} value
     * @returns {BigDecimal}
     */
    static fromNumber(value: number): BigDecimal;
    /**
     * Returns true if the value of the BigDecimal instance and other are the same
     * @param {BigDecimal} other
     * @returns {Boolean}
     */
    equals(other: BigDecimal): boolean;
    /* Excluded from this release type: inspect */
    /**
     * @param {BigDecimal} other
     * @returns {boolean}
     */
    notEquals(other: BigDecimal): boolean;
    /**
     * Compares this BigDecimal with the given one.
     * @param {BigDecimal} other Integer to compare against.
     * @return {number} 0 if they are the same, 1 if the this is greater, and -1
     *     if the given one is greater.
     */
    compare(other: BigDecimal): number;
    /**
     * Returns the difference of this and the given BigDecimal.
     * @param {BigDecimal} other The BigDecimal to subtract from this.
     * @return {!BigDecimal} The BigDecimal result.
     */
    subtract(other: BigDecimal): BigDecimal;
    /**
     * Returns the sum of this and the given <code>BigDecimal</code>.
     * @param {BigDecimal} other The BigDecimal to sum to this.
     * @return {!BigDecimal} The BigDecimal result.
     */
    add(other: BigDecimal): BigDecimal;
    /**
     * Returns true if the current instance is greater than the other
     * @param {BigDecimal} other
     * @returns {boolean}
     */
    greaterThan(other: BigDecimal): boolean;
    /** @return {boolean} Whether this value is negative. */
    isNegative(): boolean;
    /** @return {boolean} Whether this value is zero. */
    isZero(): boolean;
    /**
     * Returns the string representation of this <code>BigDecimal</code>
     * @returns {string}
     */
    toString(): string;
    /**
     * Returns a Number representation of this <code>BigDecimal</code>.
     * @returns {Number}
     */
    toNumber(): number;
    /**
     * Returns the string representation.
     * Method used by the native JSON.stringify() to serialize this instance.
     */
    toJSON(): string;
}

/**
 * Represents a client-side error indicating that all connections to a certain host have reached
 * the maximum amount of in-flight requests supported.
 */
export declare class BusyConnectionError extends DriverError {
    /**
     * Represents a client-side error indicating that all connections to a certain host have reached
     * the maximum amount of in-flight requests supported.
     * @param {String} address
     * @param {Number} maxRequestsPerConnection
     * @param {Number} connectionLength
     * @constructor
     */
    constructor(address: string, maxRequestsPerConnection: number, connectionLength: number);
}

/* Excluded from this release type: ByteOrderedToken */

/* Excluded from this release type: ByteOrderedTokenizer */

declare const cassandra: {
    Client: typeof Client;
    ExecutionProfile: typeof ExecutionProfile;
    ExecutionOptions: typeof ExecutionOptions;
    types: {
        /* Excluded from this release type: getDataTypeNameByCode */
        /* Excluded from this release type: FrameHeader */
        /* Excluded from this release type: generateTimestamp */
        opcodes: {
            error: number;
            startup: number;
            ready: number;
            authenticate: number;
            credentials: number;
            options: number;
            supported: number;
            query: number;
            result: number;
            prepare: number;
            execute: number;
            register: number;
            event: number;
            batch: number;
            authChallenge: number;
            authResponse: number;
            authSuccess: number;
            cancel: number;
            isInRange: (code: any) => boolean;
        };
        consistencies: typeof consistencies;
        consistencyToString: {};
        dataTypes: typeof dataTypes;
        distance: typeof distance;
        frameFlags: {
            compression: number;
            tracing: number;
            customPayload: number;
            warning: number;
        };
        protocolEvents: {
            topologyChange: string;
            statusChange: string;
            schemaChange: string;
        };
        protocolVersion: typeof protocolVersion;
        responseErrorCodes: typeof responseErrorCodes;
        resultKind: {
            voidResult: number;
            rows: number;
            setKeyspace: number;
            prepared: number;
            schemaChange: number;
        };
        timeuuid: typeof timeuuid;
        uuid: typeof uuid;
        BigDecimal: typeof BigDecimal;
        Duration: typeof Duration;
        InetAddress: typeof InetAddress;
        Integer: typeof Integer;
        LocalDate: typeof LocalDate;
        LocalTime: typeof LocalTime;
        Long: typeof Long_2.default;
        ResultSet: typeof ResultSet;
        ResultStream: typeof ResultStream;
        Row: typeof Row;
        DriverError: typeof DriverError;
        TimeoutError: typeof TimeoutError;
        TimeUuid: typeof TimeUuid;
        Tuple: typeof Tuple;
        Uuid: typeof Uuid;
        unset: Readonly<{
            readonly unset: true;
        }>;
        Vector: typeof Vector;
    };
    errors: {
        ArgumentError: typeof ArgumentError;
        AuthenticationError: typeof AuthenticationError;
        BusyConnectionError: typeof BusyConnectionError;
        DriverError: typeof DriverError;
        OperationTimedOutError: typeof OperationTimedOutError;
        DriverInternalError: typeof DriverInternalError;
        NoHostAvailableError: typeof NoHostAvailableError;
        NotSupportedError: typeof NotSupportedError;
        ResponseError: typeof ResponseError;
        VIntOutOfRangeException: typeof VIntOutOfRangeException;
    };
    policies: {
        addressResolution: {
            AddressTranslator: typeof AddressTranslator;
            EC2MultiRegionTranslator: typeof EC2MultiRegionTranslator;
        };
        loadBalancing: {
            AllowListPolicy: typeof AllowListPolicy;
            DCAwareRoundRobinPolicy: typeof DCAwareRoundRobinPolicy;
            DefaultLoadBalancingPolicy: typeof DefaultLoadBalancingPolicy;
            LoadBalancingPolicy: typeof LoadBalancingPolicy;
            RoundRobinPolicy: typeof RoundRobinPolicy;
            TokenAwarePolicy: typeof TokenAwarePolicy;
            WhiteListPolicy: typeof WhiteListPolicy;
        };
        reconnection: {
            ReconnectionPolicy: typeof ReconnectionPolicy;
            ConstantReconnectionPolicy: typeof ConstantReconnectionPolicy;
            ExponentialReconnectionPolicy: typeof ExponentialReconnectionPolicy;
        };
        retry: {
            IdempotenceAwareRetryPolicy: typeof IdempotenceAwareRetryPolicy;
            FallthroughRetryPolicy: typeof FallthroughRetryPolicy;
            RetryPolicy: typeof RetryPolicy;
        };
        speculativeExecution: {
            NoSpeculativeExecutionPolicy: typeof NoSpeculativeExecutionPolicy;
            SpeculativeExecutionPolicy: typeof SpeculativeExecutionPolicy;
            ConstantSpeculativeExecutionPolicy: typeof ConstantSpeculativeExecutionPolicy;
        };
        timestampGeneration: {
            TimestampGenerator: typeof TimestampGenerator;
            MonotonicTimestampGenerator: typeof MonotonicTimestampGenerator;
        };
        defaultAddressTranslator: () => AddressTranslator;
        defaultLoadBalancingPolicy: (localDc?: string) => LoadBalancingPolicy;
        defaultRetryPolicy: () => RetryPolicy;
        defaultReconnectionPolicy: () => ReconnectionPolicy;
        defaultSpeculativeExecutionPolicy: () => SpeculativeExecutionPolicy;
        defaultTimestampGenerator: () => TimestampGenerator;
    };
    auth: {
        /* Excluded from this release type: NoAuthProvider */
        Authenticator: typeof Authenticator;
        AuthProvider: typeof AuthProvider;
        DseGssapiAuthProvider: typeof DseGssapiAuthProvider;
        DsePlainTextAuthProvider: typeof DsePlainTextAuthProvider;
        PlainTextAuthProvider: typeof PlainTextAuthProvider;
    };
    mapping: {
        Mapper: typeof Mapper;
        ModelMapper: typeof ModelMapper;
        ModelBatchMapper: typeof ModelBatchMapper;
        ModelBatchItem: typeof ModelBatchItem;
        Result: typeof Result;
        TableMappings: typeof TableMappings;
        DefaultTableMappings: typeof DefaultTableMappings;
        UnderscoreCqlToCamelCaseMappings: typeof UnderscoreCqlToCamelCaseMappings;
        q: {
            in_: (arr: any) => QueryOperator;
            gt: (value: any) => QueryOperator;
            gte: (value: any) => QueryOperator;
            lt: (value: any) => QueryOperator;
            lte: (value: any) => QueryOperator;
            notEq: (value: any) => QueryOperator;
            and: (condition1: any, condition2: any) => QueryOperator;
            incr: (value: any) => QueryAssignment;
            decr: (value: any) => QueryAssignment;
            append: (value: any) => QueryAssignment;
            prepend: (value: any) => QueryAssignment;
            remove: (value: any) => QueryAssignment;
        };
    };
    tracker: {
        RequestTracker: typeof RequestTracker;
        RequestLogger: typeof RequestLogger;
    };
    metrics: {
        ClientMetrics: typeof ClientMetrics;
        DefaultMetrics: typeof DefaultMetrics;
    };
    concurrent: {
        executeConcurrent: typeof executeConcurrent;
        ResultSetGroup: typeof ResultSetGroup;
    };
    token: {
        Token: typeof Token;
        TokenRange: typeof TokenRange;
    };
    metadata: {
        Metadata: typeof Metadata;
    };
    Encoder: typeof Encoder;
    geometry: {
        Point: typeof Point;
        LineString: typeof LineString;
        Polygon: typeof Polygon;
        Geometry: typeof Geometry;
    };
    datastax: {
        graph: {
            /* Excluded from this release type: getCustomTypeSerializers */
            /* Excluded from this release type: GraphTypeWrapper */
            /* Excluded from this release type: UdtGraphWrapper */
            Edge: typeof Edge;
            Element: typeof Element;
            Path: typeof Path;
            Property: typeof Property;
            Vertex: typeof Vertex;
            VertexProperty: typeof VertexProperty;
            asInt: typeof asInt;
            asDouble: typeof asDouble;
            asFloat: typeof asFloat;
            asTimestamp: typeof asTimestamp;
            asUdt: typeof asUdt;
            direction: {
                both: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                in: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                out: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                in_: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
            };
            GraphResultSet: typeof GraphResultSet;
            t: {
                id: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                key: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                label: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
                value: {
                    typeName: any;
                    elementName: any;
                    toString(): any;
                };
            };
        };
        search: {
            DateRange: typeof DateRange;
            DateRangeBound: typeof DateRangeBound;
            dateRangePrecision: {
                readonly year: 0;
                readonly month: 1;
                readonly day: 2;
                readonly hour: 3;
                readonly minute: 4;
                readonly second: 5;
                readonly millisecond: 6;
            };
        };
    };
    /**
     * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
     */
    defaultOptions: () => ClientOptions;
    version: string;
};
export default cassandra;

/**
 * Creates a new instance of {@link Client}.
 * @classdesc
 * Represents a database client that maintains multiple connections to the cluster nodes, providing methods to
 * execute CQL statements.
 * <p>
 * The <code>Client</code> uses [policies]{@link module:policies} to decide which nodes to connect to, which node
 * to use per each query execution, when it should retry failed or timed-out executions and how reconnection to down
 * nodes should be made.
 * </p>
 * @extends EventEmitter
 * @param {ClientOptions} options The options for this instance.
 * @example <caption>Creating a new client instance</caption>
 * const client = new Client({
 *   contactPoints: ['10.0.1.101', '10.0.1.102'],
 *   localDataCenter: 'datacenter1'
 * });
 * @example <caption>Executing a query</caption>
 * const result = await client.connect();
 * console.log(`Connected to ${client.hosts.length} nodes in the cluster: ${client.hosts.keys().join(', ')}`);
 * @example <caption>Executing a query</caption>
 * const result = await client.execute('SELECT key FROM system.local');
 * const row = result.first();
 * console.log(row['key']);
 */
export declare class Client extends EventEmitter.EventEmitter {
    /* Excluded from this release type: options */
    /* Excluded from this release type: profileManager */
    private connected;
    private isShuttingDown;
    /**
     * Gets the name of the active keyspace.
     * @type {String}
     */
    keyspace: string;
    /**
     * Gets the schema and cluster metadata information.
     * @type {Metadata}
     */
    metadata: Metadata;
    /* Excluded from this release type: controlConnection */
    /**
     * Gets an associative array of cluster hosts.
     * @type {HostMap}
     */
    hosts: HostMap;
    /**
     * The [ClientMetrics]{@link module:metrics~ClientMetrics} instance used to expose measurements of its internal
     * behavior and of the server as seen from the driver side.
     * <p>By default, a [DefaultMetrics]{@link module:metrics~DefaultMetrics} instance is used.</p>
     * @type {ClientMetrics}
     */
    metrics: ClientMetrics;
    private _graphExecutor;
    private connecting;
    private insightsClient;
    /**
     * Creates a new instance of {@link Client}.
     * Represents a database client that maintains multiple connections to the cluster nodes, providing methods to
     * execute CQL statements.
     * <p>
     * The <code>Client</code> uses [policies]{@link module:policies} to decide which nodes to connect to, which node
     * to use per each query execution, when it should retry failed or timed-out executions and how reconnection to down
     * nodes should be made.
     * </p>
     * @param {DseClientOptions} options The options for this instance.
     * @example <caption>Creating a new client instance</caption>
     * const client = new Client({
     *   contactPoints: ['10.0.1.101', '10.0.1.102'],
     *   localDataCenter: 'datacenter1'
     * });
     * @example <caption>Executing a query</caption>
     * const result = await client.connect();
     * console.log(`Connected to ${client.hosts.length} nodes in the cluster: ${client.hosts.keys().join(', ')}`);
     * @example <caption>Executing a query</caption>
     * const result = await client.execute('SELECT key FROM system.local');
     * const row = result.first();
     * console.log(row['key']);
     * @constructor
     */
    constructor(options: DseClientOptions);
    /**
     * Emitted when a new host is added to the cluster.
     * <ul>
     *   <li>{@link Host} The host being added.</li>
     * </ul>
     * @event Client#hostAdd
     */
    /**
     * Emitted when a host is removed from the cluster
     * <ul>
     *   <li>{@link Host} The host being removed.</li>
     * </ul>
     * @event Client#hostRemove
     */
    /**
     * Emitted when a host in the cluster changed status from down to up.
     * <ul>
     *   <li>{@link Host host} The host that changed the status.</li>
     * </ul>
     * @event Client#hostUp
     */
    /**
     * Emitted when a host in the cluster changed status from up to down.
     * <ul>
     *   <li>{@link Host host} The host that changed the status.</li>
     * </ul>
     * @event Client#hostDown
     */
    /**
     * Attempts to connect to one of the [contactPoints]{@link ClientOptions} and discovers the rest the nodes of the
     * cluster.
     * <p>When the {@link Client} is already connected, it resolves immediately.</p>
     * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
     * @param {function} [callback] The optional callback that is invoked when the pool is connected or it failed to
     * connect.
     * @example <caption>Usage example</caption>
     * await client.connect();
     */
    connect(): Promise<void>;
    connect(callback: EmptyCallback): void;
    /**
     * Async-only version of {@link Client#connect()}.
     * @private
     */
    private _connect;
    /* Excluded from this release type: log */
    /**
     * Executes a query on an available connection.
     * <p>The query can be prepared (recommended) or not depending on the [prepare]{@linkcode QueryOptions} flag.</p>
     * <p>
     *   Some execution failures can be handled transparently by the driver, according to the
     *   [RetryPolicy]{@linkcode module:policies/retry~RetryPolicy} or the
     *   [SpeculativeExecutionPolicy]{@linkcode module:policies/speculativeExecution} used.
     * </p>
     * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
     * @param {String} query The query to execute.
     * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
     * as keys and its value.
     * @param {QueryOptions} [options] The query options for the execution.
     * @param {ResultCallback} [callback] Executes callback(err, result) when execution completed. When not defined, the
     * method will return a promise.
     * @example <caption>Promise-based API, using async/await</caption>
     * const query = 'SELECT name, email FROM users WHERE id = ?';
     * const result = await client.execute(query, [ id ], { prepare: true });
     * const row = result.first();
     * console.log('%s: %s', row['name'], row['email']);
     * @example <caption>Callback-based API</caption>
     * const query = 'SELECT name, email FROM users WHERE id = ?';
     * client.execute(query, [ id ], { prepare: true }, function (err, result) {
     *   assert.ifError(err);
     *   const row = result.first();
     *   console.log('%s: %s', row['name'], row['email']);
     * });
     * @see {@link ExecutionProfile} to reuse a set of options across different query executions.
     */
    execute(query: string, params?: ArrayOrObject, options?: QueryOptions): Promise<ResultSet>;
    execute(query: string, params: ArrayOrObject, options: QueryOptions, callback: ValueCallback<ResultSet>): void;
    execute(query: string, params: ArrayOrObject, callback: ValueCallback<ResultSet>): void;
    execute(query: string, callback: ValueCallback<ResultSet>): void;
    /**
     * Executes a graph query.
     * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
     * @param {String} query The gremlin query.
     * @param {Object|null} [parameters] An associative array containing the key and values of the parameters.
     * @param {GraphQueryOptions|null} [options] The graph query options.
     * @param {Function} [callback] Function to execute when the response is retrieved, taking two arguments:
     * <code>err</code> and <code>result</code>. When not defined, the method will return a promise.
     * @example <caption>Promise-based API, using async/await</caption>
     * const result = await client.executeGraph('g.V()');
     * // Get the first item (vertex, edge, scalar value, ...)
     * const vertex = result.first();
     * console.log(vertex.label);
     * @example <caption>Callback-based API</caption>
     * client.executeGraph('g.V()', (err, result) => {
     *   const vertex = result.first();
     *   console.log(vertex.label);
     * });
     * @example <caption>Iterating through the results</caption>
     * const result = await client.executeGraph('g.E()');
     * for (let edge of result) {
     *   console.log(edge.label); // created
     * });
     * @example <caption>Using result.forEach()</caption>
     * const result = await client.executeGraph('g.V().hasLabel("person")');
     * result.forEach(function(vertex) {
     *   console.log(vertex.type); // vertex
     *   console.log(vertex.label); // person
     * });
     * @see {@link ExecutionProfile} to reuse a set of options across different query executions.
     */
    executeGraph(traversal: string, parameters: {
        [name: string]: any;
    } | undefined, options: GraphQueryOptions, callback: ValueCallback<GraphResultSet>): void;
    executeGraph(traversal: string, parameters: {
        [name: string]: any;
    } | undefined, callback: ValueCallback<GraphResultSet>): void;
    executeGraph(traversal: string, callback: ValueCallback<GraphResultSet>): void;
    executeGraph(traversal: string, parameters?: {
        [name: string]: any;
    }, options?: GraphQueryOptions): Promise<GraphResultSet>;
    /**
     * Executes the query and calls <code>rowCallback</code> for each row as soon as they are received. Calls the final
     * <code>callback</code> after all rows have been sent, or when there is an error.
     * <p>
     *   The query can be prepared (recommended) or not depending on the [prepare]{@linkcode QueryOptions} flag.
     * </p>
     * @param {String} query The query to execute
     * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
     * as keys and its value.
     * @param {QueryOptions} [options] The query options.
     * @param {function} rowCallback Executes <code>rowCallback(n, row)</code> per each row received, where n is the row
     * index and row is the current Row.
     * @param {function} [callback] Executes <code>callback(err, result)</code> after all rows have been received.
     * <p>
     *   When dealing with paged results, [ResultSet#nextPage()]{@link module:types~ResultSet#nextPage} method can be used
     *   to retrieve the following page. In that case, <code>rowCallback()</code> will be again called for each row and
     *   the final callback will be invoked when all rows in the following page has been retrieved.
     * </p>
     * @example <caption>Using per-row callback and arrow functions</caption>
     * client.eachRow(query, params, { prepare: true }, (n, row) => console.log(n, row), err => console.error(err));
     * @example <caption>Overloads</caption>
     * client.eachRow(query, rowCallback);
     * client.eachRow(query, params, rowCallback);
     * client.eachRow(query, params, options, rowCallback);
     * client.eachRow(query, params, rowCallback, callback);
     * client.eachRow(query, params, options, rowCallback, callback);
     */
    eachRow(query: string, params: ArrayOrObject, options: QueryOptions, rowCallback: (n: number, row: Row) => void, callback?: ValueCallback<ResultSet>): void;
    eachRow(query: string, params: ArrayOrObject, rowCallback: (n: number, row: Row) => void, callback?: ValueCallback<ResultSet>): void;
    eachRow(query: string, rowCallback: (n: number, row: Row) => void): void;
    /**
     * Executes the query and pushes the rows to the result stream as soon as they received.
     * <p>
     * The stream is a [ReadableStream]{@linkcode https://nodejs.org/api/stream.html#stream_class_stream_readable} object
     *  that emits rows.
     *  It can be piped downstream and provides automatic pause/resume logic (it buffers when not read).
     * </p>
     * <p>
     *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
     *   hosts if needed.
     * </p>
     * @param {String} query The query to prepare and execute.
     * @param {ArrayOrObject} [params] Array of parameter values or an associative array (object) containing parameter names
     * as keys and its value
     * @param {QueryOptions} [options] The query options.
     * @param {function} [callback] executes callback(err) after all rows have been received or if there is an error
     * @returns {ResultStream}
     */
    stream(query: string, params?: ArrayOrObject, options?: QueryOptions, callback?: EmptyCallback): ResultStream;
    /**
     * Executes batch of queries on an available connection to a host.
     * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
     * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array
     * of object containing the query and params
     * @param {QueryOptions} [options] The query options.
     * @param {ResultCallback} [callback] Executes callback(err, result) when the batch was executed
     */
    batch(queries: Array<string | {
        query: string;
        params?: ArrayOrObject;
    }>, options?: QueryOptions): Promise<ResultSet>;
    batch(queries: Array<string | {
        query: string;
        params?: ArrayOrObject;
    }>, options: QueryOptions, callback: ValueCallback<ResultSet>): void;
    batch(queries: Array<string | {
        query: string;
        params?: ArrayOrObject;
    }>, callback: ValueCallback<ResultSet>): void;
    /**
     * Async-only version of {@link Client#batch()} .
     * @param {Array.<string>|Array.<{query, params}>}queries
     * @param {QueryOptions} options
     * @returns {Promise<ResultSet>}
     * @private
     */
    private _batch;
    /**
     * Gets the host that are replicas of a given token.
     * @param {String} keyspace
     * @param {Buffer} token
     * @returns {Array<Host>}
     */
    getReplicas(keyspace: string, token: Buffer): Array<Host>;
    /**
     * Gets a snapshot containing information on the connections pools held by this Client at the current time.
     * <p>
     *   The information provided in the returned object only represents the state at the moment this method was called and
     *   it's not maintained in sync with the driver metadata.
     * </p>
     * @returns {ClientState} A [ClientState]{@linkcode module:metadata~ClientState} instance.
     */
    getState(): ClientState;
    /**
     * Closes all connections to all hosts.
     * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
     * @param {Function} [callback] Optional callback to be invoked when finished closing all connections.
     */
    shutdown(): Promise<void>;
    shutdown(callback: EmptyCallback): void;
    /** @private */
    private _shutdown;
    /* Excluded from this release type: _waitForSchemaAgreement */
    /* Excluded from this release type: handleSchemaAgreementAndRefresh */
    /**
     * Connects and handles the execution of prepared and simple statements.
     * @param {string} query
     * @param {Array} params
     * @param {ExecutionOptions} execOptions
     * @returns {Promise<ResultSet>}
     * @private
     */
    private _execute;
    /**
     * Sets the listeners for the nodes.
     * @private
     */
    private _setHostListeners;
    /**
     * Sets the distance to each host and when warmup is true, creates all connections to local hosts.
     * @returns {Promise}
     * @private
     */
    private _warmup;
    /**
     * @returns {Encoder}
     * @private
     */
    private _getEncoder;
    /**
     * Returns a BatchRequest instance and fills the routing key information in the provided options.
     * @private
     */
    private _createBatchRequest;
    /**
     * Returns an ExecuteRequest instance and fills the routing key information in the provided options.
     * @private
     */
    private _createExecuteRequest;
    /**
     * Returns a QueryRequest instance and fills the routing key information in the provided options.
     * @private
     */
    private _createQueryRequest;
    /**
     * Sets the routing key based on the parameter values or the provided routing key components.
     * @param {ExecutionOptions} execOptions
     * @param {Array} params
     * @param meta
     * @private
     */
    private _setRoutingInfo;
}

/**
 * Represents a base class that is used to measure events from the server and the client as seen by the driver.
 * @alias module:metrics~ClientMetrics
 * @interface
 */
export declare class ClientMetrics {
    /**
     * Method invoked when an authentication error is obtained from the server.
     * @param {AuthenticationError|Error} e The error encountered.
     */
    onAuthenticationError(e: AuthenticationError | Error): void;
    /**
     * Method invoked when an error (different than a server or client timeout, authentication or connection error) is
     * encountered when executing a request.
     * @param {OperationTimedOutError} e The timeout error.
     */
    onClientTimeoutError(e: OperationTimedOutError): void;
    /**
     * Method invoked when there is a connection error.
     * @param {Error} e The error encountered.
     */
    onConnectionError(e: Error): void;
    /**
     * Method invoked when an error (different than a server or client timeout, authentication or connection error) is
     * encountered when executing a request.
     * @param {Error} e The error encountered.
     */
    onOtherError(e: Error): void;
    /**
     * Method invoked when a read timeout error is obtained from the server.
     * @param {ResponseError} e The error encountered.
     */
    onReadTimeoutError(e: ResponseError): void;
    /**
     * Method invoked when a write timeout error is obtained from the server.
     * @param {ResponseError} e The error encountered.
     */
    onWriteTimeoutError(e: ResponseError): void;
    /**
     * Method invoked when an unavailable error is obtained from the server.
     * @param {ResponseError} e The error encountered.
     */
    onUnavailableError(e: ResponseError): void;
    /**
     * Method invoked when an execution is retried as a result of a client-level timeout.
     * @param {Error} e The error that caused the retry.
     */
    onClientTimeoutRetry(e: Error): void;
    /**
     * Method invoked when an error (other than a server or client timeout) is retried.
     * @param {Error} e The error that caused the retry.
     */
    onOtherErrorRetry(e: Error): void;
    /**
     * Method invoked when an execution is retried as a result of a read timeout from the server (coordinator to replica).
     * @param {Error} e The error that caused the retry.
     */
    onReadTimeoutRetry(e: Error): void;
    /**
     * Method invoked when an execution is retried as a result of an unavailable error from the server.
     * @param {Error} e The error that caused the retry.
     */
    onUnavailableRetry(e: Error): void;
    /**
     * Method invoked when an execution is retried as a result of a write timeout from the server (coordinator to
     * replica).
     * @param {Error} e The error that caused the retry.
     */
    onWriteTimeoutRetry(e: Error): void;
    /**
     * Method invoked when an error is marked as ignored by the retry policy.
     * @param {Error} e The error that was ignored by the retry policy.
     */
    onIgnoreError(e: Error): void;
    /**
     * Method invoked when a speculative execution is started.
     */
    onSpeculativeExecution(): void;
    /**
     * Method invoked when a response is obtained successfully.
     * @param {Array<Number>} latency The latency represented in a <code>[seconds, nanoseconds]</code> tuple
     * Array, where nanoseconds is the remaining part of the real time that can't be represented in second precision.
     */
    onSuccessfulResponse(latency: Array<number>): void;
    /**
     * Method invoked when any response is obtained, the response can be the result of a successful execution or a
     * server-side error.
     * @param {Array<Number>} latency The latency represented in a <code>[seconds, nanoseconds]</code> tuple
     * Array, where nanoseconds is the remaining part of the real time that can't be represented in second precision.
     */
    onResponse(latency: Array<number>): void;
}

/**
 * Client options.
 * <p>While the driver provides lots of extensibility points and configurability, few client options are required.</p>
 * <p>Default values for all settings are designed to be suitable for the majority of use cases, you should avoid
 * fine tuning it when not needed.</p>
 * <p>See [Client constructor]{@link Client} documentation for recommended options.</p>
 * @typedef {Object} ClientOptions@typedef {Object} ClientOptions
 * @property {Array.<string>} contactPoints
 * Array of addresses or host names of the nodes to add as contact points.
 * <p>
 *  Contact points are addresses of Cassandra nodes that the driver uses to discover the cluster topology.
 * </p>
 * <p>
 *  Only one contact point is required (the driver will retrieve the address of the other nodes automatically),
 *  but it is usually a good idea to provide more than one contact point, because if that single contact point is
 *  unavailable, the driver will not be able to initialize correctly.
 * </p>
 * @property {String} [localDataCenter] The local data center to use.
 * <p>
 *   If using DCAwareRoundRobinPolicy (default), this option is required and only hosts from this data center are
 *   connected to and used in query plans.
 * </p>
 * @property {String} [keyspace] The logged keyspace for all the connections created within the {@link Client} instance.
 * @property {Object} [credentials] An object containing the username and password for plain-text authentication.
 * It configures the authentication provider to be used against Apache Cassandra's PasswordAuthenticator or DSE's
 * DseAuthenticator, when default auth scheme is plain-text.
 * <p>
 *   Note that you should configure either <code>credentials</code> or <code>authProvider</code> to connect to an
 *   auth-enabled cluster, but not both.
 * </p>
 * @property {String} [credentials.username] The username to use for plain-text authentication.
 * @property {String} [credentials.password] The password to use for plain-text authentication.
 * @property {Uuid} [id] A unique identifier assigned to a {@link Client} object, that will be communicated to the
 * server (DSE 6.0+) to identify the client instance created with this options. When not defined, the driver will
 * generate a random identifier.
 * @property {String} [applicationName] An optional setting identifying the name of the application using
 * the {@link Client} instance.
 * <p>This value is passed to DSE and is useful as metadata for describing a client connection on the server side.</p>
 * @property {String} [applicationVersion] An optional setting identifying the version of the application using
 * the {@link Client} instance.
 * <p>This value is passed to DSE and is useful as metadata for describing a client connection on the server side.</p>
 * @property {Object} [monitorReporting] Options for reporting mechanism from the client to the DSE server, for
 * versions that support it.
 * @property {Boolean} [monitorReporting.enabled=true] Determines whether the reporting mechanism is enabled.
 * Defaults to <code>true</code>.
 * @property {Object} [cloud] The options to connect to a cloud instance.
 * @property {String|URL} cloud.secureConnectBundle Determines the file path for the credentials file bundle.
 * @property {Number} [refreshSchemaDelay] The default window size in milliseconds used to debounce node list and schema
 * refresh metadata requests. Default: 1000.
 * @property {Boolean} [isMetadataSyncEnabled] Determines whether client-side schema metadata retrieval and update is
 * enabled.
 * <p>Setting this value to <code>false</code> will cause keyspace information not to be automatically loaded, affecting
 * replica calculation per token in the different keyspaces. When disabling metadata synchronization, use
 * [Metadata.refreshKeyspaces()]{@link module:metadata~Metadata#refreshKeyspaces} to keep keyspace information up to
 * date or token-awareness will not work correctly.</p>
 * Default: <code>true</code>.
 * @property {Boolean} [prepareOnAllHosts] Determines if the driver should prepare queries on all hosts in the cluster.
 * Default: <code>true</code>.
 * @property {Boolean} [rePrepareOnUp] Determines if the driver should re-prepare all cached prepared queries on a
 * host when it marks it back up.
 * Default: <code>true</code>.
 * @property {Number} [maxPrepared] Determines the maximum amount of different prepared queries before evicting items
 * from the internal cache. Reaching a high threshold hints that the queries are not being reused, like when
 * hard-coding parameter values inside the queries.
 * Default: <code>500</code>.
 * @property {Object} [policies]
 * @property {LoadBalancingPolicy} [policies.loadBalancing] The load balancing policy instance to be used to determine
 * the coordinator per query.
 * @property {RetryPolicy} [policies.retry] The retry policy.
 * @property {ReconnectionPolicy} [policies.reconnection] The reconnection policy to be used.
 * @property {AddressTranslator} [policies.addressResolution] The address resolution policy.
 * @property {SpeculativeExecutionPolicy} [policies.speculativeExecution] The <code>SpeculativeExecutionPolicy</code>
 * instance to be used to determine if the client should send speculative queries when the selected host takes more
 * time than expected.
 * <p>
 *   Default: <code>[NoSpeculativeExecutionPolicy]{@link
 *   module:policies/speculativeExecution~NoSpeculativeExecutionPolicy}</code>
 * </p>
 * @property {TimestampGenerator} [policies.timestampGeneration] The client-side
 * [query timestamp generator]{@link module:policies/timestampGeneration~TimestampGenerator}.
 * <p>
 *   Default: <code>[MonotonicTimestampGenerator]{@link module:policies/timestampGeneration~MonotonicTimestampGenerator}
 *   </code>
 * </p>
 * <p>Use <code>null</code> to disable client-side timestamp generation.</p>
 * @property {QueryOptions} [queryOptions] Default options for all queries.
 * @property {Object} [pooling] Pooling options.
 * @property {Number} [pooling.heartBeatInterval] The amount of idle time in milliseconds that has to pass before the
 * driver issues a request on an active connection to avoid idle time disconnections. Default: 30000.
 * @property {Object} [pooling.coreConnectionsPerHost] Associative array containing amount of connections per host
 * distance.
 * @property {Number} [pooling.maxRequestsPerConnection] The maximum number of requests per connection. The default
 * value is:
 * <ul>
 *   <li>For modern protocol versions (v3 and above): 2048</li>
 *   <li>For older protocol versions (v1 and v2): 128</li>
 * </ul>
 * @property {Boolean} [pooling.warmup] Determines if all connections to hosts in the local datacenter must be opened on
 * connect. Default: true.
 * @property {Object} [protocolOptions]
 * @property {Number} [protocolOptions.port] The port to use to connect to the Cassandra host. If not set through this
 * method, the default port (9042) will be used instead.
 * @property {Number} [protocolOptions.maxSchemaAgreementWaitSeconds] The maximum time in seconds to wait for schema
 * agreement between nodes before returning from a DDL query. Default: 10.
 * @property {Number} [protocolOptions.maxVersion] When set, it limits the maximum protocol version used to connect to
 * the nodes.
 * Useful for using the driver against a cluster that contains nodes with different major/minor versions of Cassandra.
 * @property {Boolean} [protocolOptions.noCompact] When set to true, enables the NO_COMPACT startup option.
 * <p>
 * When this option is supplied <code>SELECT</code>, <code>UPDATE</code>, <code>DELETE</code>, and <code>BATCH</code>
 * statements on <code>COMPACT STORAGE</code> tables function in "compatibility" mode which allows seeing these tables
 * as if they were "regular" CQL tables.
 * </p>
 * <p>
 * This option only effects interactions with interactions with tables using <code>COMPACT STORAGE</code> and is only
 * supported by C* 3.0.16+, 3.11.2+, 4.0+ and DSE 6.0+.
 * </p>
 * @property {Object} [socketOptions]
 * @property {Number} [socketOptions.connectTimeout] Connection timeout in milliseconds. Default: 5000.
 * @property {Number} [socketOptions.defunctReadTimeoutThreshold] Determines the amount of requests that simultaneously
 * have to timeout before closing the connection. Default: 64.
 * @property {Boolean} [socketOptions.keepAlive] Whether to enable TCP keep-alive on the socket. Default: true.
 * @property {Number} [socketOptions.keepAliveDelay] TCP keep-alive delay in milliseconds. Default: 0.
 * @property {Number} [socketOptions.readTimeout] Per-host read timeout in milliseconds.
 * <p>
 *   Please note that this is not the maximum time a call to {@link Client#execute} may have to wait;
 *   this is the maximum time that call will wait for one particular Cassandra host, but other hosts will be tried if
 *   one of them timeout. In other words, a {@link Client#execute} call may theoretically wait up to
 *   <code>readTimeout * number_of_cassandra_hosts</code> (though the total number of hosts tried for a given query also
 *   depends on the LoadBalancingPolicy in use).
 * <p>When setting this value, keep in mind the following:</p>
 * <ul>
 *   <li>the timeout settings used on the Cassandra side (*_request_timeout_in_ms in cassandra.yaml) should be taken
 *   into account when picking a value for this read timeout. You should pick a value a couple of seconds greater than
 *   the Cassandra timeout settings.
 *   </li>
 *   <li>
 *     the read timeout is only approximate and only control the timeout to one Cassandra host, not the full query.
 *   </li>
 * </ul>
 * Setting a value of 0 disables read timeouts. Default: <code>12000</code>.
 * @property {Boolean} [socketOptions.tcpNoDelay] When set to true, it disables the Nagle algorithm. Default: true.
 * @property {Number} [socketOptions.coalescingThreshold] Buffer length in bytes use by the write queue before flushing
 * the frames. Default: 8000.
 * @property {AuthProvider} [authProvider] Provider to be used to authenticate to an auth-enabled cluster.
 * @property {RequestTracker} [requestTracker] The instance of RequestTracker used to monitor or log requests executed
 * with this instance.
 * @property {Object} [sslOptions] Client-to-node ssl options. When set the driver will use the secure layer.
 * You can specify cert, ca, ... options named after the Node.js <code>tls.connect()</code> options.
 * <p>
 *   It uses the same default values as Node.js <code>tls.connect()</code> except for <code>rejectUnauthorized</code>
 *   which is set to <code>false</code> by default (for historical reasons). This setting is likely to change
 *   in upcoming versions to enable validation by default.
 * </p>
 * @property {Object} [encoding] Encoding options.
 * @property {Function} [encoding.map] Map constructor to use for Cassandra map<k,v> type encoding and decoding.
 * If not set, it will default to Javascript Object with map keys as property names.
 * @property {Function} [encoding.set] Set constructor to use for Cassandra set<k> type encoding and decoding.
 * If not set, it will default to Javascript Array.
 * @property {Boolean} [encoding.copyBuffer] Determines if the network buffer should be copied for buffer based data
 * types (blob, uuid, timeuuid and inet).
 * <p>
 *   Setting it to true will cause that the network buffer is copied for each row value of those types,
 *   causing additional allocations but freeing the network buffer to be reused.
 *   Setting it to true is a good choice for cases where the Row and ResultSet returned by the queries are long-lived
 *   objects.
 * </p>
 * <p>
 *  Setting it to false will cause less overhead and the reference of the network buffer to be maintained until the row
 *  / result set are de-referenced.
 *  Default: true.
 * </p>
 * @property {Boolean} [encoding.useUndefinedAsUnset] Valid for Cassandra 2.2 and above. Determines that, if a parameter
 * is set to
 * <code>undefined</code> it should be encoded as <code>unset</code>.
 * <p>
 *  By default, ECMAScript <code>undefined</code> is encoded as <code>null</code> in the driver. Cassandra 2.2
 *  introduced the concept of unset.
 *  At driver level, you can set a parameter to unset using the field <code>types.unset</code>. Setting this flag to
 *  true allows you to use ECMAScript undefined as Cassandra <code>unset</code>.
 * </p>
 * <p>
 *   Default: true.
 * </p>
 * @property {Boolean} [encoding.useBigIntAsLong] Use [BigInt ECMAScript type](https://tc39.github.io/proposal-bigint/)
 * to represent CQL bigint and counter data types.
 * @property {Boolean} [encoding.useBigIntAsVarint] Use [BigInt ECMAScript
 * type](https://tc39.github.io/proposal-bigint/) to represent CQL varint data type.
 * @property {Array.<ExecutionProfile>} [profiles] The array of [execution profiles]{@link ExecutionProfile}.
 * @property {Function} [promiseFactory] Function to be used to create a <code>Promise</code> from a
 * callback-style function.
 * <p>
 *   Promise libraries often provide different methods to create a promise. For example, you can use Bluebird's
 *   <code>Promise.fromCallback()</code> method.
 * </p>
 * <p>
 *   By default, the driver will use the
 *   [Promise constructor]{@link https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Promise}.
 * </p>
 */
export declare interface ClientOptions {
    /* Excluded from this release type: applicationName */
    /* Excluded from this release type: applicationVersion */
    authProvider?: AuthProvider;
    contactPoints?: string[];
    localDataCenter?: string;
    /* Excluded from this release type: logEmitter */
    keyspace?: string;
    credentials?: {
        username: string;
        password: string;
    };
    cloud?: {
        secureConnectBundle: string | URL;
    };
    encoding?: {
        map?: Function;
        set?: Function;
        copyBuffer?: boolean;
        useUndefinedAsUnset?: boolean;
        useBigIntAsLong?: boolean;
        useBigIntAsVarint?: boolean;
    };
    /* Excluded from this release type: id */
    isMetadataSyncEnabled?: boolean;
    maxPrepared?: number;
    metrics?: ClientMetrics;
    /* Excluded from this release type: monitorReporting */
    policies?: {
        addressResolution?: AddressTranslator;
        loadBalancing?: LoadBalancingPolicy;
        reconnection?: ReconnectionPolicy;
        retry?: RetryPolicy;
        speculativeExecution?: SpeculativeExecutionPolicy;
        timestampGeneration?: TimestampGenerator;
    };
    pooling?: {
        coreConnectionsPerHost?: {
            [key: number]: number;
        };
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
    requestTracker?: RequestTracker;
    /* Excluded from this release type: sni */
    socketOptions?: {
        coalescingThreshold?: number;
        connectTimeout?: number;
        defunctReadTimeoutThreshold?: number;
        keepAlive?: boolean;
        keepAliveDelay?: number;
        readTimeout?: number;
        tcpNoDelay?: boolean;
    };
    sslOptions?: ConnectionOptions;
}

/**
 * Represents the state of a {@link Client}.
 * <p>
 * Exposes information on the connections maintained by a Client at a specific time.
 * </p>
 * @alias module:metadata~ClientState
 * @constructor
 */
export declare class ClientState {
    private _hosts;
    private _openConnections;
    private _inFlightQueries;
    /* Excluded from this release type: __constructor */
    /**
     * Get an array of hosts to which the client is connected to.
     * @return {Array<Host>}
     */
    getConnectedHosts(): Array<Host>;
    /**
     * Gets the amount of open connections to a given host.
     * @param {Host} host
     * @return {Number}
     */
    getOpenConnections(host: Host): number;
    /**
     * Gets the amount of queries that are currently being executed through a given host.
     * <p>
     * This corresponds to the number of queries that have been sent by the Client to server Host on one of its connections
     * but haven't yet obtained a response.
     * </p>
     * @param {Host} host
     * @return {Number}
     */
    getInFlightQueries(host: Host): number;
    /**
     * Returns the string representation of the instance.
     */
    toString(): string;
    /* Excluded from this release type: from */
}

export declare interface ColumnInfo {
    name: string;
    type: DataTypeInfo;
}

export declare const concurrent: {
    executeConcurrent: typeof executeConcurrent;
    ResultSetGroup: typeof ResultSetGroup;
};

/* Excluded from this release type: Connection */

/**
 * Consistency levels
 * @type {Object}
 * @property {Number} any Writing: A write must be written to at least one node. If all replica nodes for the given row key are down, the write can still succeed after a hinted handoff has been written. If all replica nodes are down at write time, an ANY write is not readable until the replica nodes for that row have recovered.
 * @property {Number} one Returns a response from the closest replica, as determined by the snitch.
 * @property {Number} two Returns the most recent data from two of the closest replicas.
 * @property {Number} three Returns the most recent data from three of the closest replicas.
 * @property {Number} quorum Reading: Returns the record with the most recent timestamp after a quorum of replicas has responded regardless of data center. Writing: A write must be written to the commit log and memory table on a quorum of replica nodes.
 * @property {Number} all Reading: Returns the record with the most recent timestamp after all replicas have responded. The read operation will fail if a replica does not respond. Writing: A write must be written to the commit log and memory table on all replica nodes in the cluster for that row.
 * @property {Number} localQuorum Reading: Returns the record with the most recent timestamp once a quorum of replicas in the current data center as the coordinator node has reported. Writing: A write must be written to the commit log and memory table on a quorum of replica nodes in the same data center as the coordinator node. Avoids latency of inter-data center communication.
 * @property {Number} eachQuorum Reading: Returns the record once a quorum of replicas in each data center of the cluster has responded. Writing: Strong consistency. A write must be written to the commit log and memtable on a quorum of replica nodes in all data centers.
 * @property {Number} serial Achieves linearizable consistency for lightweight transactions by preventing unconditional updates.
 * @property {Number} localSerial Same as serial but confined to the data center. A write must be written conditionally to the commit log and memtable on a quorum of replica nodes in the same data center.
 * @property {Number} localOne Similar to One but only within the DC the coordinator is in.
 */
export declare enum consistencies {
    any = 0,
    one = 1,
    two = 2,
    three = 3,
    quorum = 4,
    all = 5,
    localQuorum = 6,
    eachQuorum = 7,
    serial = 8,
    localSerial = 9,
    localOne = 10
}

/**
 * Mapping of consistency level codes to their string representation.
 * @type {Object}
 */
export declare const consistencyToString: {};

/**
 * A reconnection policy that waits a constant time between each reconnection attempt.
 */
export declare class ConstantReconnectionPolicy extends ReconnectionPolicy {
    private delay;
    /**
     * A reconnection policy that waits a constant time between each reconnection attempt.
     * @param {Number} delay Delay in ms
     * @constructor
     */
    constructor(delay: number);
    /**
     * A new reconnection schedule that returns the same next delay value
     * @returns { Iterator<number>} An infinite iterator
     */
    newSchedule(): Iterator<number>;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * @classdesc
 * A {@link SpeculativeExecutionPolicy} that schedules a given number of speculative executions,
 * separated by a fixed delay.
 * @extends {SpeculativeExecutionPolicy}
 */
export declare class ConstantSpeculativeExecutionPolicy extends SpeculativeExecutionPolicy {
    private _delay;
    private _maxSpeculativeExecutions;
    /**
     * Creates a new instance of ConstantSpeculativeExecutionPolicy.
     * @constructor
     * @param {Number} delay The delay between each speculative execution.
     * @param {Number} maxSpeculativeExecutions The amount of speculative executions that should be scheduled after the
     * initial execution. Must be strictly positive.
     */
    constructor(delay: number, maxSpeculativeExecutions: number);
    newPlan(keyspace: string, queryInfo: string | Array<string>): {
        nextExecution: () => number;
    };
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/* Excluded from this release type: ControlConnection */

declare type CustomSimpleColumnInfo = {
    code: (dataTypes.custom);
    info: CustomSimpleTypeNames;
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

declare type CustomSimpleTypeCodes = ('point' | 'polygon' | 'duration' | 'lineString' | 'dateRange');

declare type CustomSimpleTypeNames = (typeof customTypeNames[CustomSimpleTypeCodes]) | CustomSimpleTypeCodes | 'empty';

declare const customTypeNames: Readonly<{
    readonly duration: "org.apache.cassandra.db.marshal.DurationType";
    readonly lineString: "org.apache.cassandra.db.marshal.LineStringType";
    readonly point: "org.apache.cassandra.db.marshal.PointType";
    readonly polygon: "org.apache.cassandra.db.marshal.PolygonType";
    readonly dateRange: "org.apache.cassandra.db.marshal.DateRangeType";
    readonly vector: "org.apache.cassandra.db.marshal.VectorType";
}>;

/**
 * Creates a new instance of DataCollection
 * @param {String} name Name of the data object.
 * @classdesc Describes a table or a view
 * @alias module:metadata~DataCollection
 * @constructor
 * @abstract
 */
export declare class DataCollection extends EventEmitter.EventEmitter {
    /**
     * Name of the object
     * @type {String}
     */
    name: string;
    /**
     * False-positive probability for SSTable Bloom filters.
     * @type {number}
     */
    bloomFilterFalsePositiveChance: number;
    /**
     * Level of caching: all, keys_only, rows_only, none
     * @type {String}
     */
    caching: string;
    /**
     * A human readable comment describing the table.
     * @type {String}
     */
    comment: string;
    /**
     * Specifies the time to wait before garbage collecting tombstones (deletion markers)
     * @type {number}
     */
    gcGraceSeconds: number;
    /**
     * Compaction strategy class used for the table.
     * @type {String}
     */
    compactionClass: string;
    /**
     * Associative-array containing the compaction options keys and values.
     * @type {Object}
     */
    compactionOptions: {
        [option: string]: any;
    };
    /**
     * Associative-array containing the compaction options keys and values.
     * @type {Object}
     */
    compression: {
        class?: string;
        [option: string]: any;
    };
    /**
     * Specifies the probability of read repairs being invoked over all replicas in the current data center.
     * @type {number}
     */
    localReadRepairChance: number;
    /**
     * Specifies the probability with which read repairs should be invoked on non-quorum reads. The value must be
     * between 0 and 1.
     * @type {number}
     */
    readRepairChance: number;
    /**
     * An associative Array containing extra metadata for the table.
     * <p>
     * For Apache Cassandra versions prior to 3.0.0, this method always returns <code>null</code>.
     * </p>
     * @type {Object}
     */
    extensions: {
        [option: string]: any;
    };
    /**
     * When compression is enabled, this option defines the probability
     * with which checksums for compressed blocks are checked during reads.
     * The default value for this options is 1.0 (always check).
     * <p>
     *   For Apache Cassandra versions prior to 3.0.0, this method always returns <code>null</code>.
     * </p>
     * @type {Number|null}
     */
    crcCheckChance?: number;
    /**
     * Whether the populate I/O cache on flush is set on this table.
     * @type {Boolean}
     */
    populateCacheOnFlush: boolean;
    /**
     * Returns the default TTL for this table.
     * @type {Number}
     */
    defaultTtl: number;
    /**
     * * Returns the speculative retry option for this table.
     * @type {String}
     */
    speculativeRetry: string;
    /**
     * Returns the minimum index interval option for this table.
     * <p>
     *   Note: this option is available in Apache Cassandra 2.1 and above, and will return <code>null</code> for
     *   earlier versions.
     * </p>
     * @type {Number|null}
     */
    minIndexInterval?: number;
    /**
     * Returns the maximum index interval option for this table.
     * <p>
     * Note: this option is available in Apache Cassandra 2.1 and above, and will return <code>null</code> for
     * earlier versions.
     * </p>
     * @type {Number|null}
     */
    maxIndexInterval?: number;
    /**
     * Array describing the table columns.
     * @type {Array}
     */
    columns: ColumnInfo[];
    /**
     * An associative Array of columns by name.
     * @type {Object}
     */
    columnsByName: {
        [key: string]: ColumnInfo;
    };
    /**
     * Array describing the columns that are part of the partition key.
     * @type {Array}
     */
    partitionKeys: ColumnInfo[];
    /**
     * Array describing the columns that form the clustering key.
     * @type {Array}
     */
    clusteringKeys: ColumnInfo[];
    /**
     * Array describing the clustering order of the columns in the same order as the clusteringKeys.
     * @type {Array}
     */
    clusteringOrder: string[];
    /**
     * An associative Array containing nodesync options for this table.
     * <p>
     * For DSE versions prior to 6.0.0, this method always returns {@code null}.  If nodesync
     * was not explicitly configured for this table this method will also return {@code null}.
     * </p>
     * @type {Object}
     */
    nodesync?: object;
    /* Excluded from this release type: __constructor */
}

/**
 * DataStax module.
 * <p>
 *   Contains modules and classes to represent functionality that is specific to DataStax products.
 * </p>
 * @module datastax
 */
export declare const datastax: {
    graph: {
        Edge: typeof Edge;
        Element: typeof Element;
        Path: typeof Path;
        Property: typeof Property;
        Vertex: typeof Vertex;
        VertexProperty: typeof VertexProperty;
        asInt: typeof asInt;
        asDouble: typeof asDouble;
        asFloat: typeof asFloat;
        asTimestamp: typeof asTimestamp;
        asUdt: typeof asUdt;
        direction: {
            both: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            in: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            out: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            in_: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
        };
        /* Excluded from this release type: getCustomTypeSerializers */
        GraphResultSet: typeof GraphResultSet;
        /* Excluded from this release type: GraphTypeWrapper */
        t: {
            id: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            key: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            label: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
            value: {
                typeName: any;
                elementName: any;
                toString(): any;
            };
        };
        /* Excluded from this release type: UdtGraphWrapper */
    };
    search: {
        DateRange: typeof DateRange;
        DateRangeBound: typeof DateRangeBound;
        dateRangePrecision: {
            readonly year: 0;
            readonly month: 1;
            readonly day: 2;
            readonly hour: 3;
            readonly minute: 4;
            readonly second: 5;
            readonly millisecond: 6;
        };
    };
};

export declare type DataTypeInfo = SingleColumnInfo | CustomSimpleColumnInfo | MapColumnInfo | TupleColumnInfo | ListSetColumnInfo | VectorColumnInfo | OtherCustomColumnInfo | UdtColumnInfo | TupleListColumnInfoWithoutSubtype;

/**
 * CQL data types
 * @type {Object}
 * @property {Number} custom A custom type.
 * @property {Number} ascii ASCII character string.
 * @property {Number} bigint 64-bit signed long.
 * @property {Number} blob Arbitrary bytes (no validation).
 * @property {Number} boolean true or false.
 * @property {Number} counter Counter column (64-bit signed value).
 * @property {Number} decimal Variable-precision decimal.
 * @property {Number} double 64-bit IEEE-754 floating point.
 * @property {Number} float 32-bit IEEE-754 floating point.
 * @property {Number} int 32-bit signed integer.
 * @property {Number} text UTF8 encoded string.
 * @property {Number} timestamp A timestamp.
 * @property {Number} uuid Type 1 or type 4 UUID.
 * @property {Number} varchar UTF8 encoded string.
 * @property {Number} varint Arbitrary-precision integer.
 * @property {Number} timeuuid  Type 1 UUID.
 * @property {Number} inet An IP address. It can be either 4 bytes long (IPv4) or 16 bytes long (IPv6).
 * @property {Number} date A date without a time-zone in the ISO-8601 calendar system.
 * @property {Number} time A value representing the time portion of the day.
 * @property {Number} smallint 16-bit two's complement integer.
 * @property {Number} tinyint 8-bit two's complement integer.
 * @property {Number} list A collection of elements.
 * @property {Number} map Key/value pairs.
 * @property {Number} set A collection that contains no duplicate elements.
 * @property {Number} udt User-defined type.
 * @property {Number} tuple A sequence of values.
 */
export declare enum dataTypes {
    custom = 0,
    ascii = 1,
    bigint = 2,
    blob = 3,
    boolean = 4,
    counter = 5,
    decimal = 6,
    double = 7,
    float = 8,
    int = 9,
    text = 10,
    timestamp = 11,
    uuid = 12,
    varchar = 13,
    varint = 14,
    timeuuid = 15,
    inet = 16,
    date = 17,
    time = 18,
    smallint = 19,
    tinyint = 20,
    duration = 21,
    list = 32,
    map = 33,
    set = 34,
    udt = 48,
    tuple = 49
}

export declare namespace dataTypes {
    /**
     * Returns the typeInfo of a given type name
     * @param {string} name
     * @returns {DateTypeInfo}
     */
    export function getByName(name: string): DataTypeInfo;
}

/**
 * @classdesc
 * Represents a range of dates, corresponding to the Apache Solr type
 * <a href="https://cwiki.apache.org/confluence/display/solr/Working+with+Dates"><code>DateRangeField</code></a>.
 * <p>
 *   A date range can have one or two bounds, namely lower bound and upper bound, to represent an interval of time.
 *   Date range bounds are both inclusive. For example:
 * </p>
 * <ul>
 *   <li><code>2015 TO 2016-10</code> represents from the first day of 2015 to the last day of October 2016</li>
 *   <li><code>2015</code> represents during the course of the year 2015.</li>
 *   <li><code>2017 TO *</code> represents any date greater or equals to the first day of the year 2017.</li>
 * </ul>
 * <p>
 *   Note that this JavaScript representation of <code>DateRangeField</code> does not support Dates outside of the range
 *   supported by ECMAScript Date: –100,000,000 days to 100,000,000 days measured relative to midnight at the
 *   beginning of 01 January, 1970 UTC. Being <code>-271821-04-20T00:00:00.000Z</code> the minimum lower boundary
 *   and <code>275760-09-13T00:00:00.000Z</code> the maximum higher boundary.
 * <p>
 * @memberOf module:datastax/search
 */
declare class DateRange {
    lowerBound: DateRangeBound;
    upperBound: DateRangeBound;
    private _type;
    constructor(lowerBound: DateRangeBound, upperBound?: DateRangeBound);
    /**
     * Returns the <code>DateRange</code> representation of a given string.
     * <p>String representations of dates are always expressed in Coordinated Universal Time (UTC)</p>
     * @param {String} dateRangeString
     */
    static fromString(dateRangeString: string): DateRange;
    /**
     * Deserializes the buffer into a <code>DateRange</code>
     * @param {Buffer} buffer
     * @return {DateRange}
     */
    static fromBuffer(buffer: Buffer): DateRange;
    /**
     * Returns true if the value of this DateRange instance and other are the same.
     * @param {DateRange} other
     * @returns {Boolean}
     */
    equals(other: DateRange): boolean;
    /**
     * Returns the string representation of the instance.
     * @return {String}
     */
    toString(): string;
    /**
     * @intenal
     */
    toBuffer(): any;
}

/**
 * @classdesc
 * Represents a date range boundary, composed by a <code>Date</code> and a precision.
 * @param {Date} date The timestamp portion, representing a single moment in time. Consider using
 * <code>Date.UTC()</code> method to build the <code>Date</code> instance.
 * @param {Number} precision The precision portion. Valid values for <code>DateRangeBound</code> precision are
 * defined in the [dateRangePrecision]{@link module:datastax/search~dateRangePrecision} member.
 * @constructor
 * @memberOf module:datastax/search
 */
declare class DateRangeBound {
    date: Date;
    precision: number;
    /* Excluded from this release type: unbounded */
    /**
     * Represents a date range boundary, composed by a <code>Date</code> and a precision.
     * @param {Date} date The timestamp portion, representing a single moment in time. Consider using
     * <code>Date.UTC()</code> method to build the <code>Date</code> instance.
     * @param {Number} precision The precision portion. Valid values for <code>DateRangeBound</code> precision are
     * defined in the [dateRangePrecision]{@link module:datastax/search~dateRangePrecision} member.
     * @constructor
     */
    constructor(date: Date, precision: number);
    /**
     * Parses a date string and returns a DateRangeBound.
     * @param {String} boundaryString
     * @return {DateRangeBound}
     */
    static fromString(boundaryString: string): DateRangeBound;
    /**
     * Converts a {DateRangeBound} into a lower-bounded bound by rounding down its date
     * based on its precision.
     *
     * @param {DateRangeBound} bound The bound to round down.
     * @returns {DateRangeBound} with the date rounded down to the given precision.
     */
    static toLowerBound(bound: DateRangeBound): DateRangeBound;
    /**
     * Converts a {DateRangeBound} into a upper-bounded bound by rounding up its date
     * based on its precision.
     *
     * @param {DateRangeBound} bound The bound to round up.
     * @returns {DateRangeBound} with the date rounded up to the given precision.
     */
    static toUpperBound(bound: DateRangeBound): DateRangeBound;
    /**
     * Returns the string representation of the instance.
     * @return {String}
     */
    toString(): string;
    /**
     * Returns true if the value of this DateRange instance and other are the same.
     * @param {DateRangeBound} other
     * @return {boolean}
     */
    equals(other: DateRangeBound): boolean;
    /* Excluded from this release type: isUnbounded */
}

/**
 * A data-center aware Round-robin load balancing policy.
 * This policy provides round-robin queries over the nodes of the local
 * data center.
 */
export declare class DCAwareRoundRobinPolicy extends LoadBalancingPolicy {
    /* Excluded from this release type: localDc */
    private index;
    private localHostsArray;
    /**
     * A data-center aware Round-robin load balancing policy.
     * This policy provides round-robin queries over the nodes of the local
     * data center.
     * @param {String} [localDc] local datacenter name.  This value overrides the 'localDataCenter' Client option \
     * and is useful for cases where you have multiple execution profiles that you intend on using for routing
     * requests to different data centers.
     * @constructor
     */
    constructor(localDc?: string);
    init(client: Client, hosts: HostMap, callback: EmptyCallback): void;
    /**
     * Returns the distance depending on the datacenter.
     * @param {Host} host
     */
    getDistance(host: Host): distance;
    private _cleanHostCache;
    private _resolveLocalHosts;
    /**
     * It returns an iterator that yields local nodes.
     * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
     * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
     * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
     * second parameter.
     */
    newQueryPlan(keyspace: string, executionOptions: ExecutionOptions, callback: (error: Error, iterator: Iterator<Host>) => void): void;
    getOptions(): Map<string, any>;
}

/**
 * Decision information
 * @typedef {Object} DecisionInfo@typedef {Object} DecisionInfo
 * @property {Number} decision The decision as specified in
 * [retryDecision]{@link module:policies/retry~RetryPolicy.retryDecision}.
 * @property {Number} [consistency] The [consistency level]{@link module:types~consistencies}.
 * @property {useCurrentHost} [useCurrentHost] Determines if it should use the same host to retry the request.
 * <p>
 *   In the case that the current host is not available anymore, it will be retried on the next host even when
 *   <code>useCurrentHost</code> is set to <code>true</code>.
 * </p>
 */
export declare type DecisionInfo = {
    decision: number;
    consistency?: consistencies;
    useCurrentHost?: boolean;
};

/**
 * Returns a new instance of the default address translator policy used by the driver.
 * @returns {AddressTranslator}
 */
export declare const defaultAddressTranslator: () => AddressTranslator;

/**
 * A load-balancing policy implementation that attempts to fairly distribute the load based on the amount of in-flight
 * request per hosts. The local replicas are initially shuffled and
 * <a href="https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf">between the first two nodes in the
 * shuffled list, the one with fewer in-flight requests is selected as coordinator</a>.
 *
 * <p>
 *   Additionally, it detects unresponsive replicas and reorders them at the back of the query plan.
 * </p>
 *
 * <p>
 *   For graph analytics queries, it uses the preferred analytics graph server previously obtained by driver as first
 *   host in the query plan.
 * </p>
 */
export declare class DefaultLoadBalancingPolicy extends LoadBalancingPolicy {
    private _client;
    private _hosts;
    private _filteredHosts;
    private _preferredHost;
    private _index;
    private _filter;
    /**
     * Creates a new instance of <code>DefaultLoadBalancingPolicy</code>.
     * @param {String|Object} [options] The local data center name or the optional policy options object.
     * <p>
     *   Note that when providing the local data center name, it overrides <code>localDataCenter</code> option at
     *   <code>Client</code> level.
     * </p>
     * @param {String} [options.localDc] local data center name.  This value overrides the 'localDataCenter' Client option
     * and is useful for cases where you have multiple execution profiles that you intend on using for routing
     * requests to different data centers.
     * @param {Function} [options.filter] A function to apply to determine if hosts are included in the query plan.
     * The function takes a Host parameter and returns a Boolean.
     */
    constructor(options?: {
        localDc?: string;
        filter?: (host: Host) => boolean;
    } | string);
    /**
     * Initializes the load balancing policy, called after the driver obtained the information of the cluster.
     * @param {Client} client
     * @param {HostMap} hosts
     * @param {Function} callback
     */
    init(client: Client, hosts: HostMap, callback: EmptyCallback): void;
    /**
     * Returns the distance assigned by this policy to the provided host, relatively to the client instance.
     * @param {Host} host
     */
    getDistance(host: Host): distance;
    /**
     * Returns a host iterator to be used for a query execution.
     * @override
     * @param {String} keyspace
     * @param {ExecutionOptions} executionOptions
     * @param {Function} callback
     */
    newQueryPlan(keyspace: string, executionOptions: ExecutionOptions, callback: (error: Error, iterator: Iterator<Host>) => void): void;
    /**
     * Yields the preferred host first, followed by the host in the provided iterable
     * @param preferredHost
     * @param iterable
     * @private
     */
    private static _getPreferredHostFirst;
    /**
     * Yields the local hosts without the replicas already yielded
     * @param {Array<Host>} [localReplicas] The local replicas that we should avoid to include again
     * @private
     */
    private _getLocalHosts;
    private _getReplicasAndLocalHosts;
    /**
     * Yields the local replicas followed by the rest of local nodes.
     * @param {Array<Host>} replicas The local replicas
     */
    private yieldReplicasFirst;
    private _isHostNewlyUp;
    /**
     * Returns a boolean determining whether the host health is ok or not.
     * A Host is considered unhealthy when there are enough items in the queue (10 items in-flight) but the
     * Host is not responding to those requests.
     * @param {Host} h
     * @return {boolean}
     * @private
     */
    private _healthCheck;
    /**
     * Compares to host and returns 1 if it needs to favor the first host otherwise, -1.
     * @return {number}
     * @private
     */
    private _compare;
    private _getReplicas;
    /**
     * Returns an Array of hosts filtered by DC and predicate.
     * @returns {Array<Host>}
     * @private
     */
    private _getFilteredLocalHosts;
    private _getIndex;
    private _sendUnhealthyToTheBack;
    private _defaultFilter;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * Returns a new instance of the default load-balancing policy used by the driver.
 * @param {string} [localDc] When provided, it sets the data center that is going to be used as local for the
 * load-balancing policy instance.
 * <p>When localDc is undefined, the load-balancing policy instance will use the <code>localDataCenter</code>
 * provided in the {@link ClientOptions}.</p>
 * @returns {LoadBalancingPolicy}
 */
export declare const defaultLoadBalancingPolicy: (localDc?: string) => LoadBalancingPolicy;

/**
 * A default implementation of [ClientMetrics]{@link module:metrics~ClientMetrics} that exposes the driver events as
 * Node.js events.
 * <p>
 *   An instance of [DefaultMetrics]{@link module:metrics~DefaultMetrics} is configured by default in the client,
 *   you can access this instance using [Client#metrics]{@link Client#metrics} property.
 * </p>
 * @implements {module:metrics~ClientMetrics}
 * @alias module:metrics~DefaultMetrics
 * @example <caption>Listening to events emitted</caption>
 * defaultMetrics.errors.on('increment', err => totalErrors++);
 * defaultMetrics.errors.clientTimeout.on('increment', () => clientTimeoutErrors++);
 * defaultMetrics.speculativeRetries.on('increment', () => specExecsCount++);
 * defaultMetrics.responses.on('increment', latency => myHistogram.record(latency));
 */
export declare class DefaultMetrics extends ClientMetrics {
    errors: EventEmitter & {
        authentication: EventEmitter;
        clientTimeout: EventEmitter;
        connection: EventEmitter;
        other: EventEmitter;
        readTimeout: EventEmitter;
        unavailable: EventEmitter;
        writeTimeout: EventEmitter;
    };
    retries: EventEmitter & {
        clientTimeout: EventEmitter;
        other: EventEmitter;
        readTimeout: EventEmitter;
        unavailable: EventEmitter;
        writeTimeout: EventEmitter;
    };
    speculativeExecutions: EventEmitter & {
        increment: EventEmitter;
    };
    ignoredErrors: EventEmitter;
    responses: EventEmitter & {
        success: EventEmitter;
    };
    /**
     * Creates a new instance of [DefaultMetrics]{@link module:metrics~DefaultMetrics}.
     */
    constructor();
    /** @override */
    onAuthenticationError(e: Error | AuthenticationError): void;
    /** @override */
    onConnectionError(e: Error): void;
    /** @override */
    onReadTimeoutError(e: ResponseError): void;
    /** @override */
    onWriteTimeoutError(e: ResponseError): void;
    /** @override */
    onUnavailableError(e: Error): void;
    /** @override */
    onClientTimeoutError(e: OperationTimedOutError): void;
    /** @override */
    onOtherError(e: Error): void;
    /** @override */
    onClientTimeoutRetry(e: Error): void;
    /** @override */
    onOtherErrorRetry(e: Error): void;
    /** @override */
    onReadTimeoutRetry(e: Error): void;
    /** @override */
    onUnavailableRetry(e: Error): void;
    /** @override */
    onWriteTimeoutRetry(e: Error): void;
    /** @override */
    onIgnoreError(e: Error): void;
    /** @override */
    onSpeculativeExecution(): void;
    /** @override */
    onSuccessfulResponse(latency: number[]): void;
    /** @override */
    onResponse(latency: number[]): void;
}

export declare const defaultOptions: () => ClientOptions;

/**
 * Returns a new instance of the default reconnection policy used by the driver.
 * @returns {ReconnectionPolicy}
 */
export declare const defaultReconnectionPolicy: () => ReconnectionPolicy;

/**
 * Returns a new instance of the default retry policy used by the driver.
 * @returns {RetryPolicy}
 */
export declare const defaultRetryPolicy: () => RetryPolicy;

/**
 * Returns a new instance of the default speculative execution policy used by the driver.
 * @returns {SpeculativeExecutionPolicy}
 */
export declare const defaultSpeculativeExecutionPolicy: () => SpeculativeExecutionPolicy;

/**
 * Default implementation of [TableMappings]{@link module:mapping~TableMappings} that doesn't perform any conversion.
 * @alias module:mapping~DefaultTableMappings
 * @implements {module:mapping~TableMappings}
 */
export declare class DefaultTableMappings extends TableMappings {
    /**
     * Creates a new instance of {@link DefaultTableMappings}.
     */
    constructor();
    /**  @override */
    getColumnName(propName: string): string;
    /** @override */
    getPropertyName(columnName: string): string;
    /**
     * Creates a new object instance, using object initializer.
     */
    newObjectInstance(): object;
}

/**
 * Returns a new instance of the default timestamp generator used by the driver.
 * @returns {TimestampGenerator}
 */
export declare const defaultTimestampGenerator: () => TimestampGenerator;

/**
 * Represents the distance of Cassandra node as assigned by a LoadBalancingPolicy relatively to the driver instance.
 * @type {Object}
 * @property {Number} local A local node.
 * @property {Number} remote A remote node.
 * @property {Number} ignored A node that is meant to be ignored.
 */
export declare enum distance {
    local = 0,
    remote = 1,
    ignored = 2
}

declare type DocInfo = FindDocInfo | UpdateDocInfo | InsertDocInfo | RemoveDocInfo;

/**
 * Contains the error classes exposed by the driver.
 * @module errors
 */
/**
 * Base Error
 */
export declare class DriverError extends Error {
    info: string;
    isSocketError: boolean;
    innerError: any;
    requestNotWritten?: boolean;
    constructor(message: string);
}

/**
 * Represents a bug inside the driver or in a Cassandra host.
 */
export declare class DriverInternalError extends DriverError {
    /**
     * Represents a bug inside the driver or in a Cassandra host.
     * @param {String} message
     * @constructor
     */
    constructor(message: string);
}

declare interface DseClientOptions extends ClientOptions {
    id?: Uuid;
    applicationName?: string;
    applicationVersion?: string;
    monitorReporting?: {
        enabled?: boolean;
    };
    graphOptions?: GraphOptions;
}

/**
 * @classdesc
 * AuthProvider that provides GSSAPI authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @example
 * const client = new cassandra.Client({
 *   contactPoints: ['h1', 'h2'],
 *   authProvider: new cassandra.auth.DseGssapiAuthProvider()
 * });
 * @alias module:auth~DseGssapiAuthProvider
 */
export declare class DseGssapiAuthProvider extends AuthProvider {
    private _kerberos;
    private authorizationId;
    private service;
    private hostNameResolver;
    /**
     * Creates a new instance of <code>DseGssapiAuthProvider</code>.
     * @classdesc
     * AuthProvider that provides GSSAPI authenticator instances for clients to connect
     * to DSE clusters secured with the DseAuthenticator.
     * @param {Object} [gssOptions] GSSAPI authenticator options
     * @param {String} [gssOptions.authorizationId] The optional authorization ID. Providing an authorization ID allows the
     * currently authenticated user to act as a different user (a.k.a. proxy authentication).
     * @param {String} [gssOptions.service] The service to use. Defaults to 'dse'.
     * @param {Function} [gssOptions.hostNameResolver] A method to be used to resolve the name of the Cassandra node based
     * on the IP Address.  Defaults to [lookupServiceResolver]{@link module:auth~DseGssapiAuthProvider.lookupServiceResolver}
     * which resolves the FQDN of the provided IP to generate principals in the format of
     * <code>dse/example.com@MYREALM.COM</code>.
     * Alternatively, you can use [reverseDnsResolver]{@link module:auth~DseGssapiAuthProvider.reverseDnsResolver} to do a
     * reverse DNS lookup or [useIpResolver]{@link module:auth~DseGssapiAuthProvider.useIpResolver} to simply use the IP
     * address provided.
     * @param {String} [gssOptions.user] DEPRECATED, it will be removed in future versions. For proxy authentication, use
     * <code>authorizationId</code> instead.
     * @example
     * const client = new cassandra.Client({
     *   contactPoints: ['h1', 'h2'],
     *   authProvider: new cassandra.auth.DseGssapiAuthProvider()
     * });
     * @alias module:auth~DseGssapiAuthProvider
     * @constructor
     */
    constructor(gssOptions: {
        authorizationId?: string;
        service?: string;
        hostNameResolver?: Function;
        user?: string;
    });
    /**
     * Returns an Authenticator instance to be used by the driver when connecting to a host.
     * @param {String} endpoint The IP address and port number in the format ip:port.
     * @param {String} name Authenticator name.
     * @override
     * @returns {Authenticator}
     */
    newAuthenticator(endpoint: string, name: string): Authenticator;
    /**
     * Performs a lookupService query that resolves an IPv4 or IPv6 address to a hostname.  This ultimately makes a
     * <code>getnameinfo()</code> system call which depends on the OS to do hostname resolution.
     * <p/>
     * <b>Note:</b> Depends on <code>dns.lookupService</code> which was added in 0.12.  For older versions falls back on
     * [reverseDnsResolver]{@link module:auth~DseGssapiAuthProvider.reverseDnsResolver}.
     *
     * @param {String} ip IP address to resolve.
     * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
     */
    private static lookupServiceResolver;
    /**
     * Performs a reverse DNS query that resolves an IPv4 or IPv6 address to a hostname.
     * @param {String} ip IP address to resolve.
     * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
     */
    static reverseDnsResolver(ip: string, callback: Function): void;
    /**
     * Effectively a no op operation, returns the IP address provided.
     * @param {String} ip IP address to use.
     * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
     */
    static useIpResolver(ip: string, callback: Function): void;
}

/**
 * @classdesc
 * AuthProvider that provides plain text authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @extends AuthProvider
 * @alias module:auth~DsePlainTextAuthProvider
 * @example
 * const client = new cassandra.Client({
 *   contactPoints: ['h1', 'h2'],
 *   authProvider: new cassandra.auth.DsePlainTextAuthProvider('user', 'p@ssword1');
 * });
 */
export declare class DsePlainTextAuthProvider extends AuthProvider {
    private username;
    private password;
    private authorizationId;
    /**
     * Creates a new instance of <code>DsePlainTextAuthProvider</code>.
     * @classdesc
     * AuthProvider that provides plain text authenticator instances for clients to connect
     * to DSE clusters secured with the DseAuthenticator.
     * @param {String} username The username; cannot be <code>null</code>.
     * @param {String} password The password; cannot be <code>null</code>.
     * @param {String} [authorizationId] The optional authorization ID. Providing an authorization ID allows the currently
     * authenticated user to act as a different user (a.k.a. proxy authentication).
     * @extends AuthProvider
     * @alias module:auth~DsePlainTextAuthProvider
     * @example
     * const client = new cassandra.Client({
     *   contactPoints: ['h1', 'h2'],
     *   authProvider: new cassandra.auth.DsePlainTextAuthProvider('user', 'p@ssword1');
     * });
     * @constructor
     */
    constructor(username: string, password: string, authorizationId?: string);
    /**
     * Returns an Authenticator instance to be used by the driver when connecting to a host.
     * @param {String} endpoint The IP address and port number in the format ip:port.
     * @param {String} name Authenticator name.
     * @override
     * @returns {Authenticator}
     */
    newAuthenticator(endpoint: string, name: string): Authenticator;
}

/**
 * Creates a new instance of {@link Duration}.
 * @classdesc
 * Represents a duration. A duration stores separately months, days, and seconds due to the fact that the number of
 * days in a month varies, and a day can have 23 or 25 hours if a daylight saving is involved.
 * @param {Number} months The number of months.
 * @param {Number} days The number of days.
 * @param {Number|Long} nanoseconds The number of nanoseconds.
 * @constructor
 */
export declare class Duration {
    private months;
    private days;
    private nanoseconds;
    constructor(months: number, days: number, nanoseconds: number | Long__default);
    /**
     * Returns true if the value of the Duration instance and other are the same
     * @param {Duration} other
     * @returns {Boolean}
     */
    equals(other: Duration): boolean;
    /**
     * Serializes the duration and returns the representation of the value in bytes.
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Returns the string representation of the value.
     * @return {string}
     */
    toString(): string;
    /**
     * Creates a new {@link Duration} instance from the binary representation of the value.
     * @param {Buffer} buffer
     * @returns {Duration}
     */
    static fromBuffer(buffer: Buffer): Duration;
    /**
     * Creates a new {@link Duration} instance from the string representation of the value.
     * <p>
     *   Accepted formats:
     * </p>
     * <ul>
     * <li>multiple digits followed by a time unit like: 12h30m where the time unit can be:
     *   <ul>
     *     <li>{@code y}: years</li>
     *     <li>{@code m}: months</li>
     *     <li>{@code w}: weeks</li>
     *     <li>{@code d}: days</li>
     *     <li>{@code h}: hours</li>
     *     <li>{@code m}: minutes</li>
     *     <li>{@code s}: seconds</li>
     *     <li>{@code ms}: milliseconds</li>
     *     <li>{@code us} or {@code µs}: microseconds</li>
     *     <li>{@code ns}: nanoseconds</li>
     *   </ul>
     * </li>
     * <li>ISO 8601 format:  <code>P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W</code></li>
     * <li>ISO 8601 alternative format: <code>P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]</code></li>
     * </ul>
     * @param {String} input
     * @returns {Duration}
     */
    static fromString(input: string): Duration;
    /**
     * @param {Boolean} isNegative
     * @param {String} source
     * @returns {Duration}
     * @private
     */
    private static parseStandardFormat;
    /**
     * @param {Boolean} isNegative
     * @param {String} source
     * @returns {Duration}
     * @private
     */
    private static parseIso8601Format;
    /**
     * @param {Boolean} isNegative
     * @param {String} source
     * @returns {Duration}
     * @private
     */
    private static parseIso8601WeekFormat;
    /**
     * @param {Boolean} isNegative
     * @param {String} source
     * @returns {Duration}
     * @private
     */
    private static parseIso8601AlternativeFormat;
}

/**
 * @class
 * @classdesc
 * {@link AddressTranslator} implementation for multi-region EC2 deployments <strong>where clients are also deployed in EC2</strong>.
 * <p>
 * Its distinctive feature is that it translates addresses according to the location of the Cassandra host:
 * </p>
 * <ul>
 *  <li>addresses in different EC2 regions (than the client) are unchanged</li>
 *  <li>addresses in the same EC2 region are <strong>translated to private IPs</strong></li>
 * </ul>
 * <p>
 * This optimizes network costs, because Amazon charges more for communication over public IPs.
 * </p>
 */
export declare class EC2MultiRegionTranslator extends AddressTranslator {
    /**
     * Addresses in the same EC2 region are translated to private IPs and addresses in
     * different EC2 regions (than the client) are unchanged
     * @param {string} address The address of a node as returned by Cassandra.
     * @param {number} port The port number, as specified in the protocol options.
     * @param {Function} callback Callback to invoke with the translated endpoint.
     */
    translate(address: string, port: number, callback: Function): void;
    /**
     * Log method called to log errors that occurred while performing dns resolution.
     * You can assign your own method to the class instance to do proper logging.
     * @param {String} address
     * @param {Error} err
     */
    logError(address: string, err: Error): void;
}

/**
 * Represents a graph Edge.
 * @extends Element
 * @memberOf module:datastax/graph
 */
declare class Edge extends Element {
    outV: Vertex;
    outVLabel: string;
    inV: Vertex;
    inVLabel: string;
    properties: {
        [s: string]: any;
    };
    /**
     * @param id
     * @param outV
     * @param {String} outVLabel
     * @param {String} label
     * @param inV
     * @param {String} inVLabel
     * @param {Object<string, Property>} properties
     */
    constructor(id: any, outV: Vertex, outVLabel: string, label: string, inV: Vertex, inVLabel?: string, properties?: {
        [s: string]: Property;
    });
    private adaptProperties;
}

/**
 * Represents a graph Element.
 * @abstract
 * @memberOf module:datastax/graph
 */
declare abstract class Element {
    id: any;
    label: string;
    /**
     * @param id
     * @param label
     */
    constructor(id: any, label: string);
}

declare type EmptyCallback = (err: Error) => void;

export declare class Encoder {
    private encodingOptions;
    private handleBuffer;
    private decodeCollectionLength;
    private getLengthBuffer;
    private collectionLengthSize;
    /* Excluded from this release type: protocolVersion */
    private readonly customDecoders;
    private readonly customEncoders;
    /**
     * Serializes and deserializes to and from a CQL type and a Javascript Type.
     * @param {Number} protocolVersion
     * @param {ClientOptions} options
     * @constructor
     */
    constructor(protocolVersion: number, options: ClientOptions);
    /* Excluded from this release type: setProtocolVersion */
    private decodeBlob;
    /**
     *
     * @param {Buffer} bytes
     * @param {OtherCustomColumnInfo | VectorColumnInfo} columnInfo
     * @returns
     */
    private decodeCustom;
    private decodeUtf8String;
    private decodeAsciiString;
    private decodeBoolean;
    private decodeDouble;
    private decodeFloat;
    private decodeInt;
    private decodeSmallint;
    private decodeTinyint;
    private decodeLong;
    private _decodeCqlLongAsLong;
    private _decodeCqlLongAsBigInt;
    private decodeVarint;
    private _decodeVarintAsInteger;
    private _decodeVarintAsBigInt;
    private decodeDecimal;
    private decodeTimestamp;
    private decodeDate;
    private decodeTime;
    private decodeList;
    private decodeSet;
    private decodeMap;
    private decodeUuid;
    private decodeTimeUuid;
    private decodeInet;
    /**
     * Decodes a user defined type into an object
     * @param {Buffer} bytes
     * @param {UdtColumnInfo} columnInfo
     * @private
     */
    private decodeUdt;
    private decodeTuple;
    private encodeFloat;
    private encodeDouble;
    /**
     * @param {Date|String|Long|Number} value
     * @private
     */
    private encodeTimestamp;
    /**
     * @param {Date|String|LocalDate} value
     * @returns {Buffer}
     * @throws {TypeError}
     * @private
     */
    private encodeDate;
    /**
     * @param {String|LocalTime} value
     * @returns {Buffer}
     * @throws {TypeError}
     * @private
     */
    private encodeTime;
    /**
     * @param {Uuid|String|Buffer} value
     * @private
     */
    private encodeUuid;
    /**
     * @param {String|InetAddress|Buffer} value
     * @returns {Buffer}
     * @private
     */
    private encodeInet;
    /**
     * @param {Long|Buffer|String|Number} value
     * @private
     */
    private _encodeBigIntFromLong;
    private encodeLong;
    private _encodeBigIntFromBigInt;
    /**
     * @param {Integer|Buffer|String|Number} value
     * @returns {Buffer}
     * @private
     */
    private _encodeVarintFromInteger;
    private encodeVarint;
    private _encodeVarintFromBigInt;
    /**
     * @param {BigDecimal|Buffer|String|Number} value
     * @returns {Buffer}
     * @private
     */
    private encodeDecimal;
    private encodeString;
    private encodeUtf8String;
    private encodeAsciiString;
    private encodeBlob;
    /**
     *
     * @param {any} value
     * @param {OtherCustomColumnInfo | VectorColumnInfo} columnInfo
     * @returns
     */
    private encodeCustom;
    /**
     * @param {Boolean} value
     * @returns {Buffer}
     * @private
     */
    private encodeBoolean;
    /**
     * @param {Number|String} value
     * @private
     */
    private encodeInt;
    /**
     * @param {Number|String} value
     * @private
     */
    private encodeSmallint;
    /**
     * @param {Number} value
     * @private
     */
    private encodeTinyint;
    private encodeList;
    private encodeSet;
    /**
     * Serializes a map into a Buffer
     * @param value
     * @param {MapColumnInfo} columnInfo
     * @returns {Buffer}
     * @private
     */
    private encodeMap;
    /**
     *
     * @param {any} value
     * @param {UdtColumnInfo} columnInfo
     * @returns
     */
    private encodeUdt;
    /**
     *
     * @param {any} value
     * @param {TupleColumnInfo} columnInfo
     * @returns
     */
    private encodeTuple;
    /**
     *
     * @param {Buffer} buffer
     * @param {VectorColumnInfo} params
     * @returns {Vector}
     */
    private decodeVector;
    /**
     * @param {DataTypeInfo} cqlType
     * @returns {Number}
     */
    private serializationSizeIfFixed;
    /**
     * @param {Vector} value
     * @param {VectorColumnInfo} params
     * @returns {Buffer}
     */
    private encodeVector;
    /* Excluded from this release type: parseVectorTypeArgs */
    /* Excluded from this release type: setRoutingKeyFromUser */
    /* Excluded from this release type: setRoutingKeyFromMeta */
    /**
     * @param {Array} parts
     * @param {Array} routingIndexes
     * @param {Function} encodeParam
     * @returns {Number} The total length
     * @private
     */
    private _encodeRoutingKeyParts;
    /* Excluded from this release type: parseTypeName */
    /**
     * @param {String} keyspace
     * @param {Array} typeNames
     * @param {Function} udtResolver
     * @returns {Promise}
     * @private
     */
    private _parseChildTypes;
    /* Excluded from this release type: parseFqTypeName */
    /* Excluded from this release type: parseKeyTypes */
    /**
     *
     * @param {string} typeName
     * @param {number} startIndex
     * @param {number} length
     * @returns {UdtColumnInfo}
     */
    private _parseUdtName;
    private decoders;
    private encoders;
    /**
     * Decodes Cassandra bytes into Javascript values.
     * <p>
     * This is part of an <b>experimental</b> API, this can be changed future releases.
     * </p>
     * @param {Buffer} buffer Raw buffer to be decoded.
     * @param {DataTypeInfo} type
     */
    decode: (buffer: Buffer, type: DataTypeInfo) => any;
    /**
     * Encodes Javascript types into Buffer according to the Cassandra protocol.
     * <p>
     * This is part of an <b>experimental</b> API, this can be changed future releases.
     * </p>
     * @param {*} value The value to be converted.
     * @param {DataTypeInfo | Number | String} typeInfo The type information.
     * <p>It can be either a:</p>
     * <ul>
     *   <li>A <code>String</code> representing the data type.</li>
     *   <li>A <code>Number</code> with one of the values of {@link module:types~dataTypes dataTypes}.</li>
     *   <li>An <code>Object</code> containing the <code>type.code</code> as one of the values of
     *   {@link module:types~dataTypes dataTypes} and <code>type.info</code>.
     *   </li>
     * </ul>
     * @returns {Buffer}
     * @throws {TypeError} When there is an encoding error
     */
    encode: (value: any, typeInfo: DataTypeInfo | number | string) => Buffer;
    /* Excluded from this release type: guessDataType */
    private static isTypedArray;
}

declare class EnumValue {
    /* Excluded from this release type: typeName */
    /* Excluded from this release type: elementName */
    /* Excluded from this release type: __constructor */
    toString(): any;
}

export declare const errors: {
    ArgumentError: typeof ArgumentError;
    AuthenticationError: typeof AuthenticationError;
    BusyConnectionError: typeof BusyConnectionError;
    DriverError: typeof DriverError;
    OperationTimedOutError: typeof OperationTimedOutError;
    DriverInternalError: typeof DriverInternalError;
    NoHostAvailableError: typeof NoHostAvailableError;
    NotSupportedError: typeof NotSupportedError;
    ResponseError: typeof ResponseError;
    VIntOutOfRangeException: typeof VIntOutOfRangeException;
};

/**
 * Utilities for concurrent query execution with the DataStax Node.js Driver.
 * @module concurrent
 */
/**
 * Executes multiple queries concurrently at the defined concurrency level.
 * @static
 * @param {Client} client The {@link Client} instance.
 * @param {String|Array<{query, params}>} query The query to execute per each parameter item.
 * @param {Array<Array>|Stream|Object} parameters An {@link Array} or a readable {@link Stream} composed of {@link Array}
 * items representing each individual set of parameters. Per each item in the {@link Array} or {@link Stream}, an
 * execution is going to be made.
 * @param {Object} [options] The execution options.
 * @param {String} [options.executionProfile] The execution profile to be used.
 * @param {Number} [options.concurrencyLevel=100] The concurrency level to determine the maximum amount of in-flight
 * operations at any given time
 * @param {Boolean} [options.raiseOnFirstError=true] Determines whether execution should stop after the first failed
 * execution and the corresponding exception will be raised.
 * @param {Boolean} [options.collectResults=false] Determines whether each individual
 * [ResultSet]{@link module:types~ResultSet} instance should be collected in the grouped result.
 * @param {Number} [options.maxErrors=100] The maximum amount of errors to be collected before ignoring the rest of
 * the error results.
 * @returns {Promise<ResultSetGroup>} A <code>Promise</code> of {@link ResultSetGroup} that is resolved when all the
 * executions completed and it's rejected when <code>raiseOnFirstError</code> is <code>true</code> and there is one
 * or more failures.
 * @example <caption>Using a fixed query and an Array of Arrays as parameters</caption>
 * const query = 'INSERT INTO table1 (id, value) VALUES (?, ?)';
 * const parameters = [[1, 'a'], [2, 'b'], [3, 'c'], ]; // ...
 * const result = await executeConcurrent(client, query, parameters);
 * @example <caption>Using a fixed query and a readable stream</caption>
 * const stream = csvStream.pipe(transformLineToArrayStream);
 * const result = await executeConcurrent(client, query, stream);
 * @example <caption>Using a different queries</caption>
 * const queryAndParameters = [
 *   { query: 'INSERT INTO videos (id, name, user_id) VALUES (?, ?, ?)',
 *     params: [ id, name, userId ] },
 *   { query: 'INSERT INTO user_videos (user_id, id, name) VALUES (?, ?, ?)',
 *     params: [ userId, id, name ] },
 *   { query: 'INSERT INTO latest_videos (id, name, user_id) VALUES (?, ?, ?)',
 *     params: [ id, name, userId ] },
 * ];
 *
 * const result = await executeConcurrent(client, queryAndParameters);
 */
export declare function executeConcurrent(client: Client, query: string, parameters: any[][] | Readable, options?: Options): Promise<ResultSetGroup>;

export declare function executeConcurrent(client: Client, queries: Array<{
    query: string;
    params: any[];
}>, options?: Options): Promise<ResultSetGroup>;

/**
 * A base class that represents a wrapper around the user provided query options with getter methods and proper
 * default values.
 * <p>
 *   Note that getter methods might return <code>undefined</code> when not set on the query options or default
 *  {@link Client} options.
 * </p>
 */
export declare class ExecutionOptions {
    /**
     * Creates a new instance of {@link ExecutionOptions}.
     */
    constructor();
    /* Excluded from this release type: empty */
    /**
     * Determines if the stack trace before the query execution should be maintained.
     * @abstract
     * @returns {Boolean}
     */
    getCaptureStackTrace(): boolean;
    /**
     * Gets the [Consistency level]{@link module:types~consistencies} to be used for the execution.
     * @abstract
     * @returns {Number}
     */
    getConsistency(): consistencies;
    /**
     * Key-value payload to be passed to the server. On the server side, implementations of QueryHandler can use
     * this data.
     * @abstract
     * @returns {{ [key: string]: any }}
     */
    getCustomPayload(): {
        [key: string]: any;
    };
    /**
     * Gets the amount of rows to retrieve per page.
     * @abstract
     * @returns {Number}
     */
    getFetchSize(): number;
    /**
     * When a fixed host is set on the query options and the query plan for the load-balancing policy is not used, it
     * gets the host that should handle the query.
     * @returns {Host}
     */
    getFixedHost(): Host;
    /**
     * Gets the type hints for parameters given in the query, ordered as for the parameters.
     * @abstract
     * @returns {string[] | string[][]}
     */
    getHints(): string[] | string[][];
    /**
     * Determines whether the driver must retrieve the following result pages automatically.
     * <p>
     *   This setting is only considered by the [Client#eachRow()]{@link Client#eachRow} method.
     * </p>
     * @abstract
     * @returns {Boolean}
     */
    isAutoPage(): boolean;
    /**
     * Determines whether its a counter batch. Only valid for [Client#batch()]{@link Client#batch}, it will be ignored by
     * other methods.
     * @abstract
     * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
     */
    isBatchCounter(): boolean;
    /**
     * Determines whether the batch should be written to the batchlog. Only valid for
     * [Client#batch()]{@link Client#batch}, it will be ignored by other methods.
     * @abstract
     * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
     */
    isBatchLogged(): boolean;
    /**
     * Determines whether the query can be applied multiple times without changing the result beyond the initial
     * application.
     * @abstract
     * @returns {Boolean}
     */
    isIdempotent(): boolean;
    /**
     * Determines whether the query must be prepared beforehand.
     * @abstract
     * @returns {Boolean} A <code>Boolean</code> value, it can't be <code>undefined</code>.
     */
    isPrepared(): boolean;
    /**
     * Determines whether query tracing is enabled for the execution.
     * @abstract
     * @returns {Boolean}
     */
    isQueryTracing(): boolean;
    /**
     * Gets the keyspace for the query when set at query options level.
     * <p>
     *   Note that this method will return <code>undefined</code> when the keyspace is not set at query options level.
     *   It will only return the keyspace name when the user provided a different keyspace than the current
     *   {@link Client} keyspace.
     * </p>
     * @abstract
     * @returns {String}
     */
    getKeyspace(): string;
    /**
     * Gets the load balancing policy used for this execution.
     * @returns {LoadBalancingPolicy} A <code>LoadBalancingPolicy</code> instance, it can't be <code>undefined</code>.
     */
    getLoadBalancingPolicy(): LoadBalancingPolicy;
    /**
     * Gets the Buffer representing the paging state.
     * @abstract
     * @returns {Buffer}
     */
    getPageState(): Buffer;
    /* Excluded from this release type: getPreferredHost */
    /**
     * Gets the query options as provided to the execution method without setting the default values.
     * @returns {QueryOptions}
     */
    getRawQueryOptions(): QueryOptions;
    /**
     * Gets the timeout in milliseconds to be used for the execution per coordinator.
     * <p>
     *   A value of <code>0</code> disables client side read timeout for the execution. Default: <code>undefined</code>.
     * </p>
     * @abstract
     * @returns {Number}
     */
    getReadTimeout(): number;
    /**
     * Gets the [retry policy]{@link module:policies/retry} to be used.
     * @abstract
     * @returns {RetryPolicy} A <code>RetryPolicy</code> instance, it can't be <code>undefined</code>.
     */
    getRetryPolicy(): RetryPolicy;
    /* Excluded from this release type: getRowCallback */
    /* Excluded from this release type: getOrGenerateTimestamp */
    /* Excluded from this release type: getRoutingIndexes */
    /**
     * Gets the partition key(s) to determine which coordinator should be used for the query.
     * @abstract
     * @returns {Buffer|Array<Buffer>}
     */
    getRoutingKey(): Buffer | Array<Buffer>;
    /* Excluded from this release type: getRoutingNames */
    /**
     * Gets the the consistency level to be used for the serial phase of conditional updates.
     * @abstract
     * @returns {consistencies}
     */
    getSerialConsistency(): consistencies;
    /**
     * Gets the provided timestamp for the execution in microseconds from the unix epoch (00:00:00, January 1st, 1970).
     * <p>When a timestamp generator is used, this method returns <code>undefined</code>.</p>
     * @abstract
     * @returns {Number|Long|undefined|null}
     */
    getTimestamp(): number | Long__default | undefined | null;
    /* Excluded from this release type: setHints */
    /* Excluded from this release type: setKeyspace */
    /* Excluded from this release type: setPageState */
    /* Excluded from this release type: setPreferredHost */
    /* Excluded from this release type: setRoutingIndexes */
    /* Excluded from this release type: setRoutingKey */
}

/**
 * @classdesc
 * Represents a set configurations to be used in a statement execution to be used for a single {@link Client} instance.
 * <p>
 *   An {@link ExecutionProfile} instance should not be shared across different {@link Client} instances.
 * </p>
 * @example
 * const { Client, ExecutionProfile } = require('cassandra-driver');
 * const client = new Client({
 *   contactPoints: ['host1', 'host2'],
 *   profiles: [
 *     new ExecutionProfile('metrics-oltp', {
 *       consistency: consistency.localQuorum,
 *       retry: myRetryPolicy
 *     })
 *   ]
 * });
 *
 * client.execute(query, params, { executionProfile: 'metrics-oltp' }, callback);
 */
export declare class ExecutionProfile {
    /**
     * Consistency level.
     * @type {Number}
     */
    consistency?: consistencies;
    /**
     * Load-balancing policy
     * @type {LoadBalancingPolicy}
     */
    loadBalancing?: LoadBalancingPolicy;
    /**
     * Name of the execution profile.
     * @type {String}
     */
    name: string;
    /**
     * Client read timeout.
     * @type {Number}
     */
    readTimeout?: number;
    /**
     * Retry policy.
     * @type {RetryPolicy}
     */
    retry?: RetryPolicy;
    /**
     * Serial consistency level.
     * @type {Number}
     */
    serialConsistency?: consistencies;
    /**
     * The graph options for this profile.
     * @type {Object}
     * @property {String} language The graph language.
     * @property {String} name The graph name.
     * @property {String} readConsistency The consistency to use for graph write queries.
     * @property {String} source The graph traversal source.
     * @property {String} writeConsistency The consistency to use for graph write queries.
     */
    graphOptions?: {
        name?: string;
        language?: string;
        source?: string;
        readConsistency?: consistencies;
        writeConsistency?: consistencies;
        /* Excluded from this release type: results */
    };
    /**
     * Creates a new instance of {@link ExecutionProfile}.
     * Represents a set configurations to be used in a statement execution to be used for a single {@link Client} instance.
     * <p>
     *   An {@link ExecutionProfile} instance should not be shared across different {@link Client} instances.
     * </p>
     * @param {String} name Name of the execution profile.
     * <p>
     *   Use <code>'default'</code> to specify that the new instance should be the default {@link ExecutionProfile} if no
     *   profile is specified in the execution.
     * </p>
     * @param {Object} [options] Profile options, when any of the options is not specified the {@link Client} will the use
     * the ones defined in the default profile.
     * @param {Number} [options.consistency] The consistency level to use for this profile.
     * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
     * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
     * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
     * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
     * @param {Object} [options.graphOptions]
     * @param {String} [options.graphOptions.language] The graph language to use for graph queries.
     * <p>
     *   Note that this setting should normally be <code>undefined</code> or set by a utility method and it's not expected
     *   to be defined manually by the user.
     * </p>
     * @param {String} [options.graphOptions.results] The protocol to use for serializing and deserializing graph results.
     * <p>
     *   Note that this setting should normally be <code>undefined</code> or set by a utility method and it's not expected
     *   to be defined manually by the user.
     * </p>
     * @param {String} [options.graphOptions.name] The graph name to use for graph queries.
     * @param {Number} [options.graphOptions.readConsistency] The consistency level to use for graph read queries.
     * @param {String} [options.graphOptions.source] The graph traversal source name to use for graph queries.
     * @param {Number} [options.graphOptions.writeConsistency] The consistency level to use for graph write queries.
     * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
     * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
     * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
     * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
     * Profile options, when any of the options is not specified the {@link Client} will the use
     * the ones defined in the default profile.
     * @param {Number} [options.consistency] The consistency level to use for this profile.
     * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
     * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
     * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
     * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
     * @param {Object} [options.graphOptions]
     * @param {String} [options.graphOptions.language] The graph language to use for graph queries.
     * <p>
     *   Note that this setting should normally be <code>undefined</code> or set by a utility method and it's not expected
     *   to be defined manually by the user.
     * </p>
     * @param {String} [options.graphOptions.results] The protocol to use for serializing and deserializing graph results.
     * <p>
     *   Note that this setting should normally be <code>undefined</code> or set by a utility method and it's not expected
     *   to be defined manually by the user.
     * </p>
     * @param {String} [options.graphOptions.name] The graph name to use for graph queries.
     * @param {Number} [options.graphOptions.readConsistency] The consistency level to use for graph read queries.
     * @param {String} [options.graphOptions.source] The graph traversal source name to use for graph queries.
     * @param {Number} [options.graphOptions.writeConsistency] The consistency level to use for graph write queries.
     * @param {LoadBalancingPolicy} [options.loadBalancing] The load-balancing policy to use for this profile.
     * @param {Number} [options.readTimeout] The client per-host request timeout to use for this profile.
     * @param {RetryPolicy} [options.retry] The retry policy to use for this profile.
     * @param {Number} [options.serialConsistency] The serial consistency level to use for this profile.
     * @example
     * const { Client, ExecutionProfile } = require('cassandra-driver');
     * const client = new Client({
     *   contactPoints: ['host1', 'host2'],
     *   profiles: [
     *     new ExecutionProfile('metrics-oltp', {
     *       consistency: consistency.localQuorum,
     *       retry: myRetryPolicy
     *     })
     *   ]
     * });
     *
     * client.execute(query, params, { executionProfile: 'metrics-oltp' }, callback);
     * @constructor
     */
    constructor(name: string, options?: {
        consistency?: consistencies;
        loadBalancing?: LoadBalancingPolicy;
        readTimeout?: number;
        retry?: RetryPolicy;
        serialConsistency?: consistencies;
        graphOptions?: {
            name?: string;
            language?: string;
            source?: string;
            readConsistency?: consistencies;
            writeConsistency?: consistencies;
        };
    });
}

/**
 * A reconnection policy that waits exponentially longer between each
 * reconnection attempt (but keeps a constant delay once a maximum delay is reached).
 * <p>
 *   A random amount of jitter (+/- 15%) will be added to the pure exponential delay value to avoid situations
 *   where many clients are in the reconnection process at exactly the same time. The jitter will never cause the
 *   delay to be less than the base delay, or more than the max delay.
 * </p>
 */
export declare class ExponentialReconnectionPolicy extends ReconnectionPolicy {
    private baseDelay;
    private maxDelay;
    private startWithNoDelay;
    /**
     * A reconnection policy that waits exponentially longer between each
     * reconnection attempt (but keeps a constant delay once a maximum delay is reached).
     * <p>
     *   A random amount of jitter (+/- 15%) will be added to the pure exponential delay value to avoid situations
     *   where many clients are in the reconnection process at exactly the same time. The jitter will never cause the
     *   delay to be less than the base delay, or more than the max delay.
     * </p>
     * @param {Number} baseDelay The base delay in milliseconds to use for the schedules created by this policy.
     * @param {Number} maxDelay The maximum delay in milliseconds to wait between two reconnection attempt.
     * @param {Boolean} [startWithNoDelay] Determines if the first attempt should be zero delay
     * @constructor
     */
    constructor(baseDelay: number, maxDelay: number, startWithNoDelay?: boolean);
    /**
     * A new schedule that uses an exponentially growing delay between reconnection attempts.
     * @returns {Iterator<number>} An infinite iterator.
     */
    newSchedule(): Iterator<number>;
    /**
     * Adds a random portion of +-15% to the delay provided.
     * Initially, its adds a random value of 15% to avoid reconnection before reaching the base delay.
     * When the schedule reaches max delay, only subtracts a random portion of 15%.
     */
    private _addJitter;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * @classdesc
 * A retry policy that never retries nor ignores.
 * <p>
 * All of the methods of this retry policy unconditionally return
 * [rethrow]{@link module:policies/retry~Retry#rethrowResult()}. If this policy is used, retry logic will have to be
 * implemented in business code.
 * </p>
 * @alias module:policies/retry~FallthroughRetryPolicy
 * @extends RetryPolicy
 */
export declare class FallthroughRetryPolicy extends RetryPolicy {
    /**
     * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
     */
    onReadTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, isDataPresent: boolean): DecisionInfo;
    /**
     * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
     */
    onRequestError(info: OperationInfo, consistency: consistencies, err: Error): DecisionInfo;
    /**
     * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
     */
    onUnavailable(info: OperationInfo, consistency: consistencies, required: number, alive: number): DecisionInfo;
    /**
     * Implementation of RetryPolicy method that returns [rethrow]{@link module:policies/retry~Retry#rethrowResult()}.
     */
    onWriteTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, writeType: string): DecisionInfo;
}

export declare type FindDocInfo = {
    fields?: string[];
    orderBy?: {
        [key: string]: string;
    };
    limit?: number;
};

/* Excluded from this release type: frameFlags */

/* Excluded from this release type: FrameHeader */

/* Excluded from this release type: generateTimestamp */

export declare class Geometry {
    static types: {
        readonly Point2D: 1;
        readonly LineString: 2;
        readonly Polygon: 3;
    };
    /* Excluded from this release type: getEndianness */
    /* Excluded from this release type: readInt32 */
    /* Excluded from this release type: readDouble */
    /* Excluded from this release type: writeInt32 */
    /* Excluded from this release type: writeDouble */
    /* Excluded from this release type: writeEndianness */
    /* Excluded from this release type: useBESerialization */
}

/**
 * Geometry module.
 * <p>
 *   Contains the classes to represent the set of additional CQL types for geospatial data that come with
 *   DSE 5.0.
 * </p>
 * @module geometry
 */
export declare const geometry: {
    Point: typeof Point;
    LineString: typeof LineString;
    Polygon: typeof Polygon;
    Geometry: typeof Geometry;
};

/* Excluded from this release type: getCustomSerializers */

/* Excluded from this release type: getDataTypeNameByCode */

export declare const graph: {
    Edge: typeof Edge;
    Element: typeof Element;
    Path: typeof Path;
    Property: typeof Property;
    Vertex: typeof Vertex;
    VertexProperty: typeof VertexProperty;
    asInt: typeof asInt;
    asDouble: typeof asDouble;
    asFloat: typeof asFloat;
    asTimestamp: typeof asTimestamp;
    asUdt: typeof asUdt;
    direction: {
        both: EnumValue;
        in: EnumValue;
        out: EnumValue;
        in_: EnumValue;
    };
    /* Excluded from this release type: getCustomTypeSerializers */
    GraphResultSet: typeof GraphResultSet;
    /* Excluded from this release type: GraphTypeWrapper */
    t: {
        id: EnumValue;
        key: EnumValue;
        label: EnumValue;
        value: EnumValue;
    };
    /* Excluded from this release type: UdtGraphWrapper */
};

declare type GraphOptions = {
    language?: string;
    name?: string;
    readConsistency?: consistencies;
    readTimeout?: number;
    source?: string;
    writeConsistency?: consistencies;
};

declare interface GraphQueryOptions extends QueryOptions {
    graphLanguage?: string;
    graphName?: string;
    graphReadConsistency?: consistencies;
    graphSource?: string;
    graphWriteConsistency?: consistencies;
    graphResults?: string;
}

/**
 * Represents the result set of a [graph query execution]{@link Client#executeGraph} containing vertices, edges, or
 * scalar values depending on the query.
 * <p>
 * It allows iteration of the items using <code>for..of</code> statements under ES2015 and exposes
 * <code>forEach()</code>, <code>first()</code>, and <code>toArray()</code> to access the underlying items.
 * </p>
 * @example
 * for (let vertex of result) { ... }
 * @example
 * const arr = result.toArray();
 * @example
 * const vertex = result.first();
 * @alias module:datastax/graph~GraphResultSet
 */
declare class GraphResultSet implements Iterable<any> {
    info: typeof ResultSet.prototype.info;
    length: number;
    pageState: string;
    private rows;
    private rowParser;
    /**
     * @param {ResultSet} result The result set from the query execution.
     * @param {Function} [rowParser] Optional row parser function.
     * @constructor
     */
    constructor(result: ResultSet, rowParser?: Function);
    /**
     * Returns the first element of the result or null if the result is empty.
     * @returns {Object}
     */
    first(): object | null;
    /**
     * Executes a provided function once per result element.
     * @param {Function} callback Function to execute for each element, taking two arguments: currentValue and index.
     * @param {Object} [thisArg] Value to use as <code>this</code> when executing callback.
     */
    forEach(callback: Function, thisArg?: object): void;
    /**
     * Returns an Array of graph result elements (vertex, edge, scalar).
     * @returns {Array}
     */
    toArray(): Array<any>;
    /**
     * Returns a new Iterator object that contains the values for each index in the result.
     * @returns {Iterator}
     */
    values(): Iterator<any>;
    /**
     * Gets the traversers contained in the result set.
     * @returns {IterableIterator}
     */
    getTraversers(): IterableIterator<any>;
    /**
     * Makes the result set iterable using `for..of`.
     * @returns {Iterator}
     */
    [Symbol.iterator](): Iterator<any, any, any>;
}

/* Excluded from this release type: GraphTypeWrapper */

/**
 * Represents a unique set of values.
 * @constructor
 */
declare class HashSet {
    length: number;
    items: object;
    constructor();
    /**
     * Adds a new item to the set.
     * @param {Object} key
     * @returns {boolean} Returns true if it was added to the set; false if the key is already present.
     */
    add(key: any): boolean;
    /**
     * @returns {boolean} Returns true if the key is present in the set.
     */
    contains(key: any): boolean;
    /**
     * Removes the item from set.
     * @param key
     * @return {boolean} Returns true if the key existed and was removed, otherwise it returns false.
     */
    remove(key: any): boolean;
    /**
     * Returns an array containing the set items.
     * @returns {Array}
     */
    toArray(): Array<any>;
}

/**
 * Represents a Cassandra node.
 * @extends EventEmitter
 */
export declare class Host extends EventEmitter.EventEmitter {
    address: string;
    private setDownAt;
    private log;
    /* Excluded from this release type: isUpSince */
    /* Excluded from this release type: pool */
    cassandraVersion: string;
    datacenter: string;
    rack: string;
    tokens: string[];
    hostId: Uuid;
    /* Excluded from this release type: dseVersion */
    /* Excluded from this release type: workloads */
    private _distance;
    private _healthResponseCounter;
    /* Excluded from this release type: reconnectionSchedule */
    /* Excluded from this release type: options */
    private reconnectionDelay;
    private _healthResponseCountTimer;
    private _metadata;
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: setDown */
    /* Excluded from this release type: setUp */
    /* Excluded from this release type: checkIsUp */
    /* Excluded from this release type: shutdown */
    /**
     * Determines if the node is UP now (seen as UP by the driver).
     * @returns {boolean}
     */
    isUp(): boolean;
    /**
     * Determines if the host can be considered as UP.
     * Deprecated: Use {@link Host#isUp()} instead.
     * @returns {boolean}
     */
    canBeConsideredAsUp(): boolean;
    /* Excluded from this release type: setDistance */
    /* Excluded from this release type: setProtocolVersion */
    /* Excluded from this release type: borrowConnection */
    /* Excluded from this release type: warmupPool */
    /* Excluded from this release type: initializePool */
    /* Excluded from this release type: getActiveConnection */
    /* Excluded from this release type: getResponseCount */
    /* Excluded from this release type: checkHealth */
    /* Excluded from this release type: removeFromPool */
    /* Excluded from this release type: getInFlight */
    /**
     * Validates that the internal state of the connection pool.
     * If the pool size is smaller than expected, schedule a new connection attempt.
     * If the amount of connections is 0 for not ignored hosts, the host must be down.
     * @private
     */
    private _checkPoolState;
    /**
     * Executed after an scheduled new connection attempt finished
     * @private
     */
    private _onNewConnectionOpen;
    /**
     * Returns an array containing the Cassandra Version as an Array of Numbers having the major version in the first
     * position.
     * @returns {Array.<Number>}
     */
    getCassandraVersion(): Array<number>;
    /**
     * Gets the DSE version of the host as an Array, containing the major version in the first position.
     * In case the cluster is not a DSE cluster, it returns an empty Array.
     * @returns {Array.<Number>}
     */
    getDseVersion(): Array<number>;
}

/**
 * Represents an associative-array of {@link Host hosts} that can be iterated.
 * It creates an internal copy when adding or removing, making it safe to iterate using the values()
 * method within async operations.
 * @extends events.EventEmitter
 * @constructor
 */
export declare class HostMap extends EventEmitter.EventEmitter {
    private _items;
    private _values;
    length: number;
    /* Excluded from this release type: __constructor */
    /**
     * Executes a provided function once per map element.
     * @param {Function} callback
     */
    forEach(callback: (value: Host, key: string) => void): void;
    /**
     * Gets a {@link Host host} by key or undefined if not found.
     * @param {String} key
     * @returns {Host}
     */
    get(key: string): Host;
    /**
     * Returns an array of host addresses.
     * @returns {Array.<String>}
     */
    keys(): Array<string>;
    /* Excluded from this release type: remove */
    /* Excluded from this release type: removeMultiple */
    /* Excluded from this release type: set */
    /* Excluded from this release type: slice */
    /* Excluded from this release type: push */
    /**
     * Returns a shallow copy of the values of the map.
     * @returns {Array.<Host>}
     */
    values(): Array<Host>;
    /* Excluded from this release type: clear */
    /* Excluded from this release type: inspect */
    /* Excluded from this release type: toJSON */
}

/**
 * @classdesc
 * A retry policy that avoids retrying non-idempotent statements.
 * <p>
 * In case of write timeouts or unexpected errors, this policy will always return
 * [rethrowResult()]{@link module:policies/retry~RetryPolicy#rethrowResult} if the statement is deemed non-idempotent
 * (see [QueryOptions.isIdempotent]{@link QueryOptions}).
 * <p/>
 * For all other cases, this policy delegates the decision to the child policy.
 * @extends module:policies/retry~RetryPolicy
 * @deprecated Since version 4.0 non-idempotent operations are never tried for write timeout or request error, use the
 * default retry policy instead.
 */
export declare class IdempotenceAwareRetryPolicy extends RetryPolicy {
    private _childPolicy;
    /**
     * Creates a new instance of <code>IdempotenceAwareRetryPolicy</code>.
     * This is a retry policy that avoids retrying non-idempotent statements.
     * <p>
     * In case of write timeouts or unexpected errors, this policy will always return
     * [rethrowResult()]{@link module:policies/retry~RetryPolicy#rethrowResult} if the statement is deemed non-idempotent
     * (see [QueryOptions.isIdempotent]{@link QueryOptions}).
     * <p/>
     * For all other cases, this policy delegates the decision to the child policy.
     * @param {RetryPolicy} [childPolicy] The child retry policy to wrap. When not defined, it will use an instance of
     * [RetryPolicy]{@link module:policies/retry~RetryPolicy} as child policy.
     * @constructor
     * @deprecated Since version 4.0 non-idempotent operations are never tried for write timeout or request error, use the
     * default retry policy instead.
     */
    constructor(childPolicy?: RetryPolicy);
    onReadTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, isDataPresent: boolean): DecisionInfo;
    onRequestError(info: OperationInfo, consistency: consistencies, err: Error): DecisionInfo;
    onUnavailable(info: OperationInfo, consistency: consistencies, required: number, alive: number): DecisionInfo;
    /**
     * If the query is not idempotent, it return a rethrow decision. Otherwise, it relies on the child policy to decide.
     */
    onWriteTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, writeType: string): DecisionInfo;
}

/**
 * @classdesc Describes a CQL index.
 * @alias module:metadata~Index
 */
export declare class Index {
    /**
     * Name of the index.
     * @type {String}
     */
    name: string;
    /**
     * Target of the index.
     * @type {String}
     */
    target: string;
    /**
     * A numeric value representing index kind (0: custom, 1: keys, 2: composite);
     * @type {IndexKind}
     */
    kind: IndexKind;
    /**
     * An associative array containing the index options
     * @type {Object}
     */
    options: object;
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: fromRows */
    /* Excluded from this release type: fromColumnRows */
    /**
     * Determines if the index is of composites kind
     * @returns {Boolean}
     */
    isCompositesKind(): boolean;
    /**
     * Determines if the index is of keys kind
     * @returns {Boolean}
     */
    isKeysKind(): boolean;
    /**
     * Determines if the index is of custom kind
     * @returns {Boolean}
     */
    isCustomKind(): boolean;
}

export declare enum IndexKind {
    custom = 0,
    keys = 1,
    composites = 2
}

/** @module types */
/**
 * @class
 * @classdesc Represents an v4 or v6 Internet Protocol (IP) address.
 */
export declare class InetAddress {
    private buffer;
    length: number;
    version: number;
    /**
     * Creates a new instance of InetAddress
     * @param {Buffer} buffer
     * @constructor
     */
    constructor(buffer: Buffer);
    /**
     * Parses the string representation and returns an Ip address
     * @param {String} value
     */
    static fromString(value: string): InetAddress;
    /**
     * Compares 2 addresses and returns true if the underlying bytes are the same
     * @param {InetAddress} other
     * @returns {Boolean}
     */
    equals(other: InetAddress): boolean;
    /**
     * Returns the underlying buffer
     * @returns {Buffer}
     */
    getBuffer(): Buffer;
    /* Excluded from this release type: inspect */
    /**
     * Returns the string representation of the IP address.
     * <p>For v4 IP addresses, a string in the form of d.d.d.d is returned.</p>
     * <p>
     *   For v6 IP addresses, a string in the form of x:x:x:x:x:x:x:x is returned, where the 'x's are the hexadecimal
     *   values of the eight 16-bit pieces of the address, according to rfc5952.
     *   In cases where there is more than one field of only zeros, it can be shortened. For example, 2001:0db8:0:0:0:1:0:1
     *   will be expressed as 2001:0db8::1:0:1.
     * </p>
     * @param {String} [encoding] If set to 'hex', the hex representation of the buffer is returned.
     * @returns {String}
     */
    toString(encoding?: string): string;
    /**
     * Returns the string representation.
     * Method used by the native JSON.stringify() to serialize this instance.
     */
    toJSON(): string;
    /**
     * Validates for a IPv4-Mapped IPv6 according to https://tools.ietf.org/html/rfc4291#section-2.5.5
     * @private
     * @param {Buffer} buffer
     */
    private static isValidIPv4Mapped;
}

export declare type InsertDocInfo = {
    fields?: string[];
    ttl?: number;
    ifNotExists?: boolean;
};

declare const inspectMethod: unique symbol;

/**
 * A two's-complement integer an array containing bits of the
 * integer in 32-bit (signed) pieces, given in little-endian order (i.e.,
 * lowest-order bits in the first piece), and the sign of -1 or 0.
 *
 * See the from* functions below for other convenient ways of constructing
 * Integers.
 *
 * The internal representation of an integer is an array of 32-bit signed
 * pieces, along with a sign (0 or -1) that indicates the contents of all the
 * other 32-bit pieces out to infinity.  We use 32-bit pieces because these are
 * the size of integers on which Javascript performs bit-operations.  For
 * operations like addition and multiplication, we split each number into 16-bit
 * pieces, which can easily be multiplied within Javascript's floating-point
 * representation without overflow or change in sign.
 * @final
 */
export declare class Integer {
    private bits_;
    private sign_;
    /**
     * Constructs a two's-complement integer an array containing bits of the
     * integer in 32-bit (signed) pieces, given in little-endian order (i.e.,
     * lowest-order bits in the first piece), and the sign of -1 or 0.
     *
     * See the from* functions below for other convenient ways of constructing
     * Integers.
     *
     * The internal representation of an integer is an array of 32-bit signed
     * pieces, along with a sign (0 or -1) that indicates the contents of all the
     * other 32-bit pieces out to infinity.  We use 32-bit pieces because these are
     * the size of integers on which Javascript performs bit-operations.  For
     * operations like addition and multiplication, we split each number into 16-bit
     * pieces, which can easily be multiplied within Javascript's floating-point
     * representation without overflow or change in sign.
     *
     * @constructor
     * @param {Array.<number>} bits Array containing the bits of the number.
     * @param {number} sign The sign of the number: -1 for negative and 0 positive.
     * @final
     */
    constructor(bits: number[], sign: number);
    /**
     * A cache of the Integer representations of small integer values.
     * @type {!Object}
     * @private
     */
    private static IntCache_;
    /**
     * Returns an Integer representing the given (32-bit) integer value.
     * @param {number} value A 32-bit integer value.
     * @return {!Integer} The corresponding Integer value.
     */
    static fromInt(value: number): Integer;
    /**
     * Returns an Integer representing the given value, provided that it is a finite
     * number.  Otherwise, zero is returned.
     * @param {number} value The value in question.
     * @return {!Integer} The corresponding Integer value.
     */
    static fromNumber(value: number): Integer;
    /**
     * Returns a Integer representing the value that comes by concatenating the
     * given entries, each is assumed to be 32 signed bits, given in little-endian
     * order (lowest order bits in the lowest index), and sign-extending the highest
     * order 32-bit value.
     * @param {Array.<number>} bits The bits of the number, in 32-bit signed pieces,
     *     in little-endian order.
     * @return {!Integer} The corresponding Integer value.
     */
    static fromBits(bits: number[]): Integer;
    /**
     * Returns an Integer representation of the given string, written using the
     * given radix.
     * @param {string} str The textual representation of the Integer.
     * @param {number=} opt_radix The radix in which the text is written.
     * @return {!Integer} The corresponding Integer value.
     */
    static fromString(str: string, opt_radix?: number): Integer;
    /**
     * Returns an Integer representation of a given big endian Buffer.
     * The internal representation of bits contains bytes in groups of 4
     * @param {Buffer} buf
     * @returns {Integer}
     */
    static fromBuffer(buf: Buffer): Integer;
    /**
     * Returns a big endian buffer representation of an Integer.
     * Internally the bits are represented using 4 bytes groups (numbers),
     * in the Buffer representation there might be the case where we need less than the 4 bytes.
     * For example: 0x00000001 -> '01', 0xFFFFFFFF -> 'FF', 0xFFFFFF01 -> 'FF01'
     * @param {Integer} value
     * @returns {Buffer}
     */
    static toBuffer(value: Integer): Buffer;
    /**
     * A number used repeatedly in calculations.  This must appear before the first
     * call to the from* functions below.
     * @type {number}
     * @private
     */
    private static TWO_PWR_32_DBL_;
    /** @type {!Integer} */
    static ZERO: Integer;
    /** @type {!Integer} */
    static ONE: Integer;
    /**
     * @type {!Integer}
     * @private
     */
    private static TWO_PWR_24_;
    /**
     * Returns the value, assuming it is a 32-bit integer.
     * @return {number} The corresponding int value.
     */
    toInt(): number;
    /** @return {number} The closest floating-point representation to this value. */
    toNumber(): number;
    /**
     * @param {number=} opt_radix The radix in which the text should be written.
     * @return {string} The textual representation of this value.
     * @override
     */
    toString(opt_radix?: number): string;
    /**
     * Returns the index-th 32-bit (signed) piece of the Integer according to
     * little-endian order (i.e., index 0 contains the smallest bits).
     * @param {number} index The index in question.
     * @return {number} The requested 32-bits as a signed number.
     */
    getBits(index: number): number;
    /**
     * Returns the index-th 32-bit piece as an unsigned number.
     * @param {number} index The index in question.
     * @return {number} The requested 32-bits as an unsigned number.
     */
    getBitsUnsigned(index: number): number;
    /** @return {number} The sign bit of this number, -1 or 0. */
    getSign(): number;
    /** @return {boolean} Whether this value is zero. */
    isZero(): boolean;
    /** @return {boolean} Whether this value is negative. */
    isNegative(): boolean;
    /** @return {boolean} Whether this value is odd. */
    isOdd(): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer equals the other.
     */
    equals(other: Integer): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer does not equal the other.
     */
    notEquals(other: Integer): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer is greater than the other.
     */
    greaterThan(other: Integer): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer is greater than or equal to the other.
     */
    greaterThanOrEqual(other: Integer): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer is less than the other.
     */
    lessThan(other: Integer): boolean;
    /**
     * @param {Integer} other Integer to compare against.
     * @return {boolean} Whether this Integer is less than or equal to the other.
     */
    lessThanOrEqual(other: Integer): boolean;
    /**
     * Compares this Integer with the given one.
     * @param {Integer} other Integer to compare against.
     * @return {number} 0 if they are the same, 1 if the this is greater, and -1
     *     if the given one is greater.
     */
    compare(other: Integer): number;
    /**
     * Returns an integer with only the first numBits bits of this value, sign
     * extended from the final bit.
     * @param {number} numBits The number of bits by which to shift.
     * @return {!Integer} The shorted integer value.
     */
    shorten(numBits: number): Integer;
    /** @return {!Integer} The negation of this value. */
    negate(): Integer;
    /**
     * Returns the sum of this and the given Integer.
     * @param {Integer} other The Integer to add to this.
     * @return {!Integer} The Integer result.
     */
    add(other: Integer): Integer;
    /**
     * Returns the difference of this and the given Integer.
     * @param {Integer} other The Integer to subtract from this.
     * @return {!Integer} The Integer result.
     */
    subtract(other: Integer): Integer;
    /**
     * Returns the product of this and the given Integer.
     * @param {Integer} other The Integer to multiply against this.
     * @return {!Integer} The product of this and the other.
     */
    multiply(other: Integer): Integer;
    /**
     * Carries any overflow from the given index into later entries.
     * @param {Array.<number>} bits Array of 16-bit values in little-endian order.
     * @param {number} index The index in question.
     * @private
     */
    private static carry16_;
    /**
     * Returns this Integer divided by the given one.
     * @param {Integer} other Th Integer to divide this by.
     * @return {!Integer} This value divided by the given one.
     */
    divide(other: Integer): Integer;
    /**
     * Returns this Integer modulo the given one.
     * @param {Integer} other The Integer by which to mod.
     * @return {!Integer} This value modulo the given one.
     */
    modulo(other: Integer): Integer;
    /** @return {!Integer} The bitwise-NOT of this value. */
    not(): Integer;
    /**
     * Returns the bitwise-AND of this Integer and the given one.
     * @param {Integer} other The Integer to AND with this.
     * @return {!Integer} The bitwise-AND of this and the other.
     */
    and(other: Integer): Integer;
    /**
     * Returns the bitwise-OR of this Integer and the given one.
     * @param {Integer} other The Integer to OR with this.
     * @return {!Integer} The bitwise-OR of this and the other.
     */
    or(other: Integer): Integer;
    /**
     * Returns the bitwise-XOR of this Integer and the given one.
     * @param {Integer} other The Integer to XOR with this.
     * @return {!Integer} The bitwise-XOR of this and the other.
     */
    xor(other: Integer): Integer;
    /**
     * Returns this value with bits shifted to the left by the given amount.
     * @param {number} numBits The number of bits by which to shift.
     * @return {!Integer} This shifted to the left by the given amount.
     */
    shiftLeft(numBits: number): Integer;
    /**
     * Returns this value with bits shifted to the right by the given amount.
     * @param {number} numBits The number of bits by which to shift.
     * @return {!Integer} This shifted to the right by the given amount.
     */
    shiftRight(numBits: number): Integer;
    /* Excluded from this release type: inspect */
    /**
     * Returns a Integer whose value is the absolute value of this
     * @returns {Integer}
     */
    abs(): Integer;
    /**
     * Returns the string representation.
     * Method used by the native JSON.stringify() to serialize this instance.
     */
    toJSON(): string;
}

declare interface Keyspace {
    name: any;
    durableWrites: any;
    strategy: any;
    strategyOptions: any;
    tokenToReplica: any;
    udts: any;
    tables: any;
    functions: any;
    aggregates: any;
    virtual: any;
    views: any;
    graphEngine: any;
}

/**
 * @classdesc
 * A LineString is a one-dimensional object representing a sequence of points and the line segments connecting them.
 * @example
 * new LineString(new Point(10.99, 20.02), new Point(14, 26), new Point(34, 1.2));
 * @alias module:geometry~LineString
 * @extends {Geometry}
 */
export declare class LineString extends Geometry {
    /* Excluded from this release type: points */
    /**
     * Creates a new {@link LineString} instance.
     * @param {...Point} points A sequence of {@link Point} items as arguments.
     */
    constructor(...points: Point[] | Point[][]);
    /**
     * Creates a {@link LineString} instance from
     * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
     * representation of a line.
     * @param {Buffer} buffer
     * @returns {LineString}
     */
    static fromBuffer(buffer: Buffer): LineString;
    /**
     * Creates a {@link LineString} instance from
     * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
     * representation of a line.
     * @param {String} textValue
     * @returns {LineString}
     */
    static fromString(textValue: string): LineString;
    /* Excluded from this release type: parseSegments */
    /**
     * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
     * representation of this instance.
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Returns true if the values of the linestrings are the same, otherwise it returns false.
     * @param {LineString} other
     * @returns {Boolean}
     */
    equals(other: LineString): boolean;
    /**
     * Returns Well-known Text (WKT) representation of the geometry object.
     * @returns {String}
     */
    toString(): string;
    /* Excluded from this release type: useBESerialization */
    /**
     * Returns a JSON representation of this geo-spatial type.
     */
    toJSON(): object;
}

declare type ListSetColumnInfo = {
    code: (dataTypes.list | dataTypes.set);
    info: DataTypeInfo;
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

export declare const loadBalancing: {
    AllowListPolicy: typeof AllowListPolicy;
    DCAwareRoundRobinPolicy: typeof DCAwareRoundRobinPolicy;
    DefaultLoadBalancingPolicy: typeof DefaultLoadBalancingPolicy;
    LoadBalancingPolicy: typeof LoadBalancingPolicy;
    RoundRobinPolicy: typeof RoundRobinPolicy;
    TokenAwarePolicy: typeof TokenAwarePolicy;
    WhiteListPolicy: typeof WhiteListPolicy;
};

/**
 * Base class for Load Balancing Policies.
 */
export declare class LoadBalancingPolicy {
    protected client: Client;
    protected hosts: HostMap;
    /* Excluded from this release type: localDc */
    /**
     * Initializes the load balancing policy, called after the driver obtained the information of the cluster.
     * @param {Client} client
     * @param {HostMap} hosts
     * @param {EmptyCallback} callback
     */
    init(client: Client, hosts: HostMap, callback: EmptyCallback): void;
    /**
     * Returns the distance assigned by this policy to the provided host.
     * @param {Host} host
     */
    getDistance(host: Host): distance;
    /**
     * Returns an iterator with the hosts for a new query.
     * Each new query will call this method. The first host in the result will
     * then be used to perform the query.
     * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
     * @param {ExecutionOptions} executionOptions The information related to the execution of the request.
     * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
     * second parameter.
     */
    newQueryPlan(keyspace: string, executionOptions: ExecutionOptions, callback: (error: Error, iterator: Iterator<Host>) => void): void;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * @class
 * @classdesc A date without a time-zone in the ISO-8601 calendar system, such as 2010-08-05.
 * <p>
 *   LocalDate is an immutable object that represents a date, often viewed as year-month-day. For example, the value "1st October 2014" can be stored in a LocalDate.
 * </p>
 * <p>
 *   This class does not store or represent a time or time-zone. Instead, it is a description of the date, as used for birthdays. It cannot represent an instant on the time-line without additional information such as an offset or time-zone.
 * </p>
 * <p>
 *   Note that this type can represent dates in the range [-5877641-06-23; 5881580-07-17] while the ES5 date type can only represent values in the range of [-271821-04-20; 275760-09-13].
 *   In the event that year, month, day parameters do not fall within the ES5 date range an Error will be thrown.  If you wish to represent a date outside of this range, pass a single
 *   parameter indicating the days since epoch.  For example, -1 represents 1969-12-31.
 * </p>
 */
export declare class LocalDate {
    /**
     * The date representation if falls within a range of an ES5 data type, otherwise an invalid date.
     */
    date: Date;
    private _value;
    year: number;
    month: number;
    day: number;
    /**
     * Creates a new instance of LocalDate.
     * A date without a time-zone in the ISO-8601 calendar system, such as 2010-08-05.
     * <p>
     *   LocalDate is an immutable object that represents a date, often viewed as year-month-day. For example, the value "1st October 2014" can be stored in a LocalDate.
     * </p>
     * <p>
     *   This class does not store or represent a time or time-zone. Instead, it is a description of the date, as used for birthdays. It cannot represent an instant on the time-line without additional information such as an offset or time-zone.
     * </p>
     * <p>
     *   Note that this type can represent dates in the range [-5877641-06-23; 5881580-07-17] while the ES5 date type can only represent values in the range of [-271821-04-20; 275760-09-13].
     *   In the event that year, month, day parameters do not fall within the ES5 date range an Error will be thrown.  If you wish to represent a date outside of this range, pass a single
     *   parameter indicating the days since epoch.  For example, -1 represents 1969-12-31.
     * </p>
     * @param {Number} year The year or days since epoch.  If days since epoch, month and day should not be provided.
     * @param {Number} [month] Between 1 and 12 inclusive.
     * @param {Number} [day] Between 1 and the number of days in the given month of the given year.
     *
     * @constructor
     */
    constructor(year: number, month?: number, day?: number);
    /**
     * Creates a new instance of LocalDate using the current year, month and day from the system clock in the default time-zone.
     */
    static now(): LocalDate;
    /**
     * Creates a new instance of LocalDate using the current date from the system clock at UTC.
     */
    static utcNow(): LocalDate;
    /**
     * Creates a new instance of LocalDate using the year, month and day from the provided local date time.
     * @param {Date} date
     */
    static fromDate(date: Date): LocalDate;
    /**
     * Creates a new instance of LocalDate using the year, month and day provided in the form: yyyy-mm-dd or
     * days since epoch (i.e. -1 for Dec 31, 1969).
     * @param {String} value
     */
    static fromString(value: string): LocalDate;
    /**
     * Creates a new instance of LocalDate using the bytes representation.
     * @param {Buffer} buffer
     */
    static fromBuffer(buffer: Buffer): LocalDate;
    /**
     * Compares this LocalDate with the given one.
     * @param {LocalDate} other date to compare against.
     * @return {number} 0 if they are the same, 1 if the this is greater, and -1
     * if the given one is greater.
     */
    compare(other: LocalDate): number;
    /**
     * Returns true if the value of the LocalDate instance and other are the same
     * @param {LocalDate} other
     * @returns {Boolean}
     */
    equals(other: LocalDate): boolean;
    inspect(): string;
    /**
     * Gets the bytes representation of the instance.
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Gets the string representation of the instance in the form: yyyy-mm-dd if
     * the value can be parsed as a Date, otherwise days since epoch.
     * @returns {String}
     */
    toString(): string;
    /**
     * Gets the string representation of the instance in the form: yyyy-mm-dd, valid for JSON.
     * @returns {String}
     */
    toJSON(): string;
}

/**
 * @class
 * @classdesc A time without a time-zone in the ISO-8601 calendar system, such as 10:30:05.
 * <p>
 *   LocalTime is an immutable date-time object that represents a time, often viewed as hour-minute-second. Time is represented to nanosecond precision. For example, the value "13:45.30.123456789" can be stored in a LocalTime.
 * </p>
 */
export declare class LocalTime {
    private value;
    /**
     * Gets the hour component of the time represented by the current instance, a number from 0 to 23.
     * @type Number
     */
    hour: number;
    /**
     * Gets the minute component of the time represented by the current instance, a number from 0 to 59.
     * @type Number
     */
    minute: number;
    /**
     * Gets the second component of the time represented by the current instance, a number from 0 to 59.
     * @type Number
     */
    second: number;
    /**
     * Gets the nanoseconds component of the time represented by the current instance, a number from 0 to 999999999.
     * @type Number
     */
    nanosecond: number;
    private _partsCache?;
    /**
     * Creates a new instance of LocalTime.
     * A time without a time-zone in the ISO-8601 calendar system, such as 10:30:05.
     * <p>
     *   LocalTime is an immutable date-time object that represents a time, often viewed as hour-minute-second. Time is represented to nanosecond precision. For example, the value "13:45.30.123456789" can be stored in a LocalTime.
     * </p>
     * @param {Long} totalNanoseconds Total nanoseconds since midnight.
     * @constructor
     */
    constructor(totalNanoseconds: Long__default);
    /**
     * Parses a string representation and returns a new LocalTime.
     * @param {String} value
     * @returns {LocalTime}
     */
    static fromString(value: string): LocalTime;
    /**
     * Uses the current local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
     * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the time nanosecond portion.
     * @returns {LocalTime}
     */
    static now(nanoseconds?: number): LocalTime;
    /**
     * Uses the provided local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
     * @param {Date} date Local date portion to extract the time passed since midnight.
     * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the nanosecond time portion.
     * @returns {LocalTime}
     */
    static fromDate(date: Date, nanoseconds?: number): LocalTime;
    /**
     * Uses the provided local time (in milliseconds) and the nanoseconds to create a new instance of LocalTime
     * @param {Number} milliseconds A Number from 0 to 86,399,999.
     * @param {Number} [nanoseconds] A Number from 0 to 999,999, representing the time nanosecond portion.
     * @returns {LocalTime}
     */
    static fromMilliseconds(milliseconds: number, nanoseconds?: number): LocalTime;
    /**
     * Creates a new instance of LocalTime from the bytes representation.
     * @param {Buffer} value
     * @returns {LocalTime}
     */
    static fromBuffer(value: Buffer): LocalTime;
    /**
     * Compares this LocalTime with the given one.
     * @param {LocalTime} other time to compare against.
     * @return {number} 0 if they are the same, 1 if the this is greater, and -1
     * if the given one is greater.
     */
    compare(other: LocalTime): number;
    /**
     * Returns true if the value of the LocalTime instance and other are the same
     * @param {LocalTime} other
     * @returns {Boolean}
     */
    equals(other: LocalTime): boolean;
    /**
     * Gets the total amount of nanoseconds since midnight for this instance.
     * @returns {Long}
     */
    getTotalNanoseconds(): Long__default;
    inspect(): string;
    /**
     * Returns a big-endian bytes representation of the instance
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Returns the string representation of the instance in the form of hh:MM:ss.ns
     * @returns {String}
     */
    toString(): string;
    /**
     * Gets the string representation of the instance in the form: hh:MM:ss.ns
     * @returns {String}
     */
    toJSON(): string;
    /* Excluded from this release type: _getParts */
}

export { Long }

declare type MapColumnInfo = {
    code: (dataTypes.map);
    info: [DataTypeInfo, DataTypeInfo];
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

/**
 * Represents an object mapper for Apache Cassandra and DataStax Enterprise.
 * @alias module:mapping~Mapper
 * @example <caption>Creating a Mapper instance with some options for the model 'User'</caption>
 * const mappingOptions = {
 *   models: {
 *     'User': {
 *       tables: ['users'],
 *       mappings: new UnderscoreCqlToCamelCaseMappings(),
 *       columnNames: {
 *         'userid': 'id'
 *       }
 *     }
 *   }
 * };
 * const mapper = new Mapper(client, mappingOptions);
 * @example <caption>Creating a Mapper instance with other possible options for a model</caption>
 * const mappingOptions = {
 *   models: {
 *     'Video': {
 *       tables: ['videos', 'user_videos', 'latest_videos', { name: 'my_videos_view', isView: true }],
 *       mappings: new UnderscoreCqlToCamelCaseMappings(),
 *       columnNames: {
 *         'videoid': 'id'
 *       },
 *       keyspace: 'ks1'
 *     }
 *   }
 * };
 * const mapper = new Mapper(client, mappingOptions);
 */
export declare class Mapper {
    private client;
    private _modelMappingInfos;
    private _modelMappers;
    /**
     * Creates a new instance of Mapper.
     * @param {Client} client The Client instance to use to execute the queries and fetch the metadata.
     * @param {MappingOptions} [options] The [MappingOptions]{@link module:mapping~MappingOptions} containing the
     * information of the models and table mappings.
     */
    constructor(client: Client, options?: MappingOptions);
    /**
     * Gets a [ModelMapper]{@link module:mapping~ModelMapper} that is able to map documents of a certain model into
     * CQL rows.
     * @param {String} name The name to identify the model. Note that the name is case-sensitive.
     * @returns {ModelMapper} A [ModelMapper]{@link module:mapping~ModelMapper} instance.
     */
    forModel<T = any>(name: string): ModelMapper<T>;
    /**
     * Executes a batch of queries represented in the items.
     * @param {Array<ModelBatchItem>} items
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
     * changing the result beyond the initial application.
     * <p>
     *   The mapper uses the generated queries to determine the default value. When an UPDATE is generated with a
     *   counter column or appending/prepending to a list column, the execution is marked as not idempotent.
     * </p>
     * <p>
     *   Additionally, the mapper uses the safest approach for queries with lightweight transactions (Compare and
     *   Set) by considering them as non-idempotent. Lightweight transactions at client level with transparent retries can
     *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
     * </p>
     * @param {Boolean} [executionOptions.logged=true] Determines whether the batch should be written to the batchlog.
     * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
     * unix epoch (00:00:00, January 1st, 1970).
     * @returns {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result}.
     */
    batch(items: Array<ModelBatchItem>, executionOptions: string | MappingExecutionOptions): Promise<Result>;
}

export declare const mapping: {
    Mapper: typeof Mapper;
    ModelMapper: typeof ModelMapper;
    ModelBatchMapper: typeof ModelBatchMapper;
    ModelBatchItem: typeof ModelBatchItem;
    Result: typeof Result;
    TableMappings: typeof TableMappings;
    DefaultTableMappings: typeof DefaultTableMappings;
    UnderscoreCqlToCamelCaseMappings: typeof UnderscoreCqlToCamelCaseMappings;
    q: {
        in_: (arr: any) => QueryOperator;
        gt: (value: any) => QueryOperator;
        gte: (value: any) => QueryOperator;
        lt: (value: any) => QueryOperator;
        lte: (value: any) => QueryOperator;
        notEq: (value: any) => QueryOperator;
        and: (condition1: any, condition2: any) => QueryOperator;
        incr: (value: any) => QueryAssignment;
        decr: (value: any) => QueryAssignment;
        append: (value: any) => QueryAssignment;
        prepend: (value: any) => QueryAssignment;
        remove: (value: any) => QueryAssignment;
    };
};

export declare type MappingExecutionOptions = {
    executionProfile?: string;
    isIdempotent?: boolean;
    logged?: boolean;
    timestamp?: number | Long__default;
    fetchSize?: number;
    pageState?: number;
};

/* Excluded from this release type: MappingHandler */

export declare type MappingOptions = {
    models: {
        [key: string]: ModelOptions;
    };
};

/**
 * @classdesc Describes a CQL materialized view.
 * @alias module:metadata~MaterializedView
 * @augments {module:metadata~DataCollection}
 * @constructor
 */
export declare class MaterializedView extends DataCollection {
    /**
     * Name of the table.
     * @type {String}
     */
    tableName: string;
    /**
     * View where clause.
     * @type {String}
     */
    whereClause: string;
    /**
     * Determines if all the table columns where are included in the view.
     * @type {boolean}
     */
    includeAllColumns: boolean;
    /* Excluded from this release type: __constructor */
}

/**
 * Represents cluster and schema information.
 * The metadata class acts as a internal state of the driver.
 */
export declare class Metadata {
    keyspaces: {
        [name: string]: Keyspace;
    };
    /* Excluded from this release type: initialized */
    private _isDbaas;
    private _schemaParser;
    /* Excluded from this release type: log */
    private _preparedQueries;
    /* Excluded from this release type: tokenizer */
    /* Excluded from this release type: primaryReplicas */
    /* Excluded from this release type: ring */
    /* Excluded from this release type: tokenRanges */
    /* Excluded from this release type: ringTokensAsStrings */
    /* Excluded from this release type: datacenters */
    private options;
    private controlConnection;
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: setCassandraVersion */
    /**
     * Determines whether the cluster is provided as a service.
     * @returns {boolean} true when the cluster is provided as a service (DataStax Astra), <code>false<code> when it's a
     * different deployment (on-prem).
     */
    isDbaas(): boolean;
    /* Excluded from this release type: setProductTypeAsDbaas */
    /* Excluded from this release type: setPartitioner */
    /* Excluded from this release type: buildTokens */
    /**
     * Gets the keyspace metadata information and updates the internal state of the driver.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the keyspaces metadata refresh completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * @param {String} name Name of the keyspace.
     * @param {Function} [callback] Optional callback.
     */
    refreshKeyspace(name: string, callback: EmptyCallback): void;
    refreshKeyspace(name: string): Promise<void>;
    /**
     * @param {String} name
     * @private
     */
    private _refreshKeyspace;
    /**
     * Gets the metadata information of all the keyspaces and updates the internal state of the driver.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the keyspace metadata refresh completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * @param {Boolean|Function} [waitReconnect] Determines if it should wait for reconnection in case the control connection is not
     * connected at the moment. Default: true.
     * @param {Function} [callback] Optional callback.
     */
    refreshKeyspaces(waitReconnect: boolean, callback: EmptyCallback): void;
    refreshKeyspaces(waitReconnect?: boolean): Promise<void>;
    refreshKeyspaces(callback: EmptyCallback): void;
    /* Excluded from this release type: refreshKeyspacesInternal */
    private _getKeyspaceReplicas;
    /**
     * Gets the host list representing the replicas that contain the given partition key, token or token range.
     * <p>
     *   It uses the pre-loaded keyspace metadata to retrieve the replicas for a token for a given keyspace.
     *   When the keyspace metadata has not been loaded, it returns null.
     * </p>
     * @param {String} keyspaceName
     * @param {Buffer|Token|TokenRange} token Can be Buffer (serialized partition key), Token or TokenRange
     * @returns {Array}
     */
    getReplicas(keyspaceName: string, token: Buffer | Token | TokenRange): Array<Host>;
    /**
     * Gets the token ranges that define data distribution in the ring.
     *
     * @returns {Set<TokenRange>} The ranges of the ring or empty set if schema metadata is not enabled.
     */
    getTokenRanges(): Set<TokenRange>;
    /**
     * Gets the token ranges that are replicated on the given host, for
     * the given keyspace.
     *
     * @param {String} keyspaceName The name of the keyspace to get ranges for.
     * @param {Host} host The host.
     * @returns {Set<TokenRange>|null} Ranges for the keyspace on this host or null if keyspace isn't found or hasn't been loaded.
     */
    getTokenRangesForHost(keyspaceName: string, host: Host): Set<TokenRange> | null;
    /**
     * Constructs a Token from the input buffer(s) or string input.  If a string is passed in
     * it is assumed this matches the token representation reported by cassandra.
     * @param {Array<Buffer>|Buffer|String} components
     * @returns {Token} constructed token from the input buffer.
     */
    newToken(components: Array<Buffer> | Buffer | string): Token;
    /**
     * Constructs a TokenRange from the given start and end tokens.
     * @param {Token} start
     * @param {Token} end
     * @returns {TokenRange} build range spanning from start (exclusive) to end (inclusive).
     */
    newTokenRange(start: Token, end: Token): TokenRange;
    /* Excluded from this release type: getPreparedInfo */
    /**
     * Clears the internal state related to the prepared statements.
     * Following calls to the Client using the prepare flag will re-prepare the statements.
     */
    clearPrepared(): void;
    /* Excluded from this release type: getPreparedById */
    /* Excluded from this release type: setPreparedById */
    /* Excluded from this release type: getAllPrepared */
    /* Excluded from this release type: _uninitializedError */
    /**
     * Gets the definition of an user-defined type.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same UDT definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace.
     * @param {String} name Name of the UDT.
     * @param {Function} [callback] The callback to invoke when retrieval completes.
     */
    getUdt(keyspaceName: string, name: string, callback: ValueCallback<Udt>): void;
    getUdt(keyspaceName: string, name: string): Promise<Udt>;
    /**
     * @param {String} keyspaceName
     * @param {String} name
     * @returns {Promise<Object|null>}
     * @private
     */
    private _getUdt;
    /**
     * Gets the definition of a table.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same table definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace.
     * @param {String} name Name of the Table.
     * @param {Function} [callback] The callback with the err as a first parameter and the {@link TableMetadata} as
     * second parameter.
     */
    getTable(keyspaceName: string, name: string, callback: ValueCallback<TableMetadata>): void;
    getTable(keyspaceName: string, name: string): Promise<TableMetadata>;
    /**
     * @param {String} keyspaceName
     * @param {String} name
     * @private
     */
    private _getTable;
    /**
     * Gets the definition of CQL functions for a given name.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same function definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace.
     * @param {String} name Name of the Function.
     * @param {Function} [callback] The callback with the err as a first parameter and the array of {@link SchemaFunction}
     * as second parameter.
     */
    getFunctions(keyspaceName: string, name: string, callback: ValueCallback<SchemaFunction[]>): void;
    getFunctions(keyspaceName: string, name: string): Promise<SchemaFunction[]>;
    /**
     * @param {String} keyspaceName
     * @param {String} name
     * @private
     */
    private _getFunctionsWrapper;
    /**
     * Gets a definition of CQL function for a given name and signature.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same function definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace
     * @param {String} name Name of the Function
     * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
     * @param {Function} [callback] The callback with the err as a first parameter and the {@link SchemaFunction} as second
     * parameter.
     */
    getFunction(keyspaceName: string, name: string, signature: string[] | Array<DataTypeInfo>, callback: ValueCallback<SchemaFunction>): void;
    getFunction(keyspaceName: string, name: string, signature: string[] | Array<DataTypeInfo>): Promise<SchemaFunction>;
    /**
     * Gets the definition of CQL aggregate for a given name.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same aggregates definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace
     * @param {String} name Name of the Function
     * @param {Function} [callback] The callback with the err as a first parameter and the array of {@link Aggregate} as
     * second parameter.
     */
    getAggregates(keyspaceName: string, name: string, callback: ValueCallback<Aggregate[]>): void;
    getAggregates(keyspaceName: string, name: string): Promise<Aggregate[]>;
    /**
     * @param {String} keyspaceName
     * @param {String} name
     * @private
     */
    private _getAggregates;
    /**
     * Gets a definition of CQL aggregate for a given name and signature.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     * When trying to retrieve the same aggregate definition concurrently, it will query once and invoke all callbacks
     * with the retrieved information.
     * </p>
     * @param {String} keyspaceName Name of the keyspace
     * @param {String} name Name of the aggregate
     * @param {Array.<String>|Array.<{code, info}>} signature Array of types of the parameters.
     * @param {Function} [callback] The callback with the err as a first parameter and the {@link Aggregate} as second parameter.
     */
    getAggregate(keyspaceName: string, name: string, signature: string[] | Array<DataTypeInfo>, callback: ValueCallback<Aggregate>): void;
    getAggregate(keyspaceName: string, name: string, signature: string[] | Array<DataTypeInfo>): Promise<Aggregate>;
    /**
     * Gets the definition of a CQL materialized view for a given name.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * <p>
     *   Note that, unlike the rest of the {@link Metadata} methods, this method does not cache the result for following
     *   calls, as the current version of the Cassandra native protocol does not support schema change events for
     *   materialized views. Each call to this method will produce one or more queries to the cluster.
     * </p>
     * @param {String} keyspaceName Name of the keyspace
     * @param {String} name Name of the materialized view
     * @param {Function} [callback] The callback with the err as a first parameter and the {@link MaterializedView} as
     * second parameter.
     */
    getMaterializedView(keyspaceName: string, name: string, callback: ValueCallback<MaterializedView>): void;
    getMaterializedView(keyspaceName: string, name: string): Promise<MaterializedView>;
    /**
     * @param {String} keyspaceName
     * @param {String} name
     * @returns {Promise<MaterializedView|null>}
     * @private
     */
    private _getMaterializedView;
    /**
     * Gets a map of cql function definitions or aggregates based on signature.
     * @param {String} keyspaceName
     * @param {String} name Name of the function or aggregate
     * @param {Boolean} aggregate
     * @returns {Promise<Map>}
     * @private
     */
    private _getFunctions;
    /**
     * Gets a single cql function or aggregate definition
     * @param {String} keyspaceName
     * @param {String} name
     * @param {Array} signature
     * @param {Boolean} aggregate
     * @returns {Promise<SchemaFunction|Aggregate|null>}
     * @private
     */
    private _getSingleFunction;
    /**
     * Gets the trace session generated by Cassandra when query tracing is enabled for the
     * query. The trace itself is stored in Cassandra in the <code>sessions</code> and
     * <code>events</code> table in the <code>system_traces</code> keyspace and can be
     * retrieve manually using the trace identifier.
     * <p>
     *   If a <code>callback</code> is provided, the callback is invoked when the metadata retrieval completes.
     *   Otherwise, it returns a <code>Promise</code>.
     * </p>
     * @param {Uuid} traceId Identifier of the trace session.
     * @param {Number} [consistency] The consistency level to obtain the trace.
     * @param {Function} [callback] The callback with the err as first parameter and the query trace as second parameter.
     */
    getTrace(traceId: Uuid, consistency: consistencies, callback: ValueCallback<QueryTrace>): void;
    getTrace(traceId: Uuid, consistency: consistencies): Promise<QueryTrace>;
    getTrace(traceId: Uuid, callback: ValueCallback<QueryTrace>): void;
    getTrace(traceId: Uuid): Promise<QueryTrace>;
    /**
     * @param {Uuid} traceId
     * @param {Number} consistency
     * @returns {Promise<Object>}
     * @private
     */
    private _getTrace;
    /* Excluded from this release type: checkSchemaAgreement */
    /**
     * Async-only version of check schema agreement.
     * @private
     */
    private _checkSchemaAgreement;
    /* Excluded from this release type: adaptUserHints */
    /**
     * @param {Array} udts
     * @param {{code, info}} type
     * @param {string} keyspace
     * @private
     */
    private _checkUdtTypes;
    /* Excluded from this release type: compareSchemaVersions */
}

export declare const metadata: {
    Metadata: typeof Metadata;
};

export declare const metrics: {
    ClientMetrics: typeof ClientMetrics;
    DefaultMetrics: typeof DefaultMetrics;
};

/**
 * Represents a query or a set of queries used to perform a mutation in a batch.
 * @alias module:mapping~ModelBatchItem
 */
export declare class ModelBatchItem {
    /* Excluded from this release type: doc */
    /* Excluded from this release type: docInfo */
    /* Excluded from this release type: handler */
    /* Excluded from this release type: cache */
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: getQueries */
    /* Excluded from this release type: getCacheKey */
    /* Excluded from this release type: createQueries */
    /* Excluded from this release type: pushQueries */
    /* Excluded from this release type: getMappingInfo */
}

/**
 * Provides utility methods to group multiple mutations on a single batch.
 * @alias module:mapping~ModelBatchMapper
 */
export declare class ModelBatchMapper {
    private _handler;
    private _cache;
    /* Excluded from this release type: __constructor */
    /**
     * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the INSERT mutation to be
     * used in a batch execution.
     * @param {Object} doc An object containing the properties to insert.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * INSERT cql statements generated. If specified, it must include the columns to insert and the primary keys.
     * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
     * @param {Boolean} [docInfo.ifNotExists] When set, it only inserts if the row does not exist prior to the insertion.
     * <p>Please note that using IF NOT EXISTS will incur a non negligible performance cost so this should be used
     * sparingly.</p>
     * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
     * or a set of queries to be included in a batch.
     */
    insert(doc: object, docInfo: InsertDocInfo): ModelBatchItem;
    /**
     * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem} containing the queries for the UPDATE mutation to be
     * used in a batch execution.
     * @param {Object} doc An object containing the properties to update.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * UPDATE cql statements generated. If specified, it must include the columns to update and the primary keys.
     * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
     * @param {Boolean} [docInfo.ifExists] When set, it only updates if the row already exists on the server.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the UPDATE to occur.
     * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
     * or a set of queries to be included in a batch.
     */
    update(doc: object, docInfo: UpdateDocInfo): ModelBatchItem;
    /**
     * Gets a [ModelBatchItem]{@link module:mapping~ModelBatchItem}  containing the queries for the DELETE mutation to be
     * used in a batch execution.
     * @param {Object} doc A document containing the primary keys values of the document to delete.
     * @param {Object} [docInfo] An object containing the additional doc information.
     * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
     * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
     * When the CQL query is generated, this would be used to generate the `IF` clause.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Boolean} [docInfo.ifExists] When set, it only issues the DELETE command if the row already exists on the
     * server.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
     * @param {Boolean} [docInfo.deleteOnlyColumns] Determines that, when more document properties are specified
     * besides the primary keys, the generated DELETE statement should be used to delete some column values but leave
     * the row. When this is enabled and more properties are specified, a DELETE statement will have the following form:
     * "DELETE col1, col2 FROM table1 WHERE pk1 = ? AND pk2 = ?"
     * @returns {ModelBatchItem} A [ModelBatchItem]{@link module:mapping~ModelBatchItem} instance representing a query
     * or a set of queries to be included in a batch.
     */
    remove(doc: object, docInfo: RemoveDocInfo): ModelBatchItem;
}

declare class ModelColumnInfo {
    columnName: any;
    toModel: any;
    fromModel: any;
    propertyName: any;
    constructor(columnName: any, propertyName: any, toModel?: any, fromModel?: any);
    static parse(columnName: any, value: any): ModelColumnInfo;
}

export declare type ModelColumnOptions = {
    name: string;
    toModel?: (columnValue: any) => any;
    fromModel?: (modelValue: any) => any;
};

/**
 * Represents an object mapper for a specific model.
 * @alias module:mapping~ModelMapper
 */
export declare class ModelMapper<T = any> {
    /**
     * Gets the name identifier of the model.
     * @type {String}
     */
    name: string;
    private _handler;
    /**
     * Gets a [ModelBatchMapper]{@link module:mapping~ModelBatchMapper} instance containing utility methods to group
     * multiple doc mutations in a single batch.
     * @type {ModelBatchMapper}
     */
    batching: ModelBatchMapper;
    /* Excluded from this release type: __constructor */
    /**
     * Gets the first document matching the provided filter or null when not found.
     * <p>
     *   Note that all partition and clustering keys must be defined in order to use this method.
     * </p>
     * @param {Object} doc The object containing the properties that map to the primary keys.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @return {Promise<Object>}
     * @example <caption>Get a video by id</caption>
     * videoMapper.get({ id })
     * @example <caption>Get a video by id, selecting specific columns</caption>
     * videoMapper.get({ id }, fields: ['name', 'description'])
     */
    get(doc: {
        [key: string]: any;
    }, docInfo?: {
        fields?: string[];
    }, executionOptions?: string | MappingExecutionOptions): Promise<null | T>;
    /**
     * Executes a SELECT query based on the filter and returns the result as an iterable of documents.
     * @param {Object} doc An object containing the properties that map to the primary keys to filter.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
     * @param {Object<String, String>} [docInfo.orderBy] An associative array containing the column names as key and
     * the order string (asc or desc) as value used to set the order of the results server-side.
     * @param {Number} [docInfo.limit] Restricts the result of the query to a maximum number of rows on the
     * server.
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Number} [executionOptions.fetchSize] The amount of rows to retrieve per page.
     * @param {Number} [executionOptions.pageState] A Buffer instance or a string token representing the paging state.
     * <p>When provided, the query will be executed starting from a given paging state.</p>
     * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
     * @example <caption>Get user's videos</caption>
     * const result = await videoMapper.find({ userId });
     * for (let video of result) {
     *   console.log(video.name);
     * }
     * @example <caption>Get user's videos from a certain date</caption>
     * videoMapper.find({ userId, addedDate: q.gte(date)});
     * @example <caption>Get user's videos in reverse order</caption>
     * videoMapper.find({ userId }, { orderBy: { addedDate: 'desc' }});
     */
    find(doc: {
        [key: string]: any;
    }, docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;
    /**
     * Executes a SELECT query without a filter and returns the result as an iterable of documents.
     * <p>
     *   This is only recommended to be used for tables with a limited amount of results. Otherwise, breaking up the
     *   token ranges on the client side should be used.
     * </p>
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * SELECT cql statement generated, in order to restrict the amount of columns retrieved.
     * @param {Object<String, String>} [docInfo.orderBy] An associative array containing the column names as key and
     * the order string (asc or desc) as value used to set the order of the results server-side.
     * @param {Number} [docInfo.limit] Restricts the result of the query to a maximum number of rows on the
     * server.
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Number} [executionOptions.fetchSize] The mount of rows to retrieve per page.
     * @param {Number} [executionOptions.pageState] A Buffer instance or a string token representing the paging state.
     * <p>When provided, the query will be executed starting from a given paging state.</p>
     * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
     */
    findAll(docInfo?: FindDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;
    /**
     * Inserts a document.
     * <p>
     *   When the model is mapped to multiple tables, it will insert a row in each table when all the primary keys
     *   are specified.
     * </p>
     * @param {Object} doc An object containing the properties to insert.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * INSERT cql statements generated. If specified, it must include the columns to insert and the primary keys.
     * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
     * @param {Boolean} [docInfo.ifNotExists] When set, it only inserts if the row does not exist prior to the insertion.
     * <p>Please note that using IF NOT EXISTS will incur a non negligible performance cost so this should be used
     * sparingly.</p>
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
     * changing the result beyond the initial application.
     * <p>
     *   By default all generated INSERT statements are considered idempotent, except in the case of lightweight
     *   transactions. Lightweight transactions at client level with transparent retries can
     *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
     * </p>
     * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
     * unix epoch (00:00:00, January 1st, 1970).
     * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
     * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
     * @example <caption>Insert a video</caption>
     * videoMapper.insert({ id, name });
     */
    insert(doc: {
        [key: string]: any;
    }, docInfo?: InsertDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;
    /**
     * Updates a document.
     * <p>
     *   When the model is mapped to multiple tables, it will update a row in each table when all the primary keys
     *   are specified.
     * </p>
     * @param {Object} doc An object containing the properties to update.
     * @param {Object} [docInfo] An object containing the additional document information.
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * UPDATE cql statements generated. If specified, it must include the columns to update and the primary keys.
     * @param {Number} [docInfo.ttl] Specifies an optional Time To Live (in seconds) for the inserted values.
     * @param {Boolean} [docInfo.ifExists] When set, it only updates if the row already exists on the server.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the UPDATE to occur.
     * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
     * changing the result beyond the initial application.
     * <p>
     *   The mapper uses the generated queries to determine the default value. When an UPDATE is generated with a
     *   counter column or appending/prepending to a list column, the execution is marked as not idempotent.
     * </p>
     * <p>
     *   Additionally, the mapper uses the safest approach for queries with lightweight transactions (Compare and
     *   Set) by considering them as non-idempotent. Lightweight transactions at client level with transparent retries can
     *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
     * </p>
     * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
     * unix epoch (00:00:00, January 1st, 1970).
     * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
     * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
     * @example <caption>Update the name of a video</caption>
     * videoMapper.update({ id, name });
     */
    update(doc: {
        [key: string]: any;
    }, docInfo?: UpdateDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;
    /**
     * Deletes a document.
     * @param {Object} doc A document containing the primary keys values of the document to delete.
     * @param {Object} [docInfo] An object containing the additional doc information.
     * @param {Object} [docInfo.when] A document that act as the condition that has to be met for the DELETE to occur.
     * Use this property only in the case you want to specify a conditional clause for lightweight transactions (CAS).
     * When the CQL query is generated, this would be used to generate the `IF` clause.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Boolean} [docInfo.ifExists] When set, it only issues the DELETE command if the row already exists on the
     * server.
     * <p>
     *   Please note that using IF conditions will incur a non negligible performance cost on the server-side so this
     *   should be used sparingly.
     * </p>
     * @param {Array<String>} [docInfo.fields] An Array containing the name of the properties that will be used in the
     * DELETE cql statement generated. If specified, it must include the columns to delete and the primary keys.
     * @param {Boolean} [docInfo.deleteOnlyColumns] Determines that, when more document properties are specified
     * besides the primary keys, the generated DELETE statement should be used to delete some column values but leave
     * the row. When this is enabled and more properties are specified, a DELETE statement will have the following form:
     * "DELETE col1, col2 FROM table1 WHERE pk1 = ? AND pk2 = ?"
     * @param {Object|String} [executionOptions] An object containing the options to be used for the requests
     * execution or a string representing the name of the execution profile.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times without
     * changing the result beyond the initial application.
     * <p>
     *   By default all generated DELETE statements are considered idempotent, except in the case of lightweight
     *   transactions. Lightweight transactions at client level with transparent retries can
     *   break linearizability. If that is not an issue for your application, you can manually set this field to true.
     * </p>
     * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
     * unix epoch (00:00:00, January 1st, 1970).
     * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
     * @return {Promise<Result>} A Promise that resolves to a [Result]{@link module:mapping~Result} instance.
     * @example <caption>Delete a video</caption>
     * videoMapper.remove({ id });
     */
    remove(doc: {
        [key: string]: any;
    }, docInfo?: RemoveDocInfo, executionOptions?: string | MappingExecutionOptions): Promise<Result<T>>;
    /**
     * Uses the provided query and param getter function to execute a query and map the results.
     * Gets a function that takes the document, executes the query and returns the mapped results.
     * @param {String} query The query to execute.
     * @param {Function} paramsHandler The function to execute to extract the parameters of a document.
     * @param {Object|String} [executionOptions] When provided, the options for all executions generated with this
     * method will use the provided options and it will not consider the executionOptions per call.
     * @param {String} [executionOptions.executionProfile] The name of the execution profile.
     * @param {Number} [executionOptions.fetchSize] Amount of rows to retrieve per page.
     * @param {Boolean} [executionOptions.isIdempotent] Defines whether the query can be applied multiple times
     * without changing the result beyond the initial application.
     * @param {Number} [executionOptions.pageState] Buffer or string token representing the paging state.
     * <p>When provided, the query will be executed starting from a given paging state.</p>
     * @param {Number|Long} [executionOptions.timestamp] The default timestamp for the query in microseconds from the
     * unix epoch (00:00:00, January 1st, 1970).
     * <p>When provided, this will replace the client generated and the server side assigned timestamp.</p>
     * @return {Function} Returns a function that takes the document and execution options as parameters and returns a
     * Promise the resolves to a [Result]{@link module:mapping~Result} instance.
     */
    mapWithQuery(query: string, paramsHandler: (doc: any) => any[], executionOptions?: string | MappingExecutionOptions): (doc: any, executionOptions?: string | MappingExecutionOptions) => Promise<Result<T>>;
}

/* Excluded from this release type: ModelMappingInfo */

export declare type ModelOptions = {
    tables?: string[] | ModelTables[];
    mappings?: TableMappings;
    columns?: {
        [key: string]: string | ModelColumnOptions;
    };
    keyspace?: string;
};

export declare interface ModelTables {
    name: string;
    isView: boolean;
}

/**
 * A timestamp generator that guarantees monotonically increasing timestamps and logs warnings when timestamps
 * drift in the future.
 * <p>
 *   {@link Date} has millisecond precision and client timestamps require microsecond precision. This generator
 *   keeps track of the last generated timestamp, and if the current time is within the same millisecond as the last,
 *   it fills the microsecond portion of the new timestamp with the value of an incrementing counter.
 * </p>
 * @extends {TimestampGenerator}
 */
export declare class MonotonicTimestampGenerator extends TimestampGenerator {
    private _warningThreshold;
    private _minLogInterval;
    private _micros;
    private _lastDate;
    private _lastLogDate;
    /**
     * A timestamp generator that guarantees monotonically increasing timestamps and logs warnings when timestamps
     * drift in the future.
     * <p>
     *   {@link Date} has millisecond precision and client timestamps require microsecond precision. This generator
     *   keeps track of the last generated timestamp, and if the current time is within the same millisecond as the last,
     *   it fills the microsecond portion of the new timestamp with the value of an incrementing counter.
     * </p>
     * @param {Number} [warningThreshold] Determines how far in the future timestamps are allowed to drift before a
     * warning is logged, expressed in milliseconds. Default: <code>1000</code>.
     * @param {Number} [minLogInterval] In case of multiple log events, it determines the time separation between log
     * events, expressed in milliseconds. Use 0 to disable. Default: <code>1000</code>.
     * @constructor
     */
    constructor(warningThreshold?: number, minLogInterval?: number);
    /**
     * Returns the current time in milliseconds since UNIX epoch
     * @returns {Number}
     */
    getDate(): number;
    next(client: Client): Long__default | number | null;
    /**
     * @private
     * @returns {Number|Long}
     */
    private _generateMicroseconds;
}

/* Excluded from this release type: Murmur3Token */

/* Excluded from this release type: Murmur3Tokenizer */

/* Excluded from this release type: MutableLong */

/* Excluded from this release type: NoAuthAuthenticator */

/* Excluded from this release type: NoAuthProvider */

/* Excluded from this release type: Node */

/**
 * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
 */
export declare class NoHostAvailableError extends DriverError {
    innerErrors: object;
    /**
     * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
     * @param {Object} innerErrors An object map containing the error per host tried
     * @param {String} [message]
     * @constructor
     */
    constructor(innerErrors: object, message?: string);
}

/**
 * Creates a new instance of NoSpeculativeExecutionPolicy.
 * @classdesc
 * A {@link SpeculativeExecutionPolicy} that never schedules speculative executions.
 * @extends {SpeculativeExecutionPolicy}
 */
export declare class NoSpeculativeExecutionPolicy extends SpeculativeExecutionPolicy {
    private _plan;
    /**
     * Creates a new instance of NoSpeculativeExecutionPolicy.
     */
    constructor();
    newPlan(keyspace: string, queryInfo: string | Array<string>): {
        nextExecution: () => number;
    };
}

/**
 * Represents an error that is raised when a feature is not supported in the driver or in the current Cassandra version.
 */
export declare class NotSupportedError extends DriverError {
    /**
     * Represents an error that is raised when a feature is not supported in the driver or in the current Cassandra version.
     * @param message
     * @constructor
     */
    constructor(message: string);
}

/* Excluded from this release type: opcodes */

/**
 * Information of the execution to be used to determine whether the operation should be retried.
 * @typedef {Object} OperationInfo@typedef {Object} OperationInfo
 * @property {String} query The query that was executed.
 * @param {ExecutionOptions} executionOptions The options related to the execution of the request.
 * @property {Number} nbRetry The number of retries already performed for this operation.
 */
declare type OperationInfo = {
    query: string;
    executionOptions: ExecutionOptions;
    nbRetry: number;
};

/* Excluded from this release type: OperationState */

/**
 * Represents a client-side error that is raised when the client didn't hear back from the server within
 * {@link ClientOptions.socketOptions.readTimeout}.
 */
export declare class OperationTimedOutError extends DriverError {
    host?: string;
    /**
     * Represents a client-side error that is raised when the client didn't hear back from the server within
     * {@link ClientOptions.socketOptions.readTimeout}.
     * @param {String} message The error message.
     * @param {String} [host] Address of the server host that caused the operation to time out.
     * @constructor
     */
    constructor(message: string, host?: string);
}

export declare type Options = {
    collectResults?: boolean;
    concurrencyLevel?: number;
    executionProfile?: string;
    maxErrors?: number;
    raiseOnFirstError?: boolean;
};

declare type OtherCustomColumnInfo = {
    code: (dataTypes.custom);
    info: string;
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

/**
 * Represents a walk through a graph as defined by a traversal.
 * @memberOf module:datastax/graph
 */
declare class Path {
    labels: any[];
    objects: any[];
    /**
     * @param {any[]} labels
     * @param {any[]} objects
     */
    constructor(labels: any[], objects: any[]);
}

/* Excluded from this release type: PlainTextAuthenticator */

/**
 * @classdesc Provides plain text [Authenticator]{@link module:auth~Authenticator} instances to be used when
 * connecting to a host.
 * @extends module:auth~AuthProvider
 * @example
 * var authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
 * //Set the auth provider in the clientOptions when creating the Client instance
 * const client = new Client({ contactPoints: contactPoints, authProvider: authProvider });
 * @alias module:auth~PlainTextAuthProvider
 */
export declare class PlainTextAuthProvider extends AuthProvider {
    private username;
    private password;
    /**
     * Creates a new instance of the Authenticator provider
     * @classdesc Provides plain text [Authenticator]{@link module:auth~Authenticator} instances to be used when
     * connecting to a host.
     * @example
     * var authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
     * //Set the auth provider in the clientOptions when creating the Client instance
     * const client = new Client({ contactPoints: contactPoints, authProvider: authProvider });
     * @param {String} username User name in plain text
     * @param {String} password Password in plain text
     * @alias module:auth~PlainTextAuthProvider
     * @constructor
     */
    constructor(username: string, password: string);
    /**
     * Returns a new [Authenticator]{@link module:auth~Authenticator} instance to be used for plain text authentication.
     * @override
     * @returns {Authenticator}
     */
    newAuthenticator(): Authenticator;
}

/**
 * @classdesc
 * A Point is a zero-dimensional object that represents a specific (X,Y)
 * location in a two-dimensional XY-Plane. In case of Geographic Coordinate
 * Systems, the X coordinate is the longitude and the Y is the latitude.
 * @extends {Geometry}
 * @alias module:geometry~Point
 */
export declare class Point extends Geometry {
    /* Excluded from this release type: x */
    /* Excluded from this release type: y */
    /**
     * Creates a new {@link Point} instance.
     * @param {Number} x The X coordinate.
     * @param {Number} y The Y coordinate.
     */
    constructor(x: number, y: number);
    /**
     * Creates a {@link Point} instance from
     * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
     * representation of a 2D point.
     * @param {Buffer} buffer
     * @returns {Point}
     */
    static fromBuffer(buffer: Buffer): Point;
    /**
     * Creates a {@link Point} instance from
     * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
     * representation of a 2D point.
     * @param {String} textValue
     * @returns {Point}
     */
    static fromString(textValue: string): Point;
    /**
     * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
     * representation of this instance.
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Returns true if the values of the point are the same, otherwise it returns false.
     * @param {Point} other
     * @returns {Boolean}
     */
    equals(other: Point): boolean;
    /**
     * Returns Well-known Text (WKT) representation of the geometry object.
     * @returns {String}
     */
    toString(): string;
    /* Excluded from this release type: useBESerialization */
    /**
     * Returns a JSON representation of this geo-spatial type.
     * @returns {Object}
     */
    toJSON(): object;
}

export declare const policies: {
    addressResolution: {
        AddressTranslator: typeof AddressTranslator;
        EC2MultiRegionTranslator: typeof EC2MultiRegionTranslator;
    };
    loadBalancing: {
        AllowListPolicy: typeof AllowListPolicy;
        DCAwareRoundRobinPolicy: typeof DCAwareRoundRobinPolicy;
        DefaultLoadBalancingPolicy: typeof DefaultLoadBalancingPolicy;
        LoadBalancingPolicy: typeof LoadBalancingPolicy;
        RoundRobinPolicy: typeof RoundRobinPolicy;
        TokenAwarePolicy: typeof TokenAwarePolicy;
        WhiteListPolicy: typeof WhiteListPolicy;
    };
    reconnection: {
        ReconnectionPolicy: typeof ReconnectionPolicy;
        ConstantReconnectionPolicy: typeof ConstantReconnectionPolicy;
        ExponentialReconnectionPolicy: typeof ExponentialReconnectionPolicy;
    };
    retry: {
        IdempotenceAwareRetryPolicy: typeof IdempotenceAwareRetryPolicy;
        FallthroughRetryPolicy: typeof FallthroughRetryPolicy;
        RetryPolicy: typeof RetryPolicy;
    };
    speculativeExecution: {
        NoSpeculativeExecutionPolicy: typeof NoSpeculativeExecutionPolicy;
        SpeculativeExecutionPolicy: typeof SpeculativeExecutionPolicy;
        ConstantSpeculativeExecutionPolicy: typeof ConstantSpeculativeExecutionPolicy;
    };
    timestampGeneration: {
        TimestampGenerator: typeof TimestampGenerator;
        MonotonicTimestampGenerator: typeof MonotonicTimestampGenerator;
    };
    defaultAddressTranslator: () => AddressTranslator;
    defaultLoadBalancingPolicy: (localDc?: string) => LoadBalancingPolicy;
    defaultRetryPolicy: () => RetryPolicy;
    defaultReconnectionPolicy: () => ReconnectionPolicy;
    defaultSpeculativeExecutionPolicy: () => SpeculativeExecutionPolicy;
    defaultTimestampGenerator: () => TimestampGenerator;
};

/**
 * @classdesc
 * Represents is a plane geometry figure that is bounded by a finite chain of straight line segments closing in a loop
 * to form a closed chain or circuit.
 * @example
 * new Polygon([ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ]);
 * @example
 * //polygon with a hole
 * new Polygon(
 *  [ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ],
 *  [ new Point(25, 20), new Point(30, 30), new Point(20, 20), new Point(25, 20) ]
 * );
 * @alias module:geometry~Polygon
 */
export declare class Polygon extends Geometry {
    /* Excluded from this release type: rings */
    /**
     * Creates a new {@link Polygon} instance.
     * @param {...Array.<Point>}[ringPoints] A sequence of Array of [Point]{@link module:geometry~Point} items as arguments
     * representing the rings of the polygon.
     * @example
     * new Polygon([ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ]);
     * @example
     * //polygon with a hole
     * new Polygon(
     *  [ new Point(30, 10), new Point(40, 40), new Point(10, 20), new Point(30, 10) ],
     *  [ new Point(25, 20), new Point(30, 30), new Point(20, 20), new Point(25, 20) ]
     * );
     * @constructor
     */
    constructor(...ringPoints: Point[][]);
    /**
     * Creates a {@link Polygon} instance from
     * a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text (WKT)</a>
     * representation of a polygon.
     * @param {Buffer} buffer
     * @returns {Polygon}
     */
    static fromBuffer(buffer: Buffer): Polygon;
    /**
     * Creates a {@link Polygon} instance from a Well-known Text (WKT) representation.
     * @param {String} textValue
     * @returns {Polygon}
     */
    static fromString(textValue: string): Polygon;
    /**
     * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a> (WKB)
     * representation of this instance.
     * @returns {Buffer}
     */
    toBuffer(): Buffer;
    /**
     * Returns true if the values of the polygons are the same, otherwise it returns false.
     * @param {Polygon} other
     * @returns {Boolean}
     */
    equals(other: Polygon): boolean;
    /* Excluded from this release type: useBESerialization */
    /**
     * Returns Well-known Text (WKT) representation of the geometry object.
     * @returns {String}
     */
    toString(): string;
    /**
     * Returns a JSON representation of this geo-spatial type.
     */
    toJSON(): object;
}

export declare type PreparedQueryInfo = {
    queryId?: Buffer;
    preparing?: boolean;
    query: string;
    keyspace: string;
    meta?: DataCollection;
} & EventEmitter_2;

/* Excluded from this release type: ProfileManager */

/**
 * Represents a property.
 * @memberOf module:datastax/graph
 */
declare class Property {
    key: string;
    value: any;
    /**
     * @param key
     * @param value
     */
    constructor(key: string, value: any);
}

/* Excluded from this release type: protocolEvents */

/**
 * Contains information for the different protocol versions supported by the driver.
 * @type {Object}
 * @property {Number} v1 Cassandra protocol v1, supported in Apache Cassandra 1.2-->2.2.
 * @property {Number} v2 Cassandra protocol v2, supported in Apache Cassandra 2.0-->2.2.
 * @property {Number} v3 Cassandra protocol v3, supported in Apache Cassandra 2.1-->3.x.
 * @property {Number} v4 Cassandra protocol v4, supported in Apache Cassandra 2.2-->3.x.
 * @property {Number} v5 Cassandra protocol v5, in beta from Apache Cassandra 3.x+. Currently not supported by the
 * driver.
 * @property {Number} dseV1 DataStax Enterprise protocol v1, DSE 5.1+
 * @property {Number} dseV2 DataStax Enterprise protocol v2, DSE 6.0+
 * @property {Number} maxSupported Returns the higher protocol version that is supported by this driver.
 * @property {Number} minSupported Returns the lower protocol version that is supported by this driver.
 * @property {Function} isSupported A function that returns a boolean determining whether a given protocol version
 * is supported.
 * @alias module:types~protocolVersion
 */
export declare enum protocolVersion {
    v1 = 1,
    v2 = 2,
    v3 = 3,
    v4 = 4,
    v5 = 5,
    v6 = 6,
    dseV1 = 65,
    dseV2 = 66,
    maxSupported = 66,
    minSupported = 1
}

export declare namespace protocolVersion {
    /* Excluded from this release type: isDse */
    /* Excluded from this release type: isSupportedCassandra */
    /**
     * Determines whether the protocol version is supported by this driver.
     * @param {Number} version
     * @returns {Boolean}
     */
    export function isSupported(version: number): boolean;
    /* Excluded from this release type: supportsPrepareFlags */
    /* Excluded from this release type: supportsKeyspaceInRequest */
    /* Excluded from this release type: supportsResultMetadataId */
    /* Excluded from this release type: supportsPreparedPartitionKey */
    /* Excluded from this release type: supportsSchemaChangeFullMetadata */
    /* Excluded from this release type: supportsContinuousPaging */
    /* Excluded from this release type: supportsPaging */
    /* Excluded from this release type: supportsTimestamp */
    /* Excluded from this release type: supportsNamedParameters */
    /* Excluded from this release type: supportsUnset */
    /* Excluded from this release type: supportsFailureReasonMap */
    /* Excluded from this release type: uses2BytesStreamIds */
    /* Excluded from this release type: uses4BytesCollectionLength */
    /* Excluded from this release type: uses4BytesQueryFlags */
    /* Excluded from this release type: canStartupResponseErrorBeWrapped */
    /* Excluded from this release type: getLowerSupported */
    /* Excluded from this release type: getHighestCommon */
    /* Excluded from this release type: isBeta */
}

/**
 * Contains functions that represents operators in a query.
 * @alias module:mapping~q
 * @type {Object}
 * @property {function} in_ Represents the CQL operator "IN".
 * @property {function} gt Represents the CQL operator greater than ">".
 * @property {function} gte Represents the CQL operator greater than or equals to ">=" .
 * @property {function} lt Represents the CQL operator less than "<" .
 * @property {function} lte Represents the CQL operator less than or equals to "<=" .
 * @property {function} notEq Represents the CQL operator not equals to "!=" .
 * @property {function} and When applied to a property, it represents two CQL conditions on the same column separated
 * by the logical AND operator, e.g: "col1 >= x col < y"
 * @property {function} incr Represents the CQL increment assignment used for counters, e.g: "col = col + x"
 * @property {function} decr Represents the CQL decrement assignment used for counters, e.g: "col = col - x"
 * @property {function} append Represents the CQL append assignment used for collections, e.g: "col = col + x"
 * @property {function} prepend Represents the CQL prepend assignment used for lists, e.g: "col = x + col"
 * @property {function} remove Represents the CQL remove assignment used for collections, e.g: "col = col - x"
 */
export declare const q: {
    in_: (arr: any) => QueryOperator;
    gt: (value: any) => QueryOperator;
    gte: (value: any) => QueryOperator;
    lt: (value: any) => QueryOperator;
    lte: (value: any) => QueryOperator;
    notEq: (value: any) => QueryOperator;
    and: (condition1: any, condition2: any) => QueryOperator;
    incr: (value: any) => QueryAssignment;
    decr: (value: any) => QueryAssignment;
    append: (value: any) => QueryAssignment;
    prepend: (value: any) => QueryAssignment;
    remove: (value: any) => QueryAssignment;
};

/**
 * Represents a CQL assignment operation, like col = col + x.
 * @ignore
 */
declare class QueryAssignment {
    /* Excluded from this release type: sign */
    /* Excluded from this release type: value */
    /* Excluded from this release type: inverted */
    /* Excluded from this release type: __constructor */
}

/**
 * Represents a CQL query operator, like >=, IN, <, ...
 * @ignore
 */
declare class QueryOperator {
    /* Excluded from this release type: key */
    /* Excluded from this release type: value */
    /* Excluded from this release type: hasChildValues */
    /* Excluded from this release type: isInOperator */
    /* Excluded from this release type: __constructor */
}

/**
 * Query options
 * @typedef {Object} QueryOptions@typedef {Object} QueryOptions
 * @property {Boolean} [autoPage] Determines if the driver must retrieve the following result pages automatically.
 * <p>
 *   This setting is only considered by the [Client#eachRow()]{@link Client#eachRow} method. For more information,
 *   check the
 *   [paging results documentation]{@link https://docs.datastax.com/en/developer/nodejs-driver/latest/features/paging/}.
 * </p>
 * @property {Boolean} [captureStackTrace] Determines if the stack trace before the query execution should be
 * maintained.
 * <p>
 *   Useful for debugging purposes, it should be set to <code>false</code> under production environment as it adds an
 *   unnecessary overhead to each execution.
 * </p>
 * Default: false.
 * @property {Number} [consistency] [Consistency level]{@link module:types~consistencies}.
 * <p>
 *   Defaults to <code>localOne</code> for Apache Cassandra and DSE deployments.
 *   For DataStax Astra, it defaults to <code>localQuorum</code>.
 * </p>
 * @property {Object} [customPayload] Key-value payload to be passed to the server. On the Cassandra side,
 * implementations of QueryHandler can use this data.
 * @property {String} [executeAs] The user or role name to act as when executing this statement.
 * <p>When set, it executes as a different user/role than the one currently authenticated (a.k.a. proxy execution).</p>
 * <p>This feature is only available in DSE 5.1+.</p>
 * @property {String|ExecutionProfile} [executionProfile] Name or instance of the [profile]{@link ExecutionProfile} to
 * be used for this execution. If not set, it will the use "default" execution profile.
 * @property {Number} [fetchSize] Amount of rows to retrieve per page.
 * @property {Array|Array<Array>} [hints] Type hints for parameters given in the query, ordered as for the parameters.
 * <p>For batch queries, an array of such arrays, ordered as with the queries in the batch.</p>
 * @property {Host} [host] The host that should handle the query.
 * <p>
 *   Use of this option is <em>heavily discouraged</em> and should only be used in the following cases:
 * </p>
 * <ol>
 *   <li>
 *     Querying node-local tables, such as tables in the <code>system</code> and <code>system_views</code>
 *     keyspaces.
 *   </li>
 *   <li>
 *     Applying a series of schema changes, where it may be advantageous to execute schema changes in sequence on the
 *     same node.
 *   </li>
 * </ol>
 * <p>
 *   Configuring a specific host causes the configured
 *   [LoadBalancingPolicy]{@link module:policies/loadBalancing~LoadBalancingPolicy} to be completely bypassed.
 *   However, if the load balancing policy dictates that the host is at a
 *   [distance of ignored]{@link module:types~distance} or there is no active connectivity to the host, the request will
 *   fail with a [NoHostAvailableError]{@link module:errors~NoHostAvailableError}.
 * </p>
 * @property {Boolean} [isIdempotent] Defines whether the query can be applied multiple times without changing the result
 * beyond the initial application.
 * <p>
 *   The query execution idempotence can be used at [RetryPolicy]{@link module:policies/retry~RetryPolicy} level to
 *   determine if an statement can be retried in case of request error or write timeout.
 * </p>
 * <p>Default: <code>false</code>.</p>
 * @property {String} [keyspace] Specifies the keyspace for the query. It is used for the following:
 * <ol>
 * <li>To indicate what keyspace the statement is applicable to (protocol V5+ only).  This is useful when the
 * query does not provide an explicit keyspace and you want to override the current {@link Client#keyspace}.</li>
 * <li>For query routing when the query operates on a different keyspace than the current {@link Client#keyspace}.</li>
 * </ol>
 * @property {Boolean} [logged] Determines if the batch should be written to the batchlog. Only valid for
 * [Client#batch()]{@link Client#batch}, it will be ignored by other methods. Default: true.
 * @property {Boolean} [counter] Determines if its a counter batch. Only valid for
 * [Client#batch()]{@link Client#batch}, it will be ignored by other methods. Default: false.
 * @property {Buffer|String} [pageState] Buffer or string token representing the paging state.
 * <p>Useful for manual paging, if provided, the query will be executed starting from a given paging state.</p>
 * @property {Boolean} [prepare] Determines if the query must be executed as a prepared statement.
 * @property {Number} [readTimeout] When defined, it overrides the default read timeout
 * (<code>socketOptions.readTimeout</code>) in milliseconds for this execution per coordinator.
 * <p>
 *   Suitable for statements for which the coordinator may allow a longer server-side timeout, for example aggregation
 *   queries.
 * </p>
 * <p>
 *   A value of <code>0</code> disables client side read timeout for the execution. Default: <code>undefined</code>.
 * </p>
 * @property {RetryPolicy} [retry] Retry policy for the query.
 * <p>
 *   This property can be used to specify a different [retry policy]{@link module:policies/retry} to the one specified
 *   in the {@link ClientOptions}.policies.
 * </p>
 * @property {Array} [routingIndexes] Index of the parameters that are part of the partition key to determine
 * the routing.
 * @property {Buffer|Array} [routingKey] Partition key(s) to determine which coordinator should be used for the query.
 * @property {Array} [routingNames] Array of the parameters names that are part of the partition key to determine the
 * routing. Only valid for non-prepared requests, it's recommended that you use the prepare flag instead.
 * @property {Number} [serialConsistency] Serial consistency is the consistency level for the serial phase of
 * conditional updates.
 * This option will be ignored for anything else that a conditional update/insert.
 * @property {Number|Long} [timestamp] The default timestamp for the query in microseconds from the unix epoch
 * (00:00:00, January 1st, 1970).
 * <p>If provided, this will replace the server side assigned timestamp as default timestamp.</p>
 * <p>Use [generateTimestamp()]{@link module:types~generateTimestamp} utility method to generate a valid timestamp
 * based on a Date and microseconds parts.</p>
 * @property {Boolean} [traceQuery] Enable query tracing for the execution. Use query tracing to diagnose performance
 * problems related to query executions. Default: false.
 * <p>To retrieve trace, you can call [Metadata.getTrace()]{@link module:metadata~Metadata#getTrace} method.</p>
 * @property {Object} [graphOptions] Default options for graph query executions.
 * <p>
 *   These options are meant to provide defaults for all graph query executions. Consider using
 *   [execution profiles]{@link ExecutionProfile} if you plan to reuse different set of options across different
 *   query executions.
 * </p>
 * @property {String} [graphOptions.language] The graph language to use in graph queries. Default:
 * <code>'gremlin-groovy'</code>.
 * @property {String} [graphOptions.name] The graph name to be used in all graph queries.
 * <p>
 * This property is required but there is no default value for it. This value can be overridden at query level.
 * </p>
 * @property {Number} [graphOptions.readConsistency] Overrides the
 * [consistency level]{@link module:types~consistencies}
 * defined in the query options for graph read queries.
 * @property {Number} [graphOptions.readTimeout] Overrides the default per-host read timeout (in milliseconds) for all
 * graph queries. Default: <code>0</code>.
 * <p>
 *   Use <code>null</code> to reset the value and use the default on <code>socketOptions.readTimeout</code> .
 * </p>
 * @property {String} [graphOptions.source] The graph traversal source name to use in graph queries. Default:
 * <code>'g'</code>.
 * @property {Number} [graphOptions.writeConsistency] Overrides the [consistency
 * level]{@link module:types~consistencies} defined in the query options for graph write queries.
 Default options for graph query executions.
 * <p>
 *   These options are meant to provide defaults for all graph query executions. Consider using
 *   [execution profiles]{@link ExecutionProfile} if you plan to reuse different set of options across different
 *   query executions.
 * </p>
 * @property {String} [graphOptions.language] The graph language to use in graph queries. Default:
 * <code>'gremlin-groovy'</code>.
 * @property {String} [graphOptions.name] The graph name to be used in all graph queries.
 * <p>
 * This property is required but there is no default value for it. This value can be overridden at query level.
 * </p>
 * @property {Number} [graphOptions.readConsistency] Overrides the
 * [consistency level]{@link module:types~consistencies}
 * defined in the query options for graph read queries.
 * @property {Number} [graphOptions.readTimeout] Overrides the default per-host read timeout (in milliseconds) for all
 * graph queries. Default: <code>0</code>.
 * <p>
 *   Use <code>null</code> to reset the value and use the default on <code>socketOptions.readTimeout</code> .
 * </p>
 * @property {String} [graphOptions.source] The graph traversal source name to use in graph queries. Default:
 * <code>'g'</code>.
 * @property {Number} [graphOptions.writeConsistency] Overrides the [consistency
 * level]{@link module:types~consistencies} defined in the query options for graph write queries.
 */
export declare interface QueryOptions {
    autoPage?: boolean;
    captureStackTrace?: boolean;
    consistency?: consistencies;
    customPayload?: object;
    executeAs?: string;
    executionProfile?: string | ExecutionProfile;
    fetchSize?: number;
    hints?: Array<string> | Array<Array<string>>;
    host?: Host;
    isIdempotent?: boolean;
    keyspace?: string;
    logged?: boolean;
    counter?: boolean;
    pageState?: Buffer | string;
    prepare?: boolean;
    readTimeout?: number;
    retry?: RetryPolicy;
    routingIndexes?: number[];
    routingKey?: Buffer | Buffer[];
    routingNames?: string[];
    serialConsistency?: number;
    timestamp?: number | Long__default;
    traceQuery?: boolean;
    graphOptions?: {
        language?: string;
        name?: string;
        readConsistency?: number;
        readTimeout?: number;
        source?: string;
        writeConsistency?: number;
    };
}

export declare interface QueryTrace {
    requestType: string;
    coordinator: InetAddress;
    parameters: {
        [key: string]: any;
    };
    startedAt: number | Long__default;
    duration: number;
    clientAddress: string;
    events: Array<{
        id: Uuid;
        activity: any;
        source: any;
        elapsed: any;
        thread: any;
    }>;
}

/* Excluded from this release type: RandomToken */

/* Excluded from this release type: RandomTokenizer */

export declare const reconnection: {
    ReconnectionPolicy: typeof ReconnectionPolicy;
    ConstantReconnectionPolicy: typeof ConstantReconnectionPolicy;
    ExponentialReconnectionPolicy: typeof ExponentialReconnectionPolicy;
};

/** @module policies/reconnection */
/**
 * Base class for Reconnection Policies
 */
export declare class ReconnectionPolicy {
    constructor();
    /**
     * A new reconnection schedule.
     * @returns {Iterator<number>} An infinite iterator
     */
    newSchedule(): Iterator<number>;
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

export declare type RemoveDocInfo = {
    fields?: string[];
    ttl?: number;
    ifExists?: boolean;
    when?: {
        [key: string]: any;
    };
    deleteOnlyColumns?: boolean;
};

/**
 * Abstract class Request
 */
declare class Request_2 {
    length: number;
    constructor();
    /**
     * @abstract
     * @param {Encoder} encoder
     * @param {Number} streamId
     * @throws {TypeError}
     * @returns {Buffer}
     */
    write(encoder: Encoder, streamId: number): Buffer;
    /**
     * Creates a new instance using the same constructor as the current instance, copying the properties.
     * @return {Request}
     */
    clone(): Request_2;
}

/**
 * A request tracker that logs the requests executed through the session, according to a set of
 * configurable options.
 * @implements {module:tracker~RequestTracker}
 * @alias module:tracker~RequestLogger
 * @example <caption>Logging slow queries</caption>
 * const requestLogger = new RequestLogger({ slowThreshold: 1000 });
 * requestLogger.emitter.on('show', message => console.log(message));
 * // Add the requestLogger to the client options
 * const client = new Client({ contactPoints, requestTracker: requestLogger });
 */
export declare class RequestLogger extends RequestTracker {
    private _options;
    /**
     * Determines whether it should emit 'normal' events for every EXECUTE, QUERY and BATCH request executed
     * successfully, useful only for debugging
     * @type {Boolean}
     */
    private logNormalRequests;
    /**
     * Determines whether it should emit 'failure' events for every EXECUTE, QUERY and BATCH request execution that
     * resulted in an error
     * @type {Boolean}
     */
    private logErroredRequests;
    /**
     * The object instance that emits <code>'slow'</code>, <code>'large'</code>, <code>'normal'</code> and
     * <code>'failure'</code> events.
     * @type {EventEmitter}
     */
    private emitter;
    /**
     * Creates a new instance of {@link RequestLogger}.
     * @param {Object} options
     * @param {Number} [options.slowThreshold] The threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     * @param {Number} [options.requestSizeThreshold] The threshold in bytes beyond which requests are considered 'large'
     * and logged as such by the driver.
     * @param {Boolean} [options.logNormalRequests] Determines whether it should emit 'normal' events for every
     * EXECUTE, QUERY and BATCH request executed successfully, useful only for debugging. This option can be modified
     * after the client is connected using the property {@link RequestLogger#logNormalRequests}.
     * @param {Boolean} [options.logErroredRequests] Determines whether it should emit 'failure' events for every
     * EXECUTE, QUERY and BATCH request execution that resulted in an error. This option can be modified
     * after the client is connected using the property {@link RequestLogger#logErroredRequests}.
     * @param {Number} [options.messageMaxQueryLength] The maximum amount of characters that are logged from the query
     * portion of the message. Defaults to 500.
     * @param {Number} [options.messageMaxParameterValueLength] The maximum amount of characters of each query parameter
     * value that will be included in the message. Defaults to 50.
     * @param {Number} [options.messageMaxErrorStackTraceLength] The maximum amount of characters of the stack trace
     * that will be included in the message. Defaults to 200.
     */
    constructor(options: {
        slowThreshold?: number;
        requestSizeThreshold?: number;
        logNormalRequests?: boolean;
        logErroredRequests?: boolean;
        messageMaxQueryLength?: number;
        messageMaxParameterValueLength?: number;
        messageMaxErrorStackTraceLength?: number;
    });
    /**
     * Logs message if request execution was deemed too slow, large or if normal requests are logged.
     * @override
     */
    onSuccess(host: Host, query: string | Array<{
        query: string;
        params?: any;
    }>, parameters: any[] | {
        [p: string]: any;
    } | null, executionOptions: ExecutionOptions, requestLength: number, responseLength: number, latency: number[]): void;
    /**
     * Logs message if request execution was too large and/or encountered an error.
     * @override
     */
    onError(host: Host, query: string | Array<{
        query: string;
        params?: any;
    }>, parameters: any[] | {
        [p: string]: any;
    } | null, executionOptions: ExecutionOptions, requestLength: number, err: Error, latency: number[]): void;
    private _logSlow;
    private _logLargeRequest;
    private _logNormalRequest;
    private _logLargeErrorRequest;
    private _logErrorRequest;
}

/**
 * Tracks request execution for a {@link Client}.
 * <p>
 *   A {@link RequestTracker} can be configured in the client options. The <code>Client</code> will execute
 *   {@link RequestTracker#onSuccess} or {@link RequestTracker#onError} for every query or batch
 *   executed (QUERY, EXECUTE and BATCH requests).
 * </p>
 * @interface
 * @alias module:tracker~RequestTracker
 */
export declare class RequestTracker {
    /**
     * Invoked each time a query or batch request succeeds.
     * @param {Host} host The node that acted as coordinator of the request.
     * @param {String|Array} query In the case of prepared or unprepared query executions, the provided
     * query string. For batch requests, an Array containing the queries and parameters provided.
     * @param {Array|Object|null} parameters In the case of prepared or unprepared query executions, the provided
     * parameters.
     * @param {ExecutionOptions} executionOptions The information related to the execution of the request.
     * @param {Number} requestLength Length of the body of the request.
     * @param {Number} responseLength Length of the body of the response.
     * @param {Array<Number>} latency An array containing [seconds, nanoseconds] tuple, where nanoseconds is the
     * remaining part of the real time that can't be represented in second precision (see <code>process.hrtime()</code>).
     */
    onSuccess?(host: Host, query: string | Array<{
        query: string;
        params?: any;
    }>, parameters: any[] | {
        [key: string]: any;
    } | null, executionOptions: ExecutionOptions, requestLength: number, responseLength: number, latency: number[]): void;
    /**
     * Invoked each time a query or batch request fails.
     * @param {Host} host The node that acted as coordinator of the request.
     * @param {String|Array} query In the case of prepared or unprepared query executions, the provided
     * query string. For batch requests, an Array containing the queries and parameters provided.
     * @param {Array|Object|null} parameters In the case of prepared or unprepared query executions, the provided
     * parameters.
     * @param {ExecutionOptions} executionOptions The information related to the execution of the request.
     * @param {Number} requestLength Length of the body of the request. When the failure occurred before the request was
     * written to the wire, the length will be <code>0</code>.
     * @param {Error} err The error that caused that caused the request to fail.
     * @param {Array<Number>} latency An array containing [seconds, nanoseconds] tuple, where nanoseconds is the
     * remaining part of the real time that can't be represented in second precision (see <code>process.hrtime()</code>).
     */
    onError?(host: Host, query: string | Array<{
        query: string;
        params?: any;
    }>, parameters: any[] | {
        [key: string]: any;
    } | null, executionOptions: ExecutionOptions, requestLength: number, err: Error, latency: number[]): void;
    /**
     * Invoked when the Client is being shutdown.
     */
    shutdown?(): void;
}

/**
 * Represents an error message from the server
 */
export declare class ResponseError extends DriverError {
    code: number;
    consistencies?: consistencies;
    required?: number;
    alive?: number;
    received?: number;
    blockFor?: number;
    failures?: number;
    reasons?: object;
    isDataPresent?: any;
    writeType?: any;
    queryId?: any;
    keyspace?: string;
    functionName?: string;
    argTypes?: string[];
    table?: string;
    /**
     * Represents an error message from the server
     * @param {Number} code Cassandra exception code
     * @param {String} message
     * @constructor
     */
    constructor(code: number, message: string);
}

/**
 * Server error codes returned by Cassandra
 * @type {Object}
 * @property {Number} serverError Something unexpected happened.
 * @property {Number} protocolError Some client message triggered a protocol violation.
 * @property {Number} badCredentials Authentication was required and failed.
 * @property {Number} unavailableException Raised when coordinator knows there is not enough replicas alive to perform a query with the requested consistency level.
 * @property {Number} overloaded The request cannot be processed because the coordinator is overloaded.
 * @property {Number} isBootstrapping The request was a read request but the coordinator node is bootstrapping.
 * @property {Number} truncateError Error encountered during a truncate request.
 * @property {Number} writeTimeout Timeout encountered on write query on coordinator waiting for response(s) from replicas.
 * @property {Number} readTimeout Timeout encountered on read query on coordinator waitign for response(s) from replicas.
 * @property {Number} readFailure A non-timeout error encountered during a read request.
 * @property {Number} functionFailure A (user defined) function encountered during execution.
 * @property {Number} writeFailure A non-timeout error encountered during a write request.
 * @property {Number} syntaxError The submitted query has a syntax error.
 * @property {Number} unauthorized The logged user doesn't have the right to perform the query.
 * @property {Number} invalid The query is syntactically correct but invalid.
 * @property {Number} configError The query is invalid because of some configuration issue.
 * @property {Number} alreadyExists The query attempted to create a schema element (i.e. keyspace, table) that already exists.
 * @property {Number} unprepared Can be thrown while a prepared statement tries to be executed if the provided statement is not known by the coordinator.
 */
export declare enum responseErrorCodes {
    serverError = 0,
    protocolError = 10,
    badCredentials = 256,
    unavailableException = 4096,
    overloaded = 4097,
    isBootstrapping = 4098,
    truncateError = 4099,
    writeTimeout = 4352,
    readTimeout = 4608,
    readFailure = 4864,
    functionFailure = 5120,
    writeFailure = 5376,
    syntaxError = 8192,
    unauthorized = 8448,
    invalid = 8704,
    configError = 8960,
    alreadyExists = 9216,
    unprepared = 9472,
    clientWriteFailure = 32768
}

/**
 * Represents the result of an execution as an iterable of objects in the Mapper.
 * @alias module:mapping~Result
 */
export declare class Result<T = any> implements IterableIterator<T> {
    private _rs;
    private _info;
    private _rowAdapter;
    private _isEmptyLwt;
    private _iteratorIndex;
    /* Excluded from this release type: length */
    /* Excluded from this release type: pageState */
    /* Excluded from this release type: __constructor */
    /**
     * When this instance is the result of a conditional update query, it returns whether it was successful.
     * Otherwise, it returns <code>true</code>.
     * <p>
     *   For consistency, this method always returns <code>true</code> for non-conditional queries (although there is
     *   no reason to call the method in that case). This is also the case for conditional DDL statements
     *   (CREATE KEYSPACE... IF NOT EXISTS, CREATE TABLE... IF NOT EXISTS), for which the server doesn't return
     *   information whether it was applied or not.
     * </p>
     */
    wasApplied(): boolean;
    /**
     * Gets the first document in this result or null when the result is empty.
     */
    first(): T | null;
    /**
     * Returns a new Iterator object that contains the document values.
     */
    [Symbol.iterator](): IterableIterator<T>;
    /**
     * Converts the current instance to an Array of documents.
     * @return {Array<T>}
     */
    toArray(): T[];
    /**
     * Executes a provided function once per result element.
     * @param {Function} callback Function to execute for each element, taking two arguments: currentValue and index.
     * @param {Object} [thisArg] Value to use as <code>this</code> when executing callback.
     */
    forEach(callback: (currentValue: T, index: number) => void, thisArg: any): void;
    [inspectMethod](): T[];
    next(): {
        done: boolean;
        value: T;
    };
}

/* Excluded from this release type: resultKind */

/** @module types */
/**
 * @class
 * @classdesc Represents the result of a query.
 */
export declare class ResultSet implements Iterable<Row>, AsyncIterable<Row> {
    info: {
        queriedHost: string;
        triedHosts: {
            [key: string]: any;
        };
        speculativeExecutions: number;
        achievedConsistency: consistencies;
        traceId: Uuid;
        warnings: string[];
        customPayload: any;
        isSchemaInAgreement: boolean;
    };
    columns: Array<{
        name: string;
        type: DataTypeInfo;
    }>;
    nextPage: (() => void) | null;
    pageState: string;
    rowLength: number;
    rows: Row[];
    /* Excluded from this release type: nextPageAsync */
    /* Excluded from this release type: rawPageState */
    /* Excluded from this release type: __constructor */
    /**
     * Returns the first row or null if the result rows are empty.
     */
    first(): Row;
    getPageState(): string;
    getColumns(): Array<{
        name: string;
        type: DataTypeInfo;
    }>;
    /**
     * When this instance is the result of a conditional update query, it returns whether it was successful.
     * Otherwise, it returns <code>true</code>.
     * <p>
     *   For consistency, this method always returns <code>true</code> for non-conditional queries (although there is
     *   no reason to call the method in that case). This is also the case for conditional DDL statements
     *   (CREATE KEYSPACE... IF NOT EXISTS, CREATE TABLE... IF NOT EXISTS), for which the server doesn't return
     *   information whether it was applied or not.
     * </p>
     */
    wasApplied(): boolean;
    /**
     * Gets the iterator function.
     * <p>
     *   Retrieves the iterator of the underlying fetched rows, without causing the driver to fetch the following
     *   result pages. For more information on result paging,
     *   [visit the documentation]{@link http://docs.datastax.com/en/developer/nodejs-driver/latest/features/paging/}.
     * </p>
     * @alias module:types~ResultSet#@@iterator
     * @see {@link module:types~ResultSet#@@asyncIterator}
     * @example <caption>Using for...of statement</caption>
     * const query = 'SELECT user_id, post_id, content FROM timeline WHERE user_id = ?';
     * const result = await client.execute(query, [ id ], { prepare: true });
     * for (const row of result) {
     *   console.log(row['email']);
     * }
     * @returns {Iterator.<Row>}
     */
    [Symbol.iterator](): Iterator<Row>;
    /**
     * Gets the async iterator function.
     * <p>
     *   Retrieves the async iterator representing the entire query result, the driver will fetch the following result
     *   pages.
     * </p>
     * <p>Use the async iterator when the query result might contain more rows than the <code>fetchSize</code>.</p>
     * <p>
     *   Note that using the async iterator will not affect the internal state of the <code>ResultSet</code> instance.
     *   You should avoid using both <code>rows</code> property that contains the row instances of the first page of
     *   results, and the async iterator, that will yield all the rows in the result regardless on the number of pages.
     * </p>
     * <p>Multiple concurrent async iterations are not supported.</p>
     * @alias module:types~ResultSet#@@asyncIterator
     * @example <caption>Using for await...of statement</caption>
     * const query = 'SELECT user_id, post_id, content FROM timeline WHERE user_id = ?';
     * const result = await client.execute(query, [ id ], { prepare: true });
     * for await (const row of result) {
     *   console.log(row['email']);
     * }
     * @returns {AsyncIterator<Row>}
     */
    [Symbol.asyncIterator](): AsyncIterator<Row>;
    /**
     * Determines whether there are more pages of results.
     * If so, the driver will initially retrieve and contain only the first page of results.
     * To obtain all the rows, use the [AsyncIterator]{@linkcode module:types~ResultSet#@@asyncIterator}.
     * @returns {boolean}
     */
    isPaged(): boolean;
}

/**
 * Represents results from different related executions.
 */
export declare class ResultSetGroup {
    private _collectResults;
    private _maxErrors;
    totalExecuted: number;
    errors: Error[];
    resultItems: any[];
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: setResultItem */
    /* Excluded from this release type: setError */
}

/** @module types */
/**
 * Readable stream using to yield data from a result or a field
 */
export declare class ResultStream extends Readable {
    buffer: any[];
    paused: boolean;
    private _cancelAllowed;
    private _handlersObject;
    private _highWaterMarkRows;
    private _readableState;
    private _readNext;
    /* Excluded from this release type: __constructor */
    /* Excluded from this release type: _read */
    /* Excluded from this release type: _valve */
    add(chunk: any): number;
    private _checkAboveHighWaterMark;
    private _checkBelowHighWaterMark;
    /* Excluded from this release type: cancel */
    /* Excluded from this release type: setHandlers */
}

export declare const retry: {
    IdempotenceAwareRetryPolicy: typeof IdempotenceAwareRetryPolicy;
    FallthroughRetryPolicy: typeof FallthroughRetryPolicy;
    RetryPolicy: typeof RetryPolicy;
};

/**
 * Base and default RetryPolicy.
 * Determines what to do when the driver encounters specific Cassandra exceptions.
 */
export declare class RetryPolicy {
    /**
     * Determines what to do when the driver gets an UnavailableException response from a Cassandra node.
     * @param {OperationInfo} info
     * @param {consistencies} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
     * the exception.
     * @param {Number} required The number of replicas whose response is required to achieve the
     * required [consistency]{@link module:types~consistencies}.
     * @param {Number} alive The number of replicas that were known to be alive when the request had been processed
     * (since an unavailable exception has been triggered, there will be alive &lt; required)
     * @returns {DecisionInfo}
     */
    onUnavailable(info: OperationInfo, consistency: consistencies, required: number, alive: number): DecisionInfo;
    /**
     * Determines what to do when the driver gets a ReadTimeoutException response from a Cassandra node.
     * @param {OperationInfo} info
     * @param {consistencies} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
     * the exception.
     * @param {Number} received The number of nodes having answered the request.
     * @param {Number} blockFor The number of replicas whose response is required to achieve the
     * required [consistency]{@link module:types~consistencies}.
     * @param {Boolean} isDataPresent When <code>false</code>, it means the replica that was asked for data has not responded.
     * @returns {DecisionInfo}
     */
    onReadTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, isDataPresent: boolean): DecisionInfo;
    /**
     * Determines what to do when the driver gets a WriteTimeoutException response from a Cassandra node.
     * @param {OperationInfo} info
     * @param {consistencies} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
     * the exception.
     * @param {Number} received The number of nodes having acknowledged the request.
     * @param {Number} blockFor The number of replicas whose acknowledgement is required to achieve the required
     * [consistency]{@link module:types~consistencies}.
     * @param {String} writeType A <code>string</code> that describes the type of the write that timed out ("SIMPLE"
     * / "BATCH" / "BATCH_LOG" / "UNLOGGED_BATCH" / "COUNTER").
     * @returns {DecisionInfo}
     */
    onWriteTimeout(info: OperationInfo, consistency: consistencies, received: number, blockFor: number, writeType: string): DecisionInfo;
    /**
     * Defines whether to retry and at which consistency level on an unexpected error.
     * <p>
     * This method might be invoked in the following situations:
     * </p>
     * <ol>
     * <li>On a client timeout, while waiting for the server response
     * (see [socketOptions.readTimeout]{@link ClientOptions}), being the error an instance of
     * [OperationTimedOutError]{@link module:errors~OperationTimedOutError}.</li>
     * <li>On a connection error (socket closed, etc.).</li>
     * <li>When the contacted host replies with an error, such as <code>overloaded</code>, <code>isBootstrapping</code>,
     * </code>serverError, etc. In this case, the error is instance of [ResponseError]{@link module:errors~ResponseError}.
     * </li>
     * </ol>
     * <p>
     * Note that when this method is invoked, <em>the driver cannot guarantee that the mutation has been effectively
     * applied server-side</em>; a retry should only be attempted if the request is known to be idempotent.
     * </p>
     * @param {OperationInfo} info
     * @param {consistencies} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
     * the exception.
     * @param {Error} err The error that caused this request to fail.
     * @returns {DecisionInfo}
     */
    onRequestError(info: OperationInfo, consistency: consistencies, err: Error): DecisionInfo;
    /**
     * Returns a {@link DecisionInfo} to retry the request with the given [consistency]{@link module:types~consistencies}.
     * @param {consistencies} [consistency] When specified, it retries the request with the given consistency.
     * @param {Boolean} [useCurrentHost] When specified, determines if the retry should be made using the same coordinator.
     * Default: true.
     * @returns {DecisionInfo}
     */
    retryResult(consistency?: consistencies, useCurrentHost?: boolean): DecisionInfo;
    /**
     * Returns a {@link DecisionInfo} to callback in error when a err is obtained for a given request.
     * @returns {DecisionInfo}
     */
    rethrowResult(): DecisionInfo;
}

/**
 *     namespace RetryDecision {
 enum retryDecision {
 ignore,
 rethrow,
 retry
 }
 }
 */
/**
 * Determines the retry decision for the retry policies.
 * @type {Object}
 * @property {Number} rethrow
 * @property {Number} retry
 * @property {Number} ignore
 * @static
 */
export declare namespace RetryPolicy {
    export enum retryDecision {
        rethrow = 0,
        retry = 1,
        ignore = 2
    }
}

/**
 * This policy yield nodes in a round-robin fashion.
 */
export declare class RoundRobinPolicy extends LoadBalancingPolicy {
    private index;
    constructor();
    /**
     * Returns an iterator with the hosts to be used as coordinator for a query.
     * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
     * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
     * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
     * second parameter.
     */
    newQueryPlan(keyspace: string, executionOptions: ExecutionOptions, callback: (error: Error, iterator: Iterator<Host>) => void): void;
}

/** @module types */
/**
 * Represents a result row
 * @param {Array} columns
 * @constructor
 */
export declare class Row {
    private readonly __columns;
    [key: string]: any;
    /* Excluded from this release type: __constructor */
    /**
     * Returns the cell value.
     * @param {String|Number} columnName Name or index of the column
     */
    get(columnName: string | number): any;
    /**
     * Returns an array of the values of the row
     * @returns {Array}
     */
    values(): Array<any>;
    /**
     * Returns an array of the column names of the row
     * @returns {Array}
     */
    keys(): string[];
    /**
     * Executes the callback for each field in the row, containing the value as first parameter followed by the columnName
     * @param {Function} callback
     */
    forEach(callback: (val: any, key: string) => void): void;
}

/**
 * @classdesc Describes a CQL function.
 * @alias module:metadata~SchemaFunction
 */
export declare class SchemaFunction {
    /**
     * Name of the cql function.
     * @type {String}
     */
    name: string;
    /**
     * Name of the keyspace where the cql function is declared.
     */
    keyspaceName: string;
    /**
     * Signature of the function.
     * @type {Array.<String>}
     */
    signature: Array<string>;
    /**
     * List of the function argument names.
     * @type {Array.<String>}
     */
    argumentNames: Array<string>;
    /**
     * List of the function argument types.
     * @type {Array.<{code, info}>}
     */
    argumentTypes: Array<DataTypeInfo>;
    /**
     * Body of the function.
     * @type {String}
     */
    body: string;
    /**
     * Determines if the function is called when the input is null.
     * @type {Boolean}
     */
    calledOnNullInput: boolean;
    /**
     * Name of the programming language, for example: java, javascript, ...
     * @type {String}
     */
    language: string;
    /**
     * Type of the return value.
     * @type {DataTypeInfo}
     */
    returnType: DataTypeInfo;
    /**
     * Indicates whether or not this function is deterministic.  This means that
     * given a particular input, the function will always produce the same output.
     * @type {Boolean}
     */
    deterministic: boolean;
    /**
     * Indicates whether or not this function is monotonic on all of its
     * arguments.  This means that it is either entirely non-increasing or
     * non-decreasing.  Even if the function is not monotonic on all of its
     * arguments, it's possible to specify that it is monotonic on one of
     * its arguments, meaning that partial applications of the function over
     * that argument will be monotonic.
     *
     * Monotonicity is required to use the function in a GROUP BY clause.
     * @type {Boolean}
     */
    monotonic: boolean;
    /**
     * The argument names that the function is monotonic on.
     *
     * If {@link monotonic} is true, this will return all argument names.
     * Otherwise, this will return either one argument or an empty array.
     * @type {Array.<String>}
     */
    monotonicOn: Array<string>;
    /* Excluded from this release type: __constructor */
}

/**
 * Search module.
 * <p>
 *   Contains the classes to represent the set of  types for search data that come with DSE 5.1+
 * </p>
 * @module datastax/search
 */
export declare const search: {
    DateRange: typeof DateRange;
    DateRangeBound: typeof DateRangeBound;
    dateRangePrecision: {
        readonly year: 0;
        readonly month: 1;
        readonly day: 2;
        readonly hour: 3;
        readonly minute: 4;
        readonly second: 5;
        readonly millisecond: 6;
    };
};

declare type SingleColumnInfo = {
    code: SingleTypeCodes;
    info?: null;
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

declare type SingleTypeCodes = (typeof singleTypeNames[keyof typeof singleTypeNames] | dataTypes.duration | dataTypes.text);

declare const singleTypeNames: Readonly<{
    readonly 'org.apache.cassandra.db.marshal.UTF8Type': dataTypes.varchar;
    readonly 'org.apache.cassandra.db.marshal.AsciiType': dataTypes.ascii;
    readonly 'org.apache.cassandra.db.marshal.UUIDType': dataTypes.uuid;
    readonly 'org.apache.cassandra.db.marshal.TimeUUIDType': dataTypes.timeuuid;
    readonly 'org.apache.cassandra.db.marshal.Int32Type': dataTypes.int;
    readonly 'org.apache.cassandra.db.marshal.BytesType': dataTypes.blob;
    readonly 'org.apache.cassandra.db.marshal.FloatType': dataTypes.float;
    readonly 'org.apache.cassandra.db.marshal.DoubleType': dataTypes.double;
    readonly 'org.apache.cassandra.db.marshal.BooleanType': dataTypes.boolean;
    readonly 'org.apache.cassandra.db.marshal.InetAddressType': dataTypes.inet;
    readonly 'org.apache.cassandra.db.marshal.SimpleDateType': dataTypes.date;
    readonly 'org.apache.cassandra.db.marshal.TimeType': dataTypes.time;
    readonly 'org.apache.cassandra.db.marshal.ShortType': dataTypes.smallint;
    readonly 'org.apache.cassandra.db.marshal.ByteType': dataTypes.tinyint;
    readonly 'org.apache.cassandra.db.marshal.DateType': dataTypes.timestamp;
    readonly 'org.apache.cassandra.db.marshal.TimestampType': dataTypes.timestamp;
    readonly 'org.apache.cassandra.db.marshal.LongType': dataTypes.bigint;
    readonly 'org.apache.cassandra.db.marshal.DecimalType': dataTypes.decimal;
    readonly 'org.apache.cassandra.db.marshal.IntegerType': dataTypes.varint;
    readonly 'org.apache.cassandra.db.marshal.CounterColumnType': dataTypes.counter;
}>;

export declare const speculativeExecution: {
    NoSpeculativeExecutionPolicy: typeof NoSpeculativeExecutionPolicy;
    SpeculativeExecutionPolicy: typeof SpeculativeExecutionPolicy;
    ConstantSpeculativeExecutionPolicy: typeof ConstantSpeculativeExecutionPolicy;
};

/** @module policies/speculativeExecution */
/**
 * @classdesc
 * The policy that decides if the driver will send speculative queries to the next hosts when the current host takes too
 * long to respond.
 * <p>Note that only idempotent statements will be speculatively retried.</p>
 * @abstract
 */
export declare class SpeculativeExecutionPolicy {
    constructor();
    /**
     * Initialization method that gets invoked on Client startup.
     * @param {Client} client
     * @abstract
     */
    init(client: Client): void;
    /**
     * Gets invoked at client shutdown, giving the opportunity to the implementor to perform cleanup.
     * @abstract
     */
    shutdown(): void;
    /**
     * Gets the plan to use for a new query.
     * Returns an object with a <code>nextExecution()</code> method, which returns a positive number representing the
     * amount of milliseconds to delay the next execution or a non-negative number to avoid further executions.
     * @param {String} keyspace The currently logged keyspace.
     * @param {String|Array<String>} queryInfo The query, or queries in the case of batches, for which to build a plan.
     * @return {{nextExecution: function}}
     * @abstract
     */
    newPlan(keyspace: string, queryInfo: string | Array<string>): {
        nextExecution: () => number;
    };
    /**
     * Gets an associative array containing the policy options.
     */
    getOptions(): Map<string, any>;
}

/**
 * Represents a queue of ids from 0 to maximum stream id supported by the protocol version.
 * Clients can dequeue a stream id using {@link StreamIdStack#shift()} and enqueue (release) using
 * {@link StreamIdStack#push()}
 */
declare class StreamIdStack {
    currentGroup: any[];
    groupIndex: number;
    groups: any[];
    releaseTimeout: NodeJS.Timeout;
    inUse: number;
    releaseDelay: number;
    maxGroups: number;
    /**
     * Creates a new instance of StreamIdStack.
     * @param {number} version Protocol version
     * @constructor
     */
    constructor(version: number);
    /**
     * Sets the protocol version
     * @param {Number} version
     */
    setVersion(version: number): void;
    /**
     * Dequeues an id.
     * Similar to {@link Array#pop()}.
     * @returns {Number} Returns an id or null
     */
    pop(): number;
    /**
     * Enqueue an id for future use.
     * Similar to {@link Array#push()}.
     * @param {Number} id
     */
    push(id: number): void;
    /**
     * Clears all timers
     */
    clear(): void;
    /**
     * Tries to create an additional group and returns a new id
     * @returns {Number} Returns a new id or null if it's not possible to create a new group
     * @private
     */
    _tryCreateGroup(): number;
    _tryIssueRelease(): void;
    _releaseGroups(): void;
}

/**
 * Contains a set of methods to represent a row into a document and a document into a row.
 * @alias module:mapping~TableMappings
 * @interface
 */
export declare class TableMappings {
    /**
     * Method that is called by the mapper to create the instance of the document.
     * @return {Object}
     */
    newObjectInstance(): object;
    /**
     * Gets the name of the column based on the document property name.
     * @param {String} propName The name of the property.
     * @returns {String}
     */
    getColumnName(propName: string): string;
    /**
     * Gets the name of the document property based on the column name.
     * @param {String} columnName The name of the column.
     * @returns {String}
     */
    getPropertyName(columnName: string): string;
}

/**
 * @classdesc Describes a table
 * @augments {module:metadata~DataCollection}
 * @alias module:metadata~TableMetadata
 */
export declare class TableMetadata extends DataCollection {
    /**
     * Applies only to counter tables.
     * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
     * the client for a write request. For counter tables, this should always be set to true.
     * @type {Boolean}
     */
    replicateOnWrite: boolean;
    /**
     * Returns the memtable flush period (in milliseconds) option for this table.
     * @type {Number}
     */
    memtableFlushPeriod: number;
    /**
     * Returns the index interval option for this table.
     * <p>
     * Note: this option is only available in Apache Cassandra 2.0. It is deprecated in Apache Cassandra 2.1 and
     * above, and will therefore return <code>null</code> for 2.1 nodes.
     * </p>
     * @type {Number|null}
     */
    indexInterval?: number;
    /**
     * Determines  whether the table uses the COMPACT STORAGE option.
     * @type {Boolean}
     */
    isCompact: boolean;
    /**
     *
     * @type {Array.<Index>}
     */
    indexes: Array<Index>;
    /**
     * Determines whether the Change Data Capture (CDC) flag is set for the table.
     * @type {Boolean|null}
     */
    cdc?: boolean;
    /**
     * Determines whether the table is a virtual table or not.
     * @type {Boolean}
     */
    virtual: boolean;
    /* Excluded from this release type: __constructor */
}

/** @private */
export declare class TimeoutError extends errors.DriverError {
    /**
     * @param {string} message
     */
    constructor(message: string);
}

export declare const timestampGeneration: {
    TimestampGenerator: typeof TimestampGenerator;
    MonotonicTimestampGenerator: typeof MonotonicTimestampGenerator;
};

/**
 * Creates a new instance of {@link TimestampGenerator}.
 * @classdesc
 * Generates client-side, microsecond-precision query timestamps.
 * <p>
 *   Given that Cassandra uses those timestamps to resolve conflicts, implementations should generate
 *   monotonically increasing timestamps for successive invocations of {@link TimestampGenerator.next()}.
 * </p>
 * @constructor
 */
export declare class TimestampGenerator {
    constructor();
    /**
     * Returns the next timestamp.
     * <p>
     *   Implementors should enforce increasing monotonicity of timestamps, that is,
     *   a timestamp returned should always be strictly greater that any previously returned
     *   timestamp.
     * <p/>
     * <p>
     *   Implementors should strive to achieve microsecond precision in the best possible way,
     *   which is usually largely dependent on the underlying operating system's capabilities.
     * </p>
     * @param {Client} client The {@link Client} instance to generate timestamps to.
     * @returns {Long|Number|null} the next timestamp (in microseconds). If it's equals to <code>null</code>, it won't be
     * sent by the driver, letting the server to generate the timestamp.
     * @abstract
     */
    next(client: Client): Long__default | number | null;
}

/**
 * If any of the arguments is not provided, it will be randomly generated, except for the date that will use the current
 * date.
 * <p>
 *   Note that when nodeId and/or clockId portions are not provided, the constructor will generate them using
 *   <code>crypto.randomBytes()</code>. As it's possible that <code>crypto.randomBytes()</code> might block, it's
 *   recommended that you use the callback-based version of the static methods <code>fromDate()</code> or
 *   <code>now()</code> in that case.
 * </p>
 * @class
 * @classdesc Represents an immutable version 1 universally unique identifier (UUID). A UUID represents a 128-bit value.
 * <p>Usage: <code>TimeUuid.now()</code></p>
 * @extends module:types~Uuid
 */
export declare class TimeUuid extends Uuid {
    /**
     * Creates a new instance of Uuid based on the parameters provided according to rfc4122.
     * If any of the arguments is not provided, it will be randomly generated, except for the date that will use the current
     * date.
     * <p>
     *   Note that when nodeId and/or clockId portions are not provided, the constructor will generate them using
     *   <code>crypto.randomBytes()</code>. As it's possible that <code>crypto.randomBytes()</code> might block, it's
     *   recommended that you use the callback-based version of the static methods <code>fromDate()</code> or
     *   <code>now()</code> in that case.
     * </p>
     * This class represents an immutable version 1 universally unique identifier (UUID). A UUID represents a 128-bit value.
     * <p>Usage: <code>TimeUuid.now()</code></p>
     * @param {Date} [value] The datetime for the instance, if not provided, it will use the current Date.
     * @param {Number} [ticks] A number from 0 to 10000 representing the 100-nanoseconds units for this instance to fill in the information not available in the Date,
     * as Ecmascript Dates have only milliseconds precision.
     * @param {String|Buffer} [nodeId] A 6-length Buffer or string of 6 ascii characters representing the node identifier, ie: 'host01'.
     * @param {String|Buffer} [clockId] A 2-length Buffer or string of 6 ascii characters representing the clock identifier.
     * @constructor
     */
    constructor(value: Date | Buffer, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer);
    /**
     * Generates a TimeUuid instance based on the Date provided using random node and clock values.
     * @param {Date} date Date to generate the v1 uuid.
     * @param {Number} [ticks] A number from 0 to 10000 representing the 100-nanoseconds units for this instance to fill in the information not available in the Date,
     * as Ecmascript Dates have only milliseconds precision.
     * @param {String|Buffer} [nodeId] A 6-length Buffer or string of 6 ascii characters representing the node identifier, ie: 'host01'.
     * If not provided, a random nodeId will be generated.
     * @param {String|Buffer} [clockId] A 2-length Buffer or string of 6 ascii characters representing the clock identifier.
     * If not provided a random clockId will be generated.
     * @param {Function} [callback] An optional callback to be invoked with the error as first parameter and the created
     * <code>TimeUuid</code> as second parameter. When a callback is provided, the random portions of the
     * <code>TimeUuid</code> instance are created asynchronously.
     * <p>
     *   When nodeId and/or clockId portions are not provided, this method will generate them using
     *   <code>crypto.randomBytes()</code>. As it's possible that <code>crypto.randomBytes()</code> might block, it's
     *   recommended that you use the callback-based version of this method in that case.
     * </p>
     * @example <caption>Generate a TimeUuid from a ECMAScript Date</caption>
     * const timeuuid = TimeUuid.fromDate(new Date());
     * @example <caption>Generate a TimeUuid from a Date with ticks portion</caption>
     * const timeuuid = TimeUuid.fromDate(new Date(), 1203);
     * @example <caption>Generate a TimeUuid from a Date without any random portion</caption>
     * const timeuuid = TimeUuid.fromDate(new Date(), 1203, 'host01', '02');
     * @example <caption>Generate a TimeUuid from a Date with random node and clock identifiers</caption>
     * TimeUuid.fromDate(new Date(), 1203, function (err, timeuuid) {
     *   // do something with the generated timeuuid
     * });
     */
    static fromDate(date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer): TimeUuid;
    static fromDate(date: Date, ticks: number, nodeId: string | Buffer, clockId: string | Buffer, callback: ValueCallback<TimeUuid>): void;
    /**
     * Parses a string representation of a TimeUuid
     * @param {String} value
     * @returns {TimeUuid}
     */
    static fromString(value: string): TimeUuid;
    /**
     * Returns the smaller possible type 1 uuid with the provided Date.
     */
    static min(date: Date, ticks?: number): TimeUuid;
    /**
     * Returns the biggest possible type 1 uuid with the provided Date.
     */
    static max(date: Date, ticks?: number): TimeUuid;
    /**
     * Generates a TimeUuid instance based on the current date using random node and clock values.
     * @param {String|Buffer} [nodeId] A 6-length Buffer or string of 6 ascii characters representing the node identifier, ie: 'host01'.
     * If not provided, a random nodeId will be generated.
     * @param {String|Buffer} [clockId] A 2-length Buffer or string of 6 ascii characters representing the clock identifier.
     * If not provided a random clockId will be generated.
     * @param {Function} [callback] An optional callback to be invoked with the error as first parameter and the created
     * <code>TimeUuid</code> as second parameter. When a callback is provided, the random portions of the
     * <code>TimeUuid</code> instance are created asynchronously.
     * <p>
     *   When nodeId and/or clockId portions are not provided, this method will generate them using
     *   <code>crypto.randomBytes()</code>. As it's possible that <code>crypto.randomBytes()</code> might block, it's
     *   recommended that you use the callback-based version of this method in that case.
     * </p>
     * @example <caption>Generate a TimeUuid from a Date without any random portion</caption>
     * const timeuuid = TimeUuid.now('host01', '02');
     * @example <caption>Generate a TimeUuid with random node and clock identifiers</caption>
     * TimeUuid.now(function (err, timeuuid) {
     *   // do something with the generated timeuuid
     * });
     * @example <caption>Generate a TimeUuid based on the current date (might block)</caption>
     * const timeuuid = TimeUuid.now();
     */
    static now(): TimeUuid;
    static now(nodeId: string | Buffer, clockId?: string | Buffer): TimeUuid;
    static now(nodeId: string | Buffer, clockId: string | Buffer, callback: ValueCallback<TimeUuid>): void;
    static now(callback: ValueCallback<TimeUuid>): void;
    /**
     * Gets the Date and 100-nanoseconds units representation of this instance.
     * @returns {{date: Date, ticks: Number}}
     */
    getDatePrecision(): {
        date: Date;
        ticks: number;
    };
    /**
     * Gets the Date representation of this instance.
     * @returns {Date}
     */
    getDate(): Date;
    /**
     * Returns the node id this instance
     * @returns {Buffer}
     */
    getNodeId(): Buffer;
    /**
     * Returns the clock id this instance, with the variant applied (first 2 msb being 1 and 0).
     * @returns {Buffer}
     */
    getClockId(): Buffer;
    /**
     * Returns the node id this instance as an ascii string
     * @returns {String}
     */
    getNodeIdString(): string;
}

/**
 * <p><strong>Backward compatibility only, use [TimeUuid]{@link module:types~TimeUuid} instead</strong>.</p>
 * Generates and returns a RFC4122 v1 (timestamp based) UUID in a string representation.
 * @param {{msecs, node, clockseq, nsecs}} [options]
 * @param {Buffer} [buffer]
 * @param {Number} [offset]
 * @deprecated Use [TimeUuid]{@link module:types~TimeUuid} instead
 */
export declare function timeuuid(options: {
    msecs: any;
    node: any;
    clockseq: any;
    nsecs: any;
}, buffer: Buffer, offset: number): string | Buffer;

/**
 * Represents a token on the Cassandra ring.
 */
declare class Token {
    protected _value: any;
    /* Excluded from this release type: __constructor */
    /**
     * @returns {DataTypeInfo} The type info for the
     *                                           type of the value of the token.
     */
    getType(): DataTypeInfo;
    /**
     * @returns {*} The raw value of the token.
     */
    getValue(): any;
    /* Excluded from this release type: toString */
    /**
     * Returns 0 if the values are equal, 1 if greater than other, -1
     * otherwise.
     *
     * @param {Token} other
     * @returns {Number}
     */
    compare(other: Token): number;
    equals(other: Token): boolean;
    /* Excluded from this release type: inspect */
}

export declare const token: {
    Token: typeof Token;
    TokenRange: typeof TokenRange;
};

/**
 * A wrapper load balancing policy that adds token awareness to a child policy.
 */
export declare class TokenAwarePolicy extends LoadBalancingPolicy {
    private childPolicy;
    /**
     * A wrapper load balancing policy that add token awareness to a child policy.
     * @param {LoadBalancingPolicy} childPolicy
     * @constructor
     */
    constructor(childPolicy: LoadBalancingPolicy);
    init(client: Client, hosts: HostMap, callback: EmptyCallback): void;
    getDistance(host: Host): distance;
    /**
     * Returns the hosts to use for a new query.
     * The returned plan will return local replicas first, if replicas can be determined, followed by the plan of the
     * child policy.
     * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
     * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
     * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
     * second parameter.
     */
    newQueryPlan(keyspace: string, executionOptions: ExecutionOptions, callback: (error: Error, iterator: Iterator<Host>) => void): void;
    getOptions(): Map<string, any>;
}

/* Excluded from this release type: Tokenizer */

/**
 * Represents a range of tokens on a Cassandra ring.
 *
 * A range is start-exclusive and end-inclusive.  It is empty when
 * start and end are the same token, except if that is the minimum
 * token, in which case the range covers the whole ring (this is
 * consistent with the behavior of CQL range queries).
 *
 * Note that CQL does not handle wrapping.  To query all partitions
 * in a range, see {@link unwrap}.
 */
declare class TokenRange {
    end: Token;
    start: Token;
    private _tokenizer;
    /* Excluded from this release type: __constructor */
    /**
     * Splits this range into a number of smaller ranges of equal "size"
     * (referring to the number of tokens, not the actual amount of data).
     *
     * Splitting an empty range is not permitted.  But not that, in edge
     * cases, splitting a range might produce one or more empty ranges.
     *
     * @param {Number} numberOfSplits Number of splits to make.
     * @returns {TokenRange[]} Split ranges.
     * @throws {Error} If splitting an empty range.
     */
    splitEvenly(numberOfSplits: number): TokenRange[];
    /**
     * A range is empty when start and end are the same token, except if
     * that is the minimum token, in which case the range covers the
     * whole ring.  This is consistent with the behavior of CQL range
     * queries.
     *
     * @returns {boolean} Whether this range is empty.
     */
    isEmpty(): boolean;
    /**
     * A range wraps around the end of the ring when the start token
     * is greater than the end token and the end token is not the
     * minimum token.
     *
     * @returns {boolean} Whether this range wraps around.
     */
    isWrappedAround(): boolean;
    /**
     * Splits this range into a list of two non-wrapping ranges.
     *
     * This will return the range itself if it is non-wrapped, or two
     * ranges otherwise.
     *
     * This is useful for CQL range queries, which do not handle
     * wrapping.
     *
     * @returns {TokenRange[]} The list of non-wrapping ranges.
     */
    unwrap(): TokenRange[];
    /**
     * Whether this range contains a given Token.
     *
     * @param {Token} token Token to check for.
     * @returns {boolean} Whether or not the Token is in this range.
     */
    contains(token: Token): boolean;
    /**
     * Determines if the input range is equivalent to this one.
     *
     * @param {TokenRange} other Range to compare with.
     * @returns {boolean} Whether or not the ranges are equal.
     */
    equals(other: TokenRange): boolean;
    /**
     * Returns 0 if the values are equal, otherwise compares against
     * start, if start is equal, compares against end.
     *
     * @param {TokenRange} other Range to compare with.
     * @returns {Number}
     */
    compare(other: TokenRange): number;
    /* Excluded from this release type: toString */
}

/**
 * Tracker module.
 * @module tracker
 */
export declare const tracker: {
    RequestTracker: typeof RequestTracker;
    RequestLogger: typeof RequestLogger;
};

/* Excluded from this release type: TransitionalModePlainTextAuthenticator */

/* Excluded from this release type: Tree */

/** @module types */
/**
 * @class
 * @classdesc A tuple is a sequence of immutable objects.
 * Tuples are sequences, just like [Arrays]{@link Array}. The only difference is that tuples can't be changed.
 * <p>
 *   As tuples can be used as a Map keys, the {@link Tuple#toString toString()} method calls toString of each element,
 *   to try to get a unique string key.
 * </p>
 */
export declare class Tuple {
    elements: any[];
    length: number;
    /**
     * Creates a new sequence of immutable objects with the parameters provided.
     * A tuple is a sequence of immutable objects.
     * Tuples are sequences, just like [Arrays]{@link Array}. The only difference is that tuples can't be changed.
     * <p>
     *   As tuples can be used as a Map keys, the {@link Tuple#toString toString()} method calls toString of each element,
     *   to try to get a unique string key.
     * </p>
     * @param {any[]} args The sequence elements as arguments.
     * @constructor
     */
    constructor(...args: any[]);
    /**
     * Creates a new instance of a tuple based on the Array
     * @param {Array} elements
     * @returns {Tuple}
     */
    static fromArray(elements: any[]): Tuple;
    /**
     * Returns the value located at the index.
     * @param {Number} index Element index
     */
    get(index: number): any;
    /**
     * Returns the string representation of the sequence surrounded by parenthesis, ie: (1, 2).
     * <p>
     *   The returned value attempts to be a unique string representation of its values.
     * </p>
     * @returns {string}
     */
    toString(): string;
    /**
     * Returns the Array representation of the sequence.
     * @returns {Array}
     */
    toJSON(): any[];
    /**
     * Gets the elements as an array
     * @returns {Array}
     */
    values(): any[];
}

declare type TupleColumnInfo = {
    code: (dataTypes.tuple);
    info: Array<DataTypeInfo>;
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

declare type TupleListColumnInfoWithoutSubtype = {
    code: (dataTypes.tuple | dataTypes.list);
};

export declare const types: {
    opcodes: {
        error: number;
        startup: number;
        ready: number;
        authenticate: number;
        credentials: number;
        options: number;
        supported: number;
        query: number;
        result: number;
        prepare: number;
        execute: number;
        register: number;
        event: number;
        batch: number;
        authChallenge: number;
        authResponse: number;
        authSuccess: number;
        cancel: number;
        /**
         * Determines if the code is a valid opcode
         */
        isInRange: (code: any) => boolean;
    };
    consistencies: typeof consistencies;
    consistencyToString: {};
    dataTypes: typeof dataTypes;
    /* Excluded from this release type: getDataTypeNameByCode */
    distance: typeof distance;
    frameFlags: {
        compression: number;
        tracing: number;
        customPayload: number;
        warning: number;
    };
    protocolEvents: {
        topologyChange: string;
        statusChange: string;
        schemaChange: string;
    };
    protocolVersion: typeof protocolVersion;
    responseErrorCodes: typeof responseErrorCodes;
    resultKind: {
        voidResult: number;
        rows: number;
        setKeyspace: number;
        prepared: number;
        schemaChange: number;
    };
    timeuuid: typeof timeuuid;
    uuid: typeof uuid;
    BigDecimal: typeof BigDecimal;
    Duration: typeof Duration;
    /* Excluded from this release type: FrameHeader */
    InetAddress: typeof InetAddress;
    Integer: typeof Integer;
    LocalDate: typeof LocalDate;
    LocalTime: typeof LocalTime;
    Long: typeof Long__default;
    ResultSet: typeof ResultSet;
    ResultStream: typeof ResultStream;
    Row: typeof Row;
    DriverError: typeof DriverError;
    TimeoutError: typeof TimeoutError;
    TimeUuid: typeof TimeUuid;
    Tuple: typeof Tuple;
    Uuid: typeof Uuid;
    unset: Readonly<{
        readonly unset: true;
    }>;
    /* Excluded from this release type: generateTimestamp */
    Vector: typeof Vector;
};

export declare interface Udt {
    name: string;
    fields: ColumnInfo[];
}

declare type UdtColumnInfo = {
    code: (dataTypes.udt);
    info: {
        name: string;
        fields: Array<{
            name: string;
            type: DataTypeInfo;
        }>;
    };
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

/* Excluded from this release type: UdtGraphWrapper */

/**
 * A [TableMappings]{@link module:mapping~TableMappings} implementation that converts CQL column names in all-lowercase
 * identifiers with underscores (snake case) to camel case (initial lowercase letter) property names.
 * <p>
 *   The conversion is performed without any checks for the source format, you should make sure that the source
 *   format is snake case for CQL identifiers and camel case for properties.
 * </p>
 * @alias module:mapping~UnderscoreCqlToCamelCaseMappings
 * @implements {module:mapping~TableMappings}
 */
export declare class UnderscoreCqlToCamelCaseMappings extends TableMappings {
    /**
     * Creates a new instance of {@link UnderscoreCqlToCamelCaseMappings}
     */
    constructor();
    /**
     * Converts a property name in camel case to snake case.
     * @param {String} propName Name of the property to convert to snake case.
     * @return {String}
     */
    getColumnName(propName: string): string;
    /**
     * Converts a column name in snake case to camel case.
     * @param {String} columnName The column name to convert to camel case.
     * @return {String}
     */
    getPropertyName(columnName: string): string;
}

/**
 * Unset representation.
 * <p>
 *   Use this field if you want to set a parameter to <code>unset</code>. Valid for Cassandra 2.2 and above.
 * </p>
 */
export declare const unset: Readonly<{
    readonly unset: true;
}>;

export declare type UpdateDocInfo = {
    fields?: string[];
    ttl?: number;
    ifExists?: boolean;
    when?: {
        [key: string]: any;
    };
    orderBy?: {
        [key: string]: string;
    };
    limit?: number;
    deleteOnlyColumns?: boolean;
};

/** @module types */
/**
 * @class
 * @classdesc Represents an immutable universally unique identifier (UUID). A UUID represents a 128-bit value.
 */
export declare class Uuid {
    /* Excluded from this release type: buffer */
    /**
     * Creates a new instance of Uuid based on a Buffer
     * Represents an immutable universally unique identifier (UUID). A UUID represents a 128-bit value.
     * @param {Buffer} buffer The 16-length buffer.
     * @constructor
     */
    constructor(buffer: Buffer);
    /**
     * Parses a string representation of a Uuid
     * @param {String} value
     * @returns {Uuid}
     */
    static fromString(value: string): Uuid;
    /**
     * Creates a new random (version 4) Uuid.
     * @param {function} [callback] Optional callback to be invoked with the error as first parameter and the created Uuid as
     * second parameter.
     * @returns {Uuid}
     */
    static random(): Uuid;
    static random(callback: ValueCallback<Uuid>): void;
    /**
     * Gets the bytes representation of a Uuid
     * @returns {Buffer}
     */
    getBuffer(): Buffer;
    /**
     * Compares this object to the specified object.
     * The result is true if and only if the argument is not null, is a UUID object, and contains the same value, bit for bit, as this UUID.
     * @param {Uuid} other The other value to test for equality.
     */
    equals(other: Uuid): boolean;
    /**
     * Returns a string representation of the value of this Uuid instance.
     * 32 hex separated by hyphens, in the form of 00000000-0000-0000-0000-000000000000.
     * @returns {String}
     */
    toString(): string;
    /**
     * Provide the name of the constructor and the string representation
     * @returns {string}
     */
    inspect(): string;
    /**
     * Returns the string representation.
     * Method used by the native JSON.stringify() to serialize this instance.
     */
    toJSON(): string;
}

/**
 * <p><strong>Backward compatibility only, use [Uuid]{@link module:types~Uuid} class instead</strong>.</p>
 * Generate and return a RFC4122 v4 UUID in a string representation.
 * @deprecated Use [Uuid]{@link module:types~Uuid} class instead
 */
export declare function uuid(options: any, buffer: any, offset: any): any;

declare type ValueCallback<T> = (err: Error, val: T) => void;

export declare class Vector {
    /**
     * Returns the number of the elements.
     * @type Number
     */
    length: number;
    subtype: string;
    elements: any[];
    /**
     *
     * @param {Float32Array | Array<any>} elements
     * @param {string} [subtype]
     */
    constructor(elements: Float32Array | Array<any>, subtype?: string);
    /**
     * Returns the string representation of the vector.
     * @returns {string}
     */
    toString(): string;
    /**
     *
     * @param {number} index
     */
    at(index: number): any;
    /**
     *
     * @returns {IterableIterator<any>} an iterator over the elements of the vector
     */
    [Symbol.iterator](): IterableIterator<any>;
    static get [Symbol.species](): typeof Vector;
    /**
     *
     * @param {(value: any, index: number, array: any[]) => void} callback
     */
    forEach(callback: (value: any, index: number, array: any[]) => void): void;
    /**
     * @returns {string | null} get the subtype string, e.g., "float", but it's optional so it can return null
     */
    getSubtype(): string | null;
}

declare type VectorColumnInfo = {
    code: (dataTypes.custom);
    customTypeName: ('vector');
    info: [DataTypeInfo, number];
    options?: {
        frozen?: boolean;
        reversed?: boolean;
    };
};

export declare const version: string;

/**
 * Represents a graph Vertex.
 * @extends Element
 * @memberOf module:datastax/graph
 */
declare class Vertex extends Element {
    properties: {
        [key: string]: any[];
    };
    /**
     * @param id
     * @param {String} label
     * @param {{ [key: string]: any[] }} properties
     */
    constructor(id: any, label: string, properties?: {
        [key: string]: any[];
    });
}

/**
 * Represents a graph vertex property.
 * @extends Element
 * @memberOf module:datastax/graph
 */
declare class VertexProperty extends Element {
    value: any;
    key: string;
    properties: any;
    /**
     * @param id
     * @param {String} label
     * @param value
     * @param {Object} properties
     */
    constructor(id: any, label: string, value: any, properties: object);
}

/**
 * Represents a run-time exception when attempting to decode a vint and the JavaScript Number doesn't have enough space to fit the value that was decoded
 */
export declare class VIntOutOfRangeException extends DriverError {
    /**
     * Represents a run-time exception when attempting to decode a vint and the JavaScript Number doesn't have enough space to fit the value that was decoded
     * @param {Long} long
     */
    constructor(long: Long__default);
}

/**
 * @classdesc
 * Exposed for backward-compatibility only, it's recommended that you use {@link AllowListPolicy} instead.
 * @extends AllowListPolicy
 * @deprecated Use allow-list instead. It will be removed in future major versions.
 */
export declare class WhiteListPolicy extends AllowListPolicy {
    /**
     * Creates a new instance of WhiteListPolicy.
     * @param {LoadBalancingPolicy} childPolicy - The wrapped policy.
     * @param {Array.<string>} allowList - The hosts address in the format ipAddress:port.
     * @deprecated Use AllowListPolicy instead. It will be removed in future major versions.
     */
    constructor(childPolicy: LoadBalancingPolicy, allowList: Array<string>);
}

/* Excluded from this release type: WriteQueue */

export { }
export namespace auth {
    export type Authenticator = InstanceType<typeof auth.Authenticator>;
    export type AuthProvider = InstanceType<typeof auth.AuthProvider>;
    export type PlainTextAuthProvider = InstanceType<typeof auth.PlainTextAuthProvider>;
    export type DsePlainTextAuthProvider = InstanceType<typeof auth.DsePlainTextAuthProvider>;
    export type DseGssapiAuthProvider = InstanceType<typeof auth.DseGssapiAuthProvider>;
}

type _Options = Options;

export namespace concurrent {
    export type ResultSetGroup = InstanceType<typeof concurrent.ResultSetGroup>;
    export type executeConcurrent = typeof concurrent.executeConcurrent;
    export type Options = _Options;
}

export namespace datastax {
    export namespace graph {
        export type asDouble = typeof datastax.graph.asDouble;
        export type asFloat = typeof datastax.graph.asFloat;
        export type asInt = typeof datastax.graph.asInt;
        export type asTimestamp = typeof datastax.graph.asTimestamp;
        export type asUdt = typeof datastax.graph.asUdt;
        export type direction = typeof datastax.graph.direction;
        export type Edge = InstanceType<typeof datastax.graph.Edge>;
        export type Element = InstanceType<typeof datastax.graph.Element>;
        export type GraphResultSet = InstanceType<typeof datastax.graph.GraphResultSet>;
        export type Path = InstanceType<typeof datastax.graph.Path>;
        export type Property = InstanceType<typeof datastax.graph.Property>;
        export type t = typeof datastax.graph.t;
        export type Vertex = InstanceType<typeof datastax.graph.Vertex>;
        export type VertexProperty = InstanceType<typeof datastax.graph.VertexProperty>;
    }
    export namespace search {
        export type DateRange = InstanceType<typeof datastax.search.DateRange>;
        export type DateRangeBound = InstanceType<typeof datastax.search.DateRangeBound>;
        export type dateRangePrecision = typeof datastax.search.dateRangePrecision;
    }
}

export namespace geometry {
    export type LineString = InstanceType<typeof geometry.LineString>;
    export type Point = InstanceType<typeof geometry.Point>;
    export type Polygon = InstanceType<typeof geometry.Polygon>;
}

type _MappingExecutionOptions = MappingExecutionOptions;
type _MappingOptions = MappingOptions;
type _FindDocInfo = FindDocInfo;
type _UpdateDocInfo = UpdateDocInfo;
type _RemoveDocInfo = RemoveDocInfo;
type _ModelOptions = ModelOptions;
type _ModelColumnOptions = ModelColumnOptions;
type _QueryOperator = QueryOperator;
type _QueryAssignment = QueryAssignment;

export namespace mapping {
    export type TableMappings = InstanceType<typeof mapping.TableMappings>;
    export type DefaultTableMappings = InstanceType<typeof mapping.DefaultTableMappings>;
    export type UnderscoreCqlToCamelCaseMappings = InstanceType<typeof mapping.UnderscoreCqlToCamelCaseMappings>;
    export type Result = InstanceType<typeof mapping.Result>;
    export type MappingExecutionOptions = _MappingExecutionOptions;
    export type ModelTables = InstanceType<typeof mapping.ModelTables>;
    export type Mapper = InstanceType<typeof mapping.Mapper>;
    export type MappingOptions = _MappingOptions;
    export type FindDocInfo = _FindDocInfo;
    export type UpdateDocInfo = _UpdateDocInfo;
    export type RemoveDocInfo = _RemoveDocInfo;
    export type ModelOptions = _ModelOptions;
    export type ModelColumnOptions = _ModelColumnOptions;
    export type ModelBatchItem = InstanceType<typeof mapping.ModelBatchItem>;
    export type ModelBatchMapper = InstanceType<typeof mapping.ModelBatchMapper>;
    export type ModelMapper = InstanceType<typeof mapping.ModelMapper>;
    export namespace q{
        export type QueryOperator = _QueryOperator;
        export type QueryAssignment = _QueryAssignment;
        export type in_ = typeof mapping.q.in_;
        export type gt = typeof mapping.q.gt;
        export type gte = typeof mapping.q.gte;
        export type lt = typeof mapping.q.lt;
        export type lte = typeof mapping.q.lte;
        export type notEq = typeof mapping.q.notEq;
        export type and = typeof mapping.q.and;
        export type incr = typeof mapping.q.incr;
        export type decr = typeof mapping.q.decr;
        export type append = typeof mapping.q.append;
        export type prepend = typeof mapping.q.prepend;
        export type remove = typeof mapping.q.remove;
    }
}

type _IndexKind = IndexKind;
export namespace metadata {
    export type Aggregate = InstanceType<typeof Aggregate>;
    export type ClientState = InstanceType<typeof ClientState>;
    export type DataTypeInfo = InstanceType<typeof DataTypeInfo>;
    export type ColumnInfo = InstanceType<typeof ColumnInfo>;
    export type IndexKind = _IndexKind;
    export type Index = InstanceType<typeof Index>;
    export type DataCollection = InstanceType<typeof DataCollection>;
    export type MaterializedView = InstanceType<typeof MaterializedView>;
    export type TableMetadata = InstanceType<typeof TableMetadata>;
    export type QueryTrace = InstanceType<typeof QueryTrace>;
    export type SchemaFunction = InstanceType<typeof SchemaFunction>;
    export type Udt = InstanceType<typeof Udt>;
    export type Metadata = InstanceType<typeof Metadata>;
}

export namespace metrics{
    export type ClientMetrics = InstanceType<typeof ClientMetrics>;
    export type DefaultMetrics = InstanceType<typeof DefaultMetrics>;
}

type _DecisionInfo = DecisionInfo;
type _OperationInfo = OperationInfo;
export namespace policies{
    export type defaultAddressTranslator = typeof defaultAddressTranslator;
    export type defaultLoadBalancingPolicy = typeof defaultLoadBalancingPolicy;
    export type defaultReconnectionPolicy = typeof defaultReconnectionPolicy;
    export type defaultRetryPolicy = typeof defaultRetryPolicy;
    export type defaultSpeculativeExecutionPolicy = typeof defaultSpeculativeExecutionPolicy;
    export type defaultTimestampGenerator = typeof defaultTimestampGenerator;
    export namespace addressResolution{
        export type AddressTranslator = InstanceType<typeof AddressTranslator>;
        export type EC2MultiRegionTranslator = InstanceType<typeof EC2MultiRegionTranslator>;
    }
    export namespace loadBalancing{
        export type LoadBalancingPolicy = InstanceType<typeof LoadBalancingPolicy>;
        export type DCAwareRoundRobinPolicy = InstanceType<typeof DCAwareRoundRobinPolicy>;
        export type TokenAwarePolicy = InstanceType<typeof TokenAwarePolicy>;
        export type AllowListPolicy = InstanceType<typeof AllowListPolicy>;
        export type WhiteListPolicy = InstanceType<typeof WhiteListPolicy>;
        export type RoundRobinPolicy = InstanceType<typeof RoundRobinPolicy>;
        export type DefaultLoadBalancingPolicy = InstanceType<typeof DefaultLoadBalancingPolicy>;
    }
    export namespace reconnection{
        export type ReconnectionPolicy = InstanceType<typeof ReconnectionPolicy>;
        export type ConstantReconnectionPolicy = InstanceType<typeof ConstantReconnectionPolicy>;
        export type ExponentialReconnectionPolicy = InstanceType<typeof ExponentialReconnectionPolicy>;
    }
    export namespace retry{
        export type DecisionInfo = _DecisionInfo;
        export type OperationInfo = _OperationInfo;
        export type IdempotenceAwareRetryPolicy  = InstanceType<typeof IdempotenceAwareRetryPolicy>;
        export type FallthroughRetryPolicy = InstanceType<typeof FallthroughRetryPolicy>;
        export type RetryPolicy = InstanceType<typeof RetryPolicy>;
        export namespace RetryDecision{
            export type retryDecision = RetryPolicy.retryDecision;
        }
        export namespace speculativeExecution{
            export type ConstantSpeculativeExecutionPolicy = InstanceType<typeof ConstantSpeculativeExecutionPolicy>;
            export type NoSpeculativeExecutionPolicy = InstanceType<typeof NoSpeculativeExecutionPolicy>;
            export type SpeculativeExecutionPolicy = InstanceType<typeof SpeculativeExecutionPolicy>;
        }
        export namespace timestampGeneration{
            export type TimestampGenerator = InstanceType<typeof TimestampGenerator>;
            export type MonotonicTimestampGenerator = InstanceType<typeof MonotonicTimestampGenerator>;
        }
    }    
}

export namespace tracker{
    export type RequestTracker = InstanceType<typeof RequestTracker>;
    export type RequestLogger = InstanceType<typeof RequestLogger>;
}

export namespace types {
    export type Long = InstanceType<typeof types.Long>;
    export type consistencies = typeof types.consistencies;
    export type dataTypes = typeof types.dataTypes;
    export type distance = typeof types.distance;
    export type responseErrorCodes = typeof types.responseErrorCodes;
    export type protocolVersion = typeof types.protocolVersion;
    export type unset = unset;
    export type BigDecimal = InstanceType<typeof types.BigDecimal>;
    export type Duration = InstanceType<typeof types.Duration>;
    export type InetAddress = InstanceType<typeof types.InetAddress>;
    export type Integer = InstanceType<typeof types.Integer>;
    export type LocalDate = InstanceType<typeof types.LocalDate>;
    export type LocalTime = InstanceType<typeof types.LocalTime>;
    export type ResultSet = InstanceType<typeof types.ResultSet>;
    export type ResultStream = InstanceType<typeof types.ResultStream>;
    export type Row = InstanceType<typeof types.Row>;
    export type TimeUuid = InstanceType<typeof types.TimeUuid>;
    export type Tuple = InstanceType<typeof types.Tuple>;
    export type Uuid = InstanceType<typeof types.Uuid>;
    export type Vector = InstanceType<typeof types.Vector>;
}

export namespace errors {
    export type ArgumentError = InstanceType<typeof errors.ArgumentError>;
    export type AuthenticationError = InstanceType<typeof errors.AuthenticationError>;
    export type BusyConnectionError = InstanceType<typeof errors.BusyConnectionError>;
    export type DriverError = InstanceType<typeof errors.DriverError>;
    export type DriverInternalError = InstanceType<typeof errors.DriverInternalError>;
    export type NoHostAvailableError = InstanceType<typeof errors.NoHostAvailableError>;
    export type NotSupportedError = InstanceType<typeof errors.NotSupportedError>;
    export type OperationTimedOutError = InstanceType<typeof errors.OperationTimedOutError>;
    export type ResponseError = InstanceType<typeof errors.ResponseError>;
    export type VIntOutOfRangeException = InstanceType<typeof errors.VIntOutOfRangeException>;
}

export namespace token{
    export type Token = InstanceType<typeof Token>;
    export type TokenRange = InstanceType<typeof TokenRange>;
}