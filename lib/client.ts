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

const events = require('events');
const util = require('util');

const utils = require('./utils.js');
const errors = require('./errors.js');
const types = require('./types');
const { ProfileManager } = require('./execution-profile');
const requests = require('./requests');
const clientOptions = require('./client-options');
const ClientState = require('./metadata/client-state');
const description = require('../package.json').description;
const { version } = require('../package.json');
const { DefaultExecutionOptions } = require('./execution-options');
const ControlConnection = require('./control-connection');
const RequestHandler = require('./request-handler');
const PrepareHandler = require('./prepare-handler');
const InsightsClient = require('./insights-client');
const cloud = require('./datastax/cloud');
const GraphExecutor = require('./datastax/graph/graph-executor');
const promiseUtils = require('./promise-utils');

/**
 * Max amount of pools being warmup in parallel, when warmup is enabled
 * @private
 */
const warmupLimit = 32;

/**
 * Client options.
 * <p>While the driver provides lots of extensibility points and configurability, few client options are required.</p>
 * <p>Default values for all settings are designed to be suitable for the majority of use cases, you should avoid
 * fine tuning it when not needed.</p>
 * <p>See [Client constructor]{@link Client} documentation for recommended options.</p>
 * @typedef {Object} ClientOptions
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

/**
 * Query options
 * @typedef {Object} QueryOptions
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
 */

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
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  this.options = clientOptions.extend({ logEmitter: this.emit.bind(this), id: types.Uuid.random() }, options);
  Object.defineProperty(this, 'profileManager', { value: new ProfileManager(this.options) });
  Object.defineProperty(this, 'controlConnection', {
    value: new ControlConnection(this.options, this.profileManager), writable: true }
  );
  Object.defineProperty(this, 'insightsClient', { value: new InsightsClient(this)});

  //Unlimited amount of listeners for internal event queues by default
  this.setMaxListeners(0);
  this.connected = false;
  this.isShuttingDown = false;
  /**
   * Gets the name of the active keyspace.
   * @type {String}
   */
  this.keyspace = options.keyspace;
  /**
   * Gets the schema and cluster metadata information.
   * @type {Metadata}
   */
  this.metadata = this.controlConnection.metadata;
  /**
   * Gets an associative array of cluster hosts.
   * @type {HostMap}
   */
  this.hosts = this.controlConnection.hosts;

  /**
   * The [ClientMetrics]{@link module:metrics~ClientMetrics} instance used to expose measurements of its internal
   * behavior and of the server as seen from the driver side.
   * <p>By default, a [DefaultMetrics]{@link module:metrics~DefaultMetrics} instance is used.</p>
   * @type {ClientMetrics}
   */
  this.metrics = this.options.metrics;

  this._graphExecutor = new GraphExecutor(this, options, this._execute);
}

util.inherits(Client, events.EventEmitter);

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
Client.prototype.connect = function (callback) {
  if (this.connected && callback) {
    // Avoid creating Promise to immediately resolve them
    return callback();
  }

  return promiseUtils.optionalCallback(this._connect(), callback);
};

/**
 * Async-only version of {@link Client#connect()}.
 * @private
 */
Client.prototype._connect = async function () {
  if (this.connected) {
    return;
  }

  if (this.isShuttingDown) {
    //it is being shutdown, don't allow further calls to connect()
    throw new errors.NoHostAvailableError(null, 'Connecting after shutdown is not supported');
  }

  if (this.connecting) {
    return promiseUtils.fromEvent(this, 'connected');
  }

  this.connecting = true;
  this.log('info', util.format("Connecting to cluster using '%s' version %s", description, version));

  try {
    await cloud.init(this.options);
    await this.controlConnection.init();
    this.hosts = this.controlConnection.hosts;
    await this.profileManager.init(this, this.hosts);

    if (this.keyspace) {
      await RequestHandler.setKeyspace(this);
    }

    clientOptions.setMetadataDependent(this);

    await this._warmup();

  } catch (err) {
    // We should close the pools (if any) and reset the state to allow successive calls to connect()
    await this.controlConnection.reset();
    this.connected = false;
    this.connecting = false;
    this.emit('connected', err);
    throw err;
  }

  this._setHostListeners();

  // Set the distance of the control connection host relatively to this instance
  this.profileManager.getDistance(this.controlConnection.host);
  this.insightsClient.init();
  this.connected = true;
  this.connecting = false;
  this.emit('connected');
};

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
Client.prototype.execute = function (query, params, options, callback) {
  // This method acts as a wrapper for the async method _execute()

  if (!callback) {
    // Set default argument values for optional parameters
    if (typeof options === 'function') {
      callback = options;
      options = null;
    } else if (typeof params === 'function') {
      callback = params;
      params = null;
    }
  }

  try {
    const execOptions = DefaultExecutionOptions.create(options, this);
    return promiseUtils.optionalCallback(this._execute(query, params, execOptions), callback);
  }
  catch (err) {
    // There was an error when parsing the user options
    if (callback) {
      return callback(err);
    }

    return Promise.reject(err);
  }
};

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
Client.prototype.executeGraph = function (query, parameters, options, callback) {
  callback = callback || (options ? options : parameters);

  if (typeof callback === 'function') {
    parameters = typeof parameters !== 'function' ? parameters : null;
    return promiseUtils.toCallback(this._graphExecutor.send(query, parameters, options), callback);
  }

  return this._graphExecutor.send(query, parameters, options);
};

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
Client.prototype.eachRow = function (query, params, options, rowCallback, callback) {
  if (!callback && rowCallback && typeof options === 'function') {
    callback = utils.validateFn(rowCallback, 'rowCallback');
    rowCallback = options;
  } else {
    callback = callback || utils.noop;
    rowCallback = utils.validateFn(rowCallback || options || params, 'rowCallback');
  }

  params = typeof params !== 'function' ? params : null;

  let execOptions;
  try {
    execOptions = DefaultExecutionOptions.create(options, this, rowCallback);
  }
  catch (e) {
    return callback(e);
  }

  let rowLength = 0;

  const nextPage = () => promiseUtils.toCallback(this._execute(query, params, execOptions), pageCallback);

  function pageCallback (err, result) {
    if (err) {
      return callback(err);
    }
    // Next requests in case paging (auto or explicit) is used
    rowLength += result.rowLength;

    if (result.rawPageState !== undefined) {
      // Use new page state as next request page state
      execOptions.setPageState(result.rawPageState);
      if (execOptions.isAutoPage()) {
        // Issue next request for the next page
        return nextPage();
      }
      // Allows for explicit (manual) paging, in case the caller needs it
      result.nextPage = nextPage;
    }

    // Finished auto-paging
    result.rowLength = rowLength;
    callback(null, result);
  }

  promiseUtils.toCallback(this._execute(query, params, execOptions), pageCallback);
};

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
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value
 * @param {QueryOptions} [options] The query options.
 * @param {function} [callback] executes callback(err) after all rows have been received or if there is an error
 * @returns {ResultStream}
 */
Client.prototype.stream = function (query, params, options, callback) {
  callback = callback || utils.noop;
  // NOTE: the nodejs stream maintains yet another internal buffer
  // we rely on the default stream implementation to keep memory
  // usage reasonable.
  const resultStream = new types.ResultStream({ objectMode: 1 });
  function onFinish(err, result) {
    if (err) {
      resultStream.emit('error', err);
    }
    if (result && result.nextPage ) {
      // allows for throttling as per the
      // default nodejs stream implementation
      resultStream._valve(function pageValve() {
        try {
          result.nextPage();
        }
        catch( ex ) {
          resultStream.emit('error', ex );
        }
      });
      return;
    }
    // Explicitly dropping the valve (closure)
    resultStream._valve(null);
    resultStream.add(null);
    callback(err);
  }
  let sync = true;
  this.eachRow(query, params, options, function rowCallback(n, row) {
    resultStream.add(row);
  }, function eachRowFinished(err, result) {
    if (sync) {
      // Prevent sync callback
      return setImmediate(function eachRowFinishedImmediate() {
        onFinish(err, result);
      });
    }
    onFinish(err, result);
  });
  sync = false;
  return resultStream;
};

/**
 * Executes batch of queries on an available connection to a host.
 * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
 * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array
 * of object containing the query and params
 * @param {QueryOptions} [options] The query options.
 * @param {ResultCallback} [callback] Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function (queries, options, callback) {
  if (!callback && typeof options === 'function') {
    callback = options;
    options = null;
  }

  return promiseUtils.optionalCallback(this._batch(queries, options), callback);
};

/**
 * Async-only version of {@link Client#batch()} .
 * @param {Array.<string>|Array.<{query, params}>}queries
 * @param {QueryOptions} options
 * @returns {Promise<ResultSet>}
 * @private
 */
Client.prototype._batch = async function (queries, options) {
  if (!Array.isArray(queries)) {
    throw new errors.ArgumentError('Queries should be an Array');
  }

  if (queries.length === 0) {
    throw new errors.ArgumentError('Queries array should not be empty');
  }

  await this._connect();

  const execOptions = DefaultExecutionOptions.create(options, this);
  let queryItems;

  if (execOptions.isPrepared()) {
    // use keyspace from query options if protocol supports per-query keyspace, otherwise use connection keyspace.
    const version = this.controlConnection.protocolVersion;
    const queryKeyspace = types.protocolVersion.supportsKeyspaceInRequest(version) && options.keyspace || this.keyspace;
    queryItems = await PrepareHandler.getPreparedMultiple(
      this, execOptions.getLoadBalancingPolicy(), queries, queryKeyspace);
  } else {
    queryItems = new Array(queries.length);

    for (let i = 0; i < queries.length; i++) {
      const item = queries[i];
      if (!item) {
        throw new errors.ArgumentError(`Invalid query at index ${i}`);
      }

      const query = typeof item === 'string' ? item : item.query;
      if (!query) {
        throw new errors.ArgumentError(`Invalid query at index ${i}`);
      }

      queryItems[i] = { query, params: item.params };
    }
  }

  const request = await this._createBatchRequest(queryItems, execOptions);
  return await RequestHandler.send(request, execOptions, this);
};

/**
 * Gets the host that are replicas of a given token.
 * @param {String} keyspace
 * @param {Buffer} token
 * @returns {Array<Host>}
 */
Client.prototype.getReplicas = function (keyspace, token) {
  return this.metadata.getReplicas(keyspace, token);
};

/**
 * Gets a snapshot containing information on the connections pools held by this Client at the current time.
 * <p>
 *   The information provided in the returned object only represents the state at the moment this method was called and
 *   it's not maintained in sync with the driver metadata.
 * </p>
 * @returns {ClientState} A [ClientState]{@linkcode module:metadata~ClientState} instance.
 */
Client.prototype.getState = function () {
  return ClientState.from(this);
};

Client.prototype.log = utils.log;

/**
 * Closes all connections to all hosts.
 * <p>It returns a <code>Promise</code> when a <code>callback</code> is not provided.</p>
 * @param {Function} [callback] Optional callback to be invoked when finished closing all connections.
 */
Client.prototype.shutdown = function (callback) {
  return promiseUtils.optionalCallback(this._shutdown(), callback);
};

/** @private */
Client.prototype._shutdown = async function () {
  this.log('info', 'Shutting down');

  if (!this.hosts || !this.connected) {
    // not initialized
    this.connected = false;
    return;
  }

  if (this.connecting) {
    this.log('warning', 'Shutting down while connecting');
    // wait until finish connecting for easier troubleshooting
    await promiseUtils.fromEvent(this, 'connected');
  }

  this.connected = false;
  this.isShuttingDown = true;
  const hosts = this.hosts.values();

  this.insightsClient.shutdown();

  // Shutdown the ControlConnection before shutting down the pools
  this.controlConnection.shutdown();
  this.options.policies.speculativeExecution.shutdown();

  if (this.options.requestTracker) {
    this.options.requestTracker.shutdown();
  }

  // go through all the host and shut down their pools
  await Promise.all(hosts.map(h => h.shutdown(false)));
};

/**
 * Waits until that the schema version in all nodes is the same or the waiting time passed.
 * @param {Connection} connection
 * @returns {Promise<boolean>}
 * @ignore
 */
Client.prototype._waitForSchemaAgreement = async function (connection) {
  if (this.hosts.length === 1) {
    return true;
  }

  const start = process.hrtime();
  const maxWaitSeconds = this.options.protocolOptions.maxSchemaAgreementWaitSeconds;

  this.log('info', 'Waiting for schema agreement');

  let versionsMatch;

  while (!versionsMatch && process.hrtime(start)[0] < maxWaitSeconds) {
    versionsMatch = await this.metadata.compareSchemaVersions(connection);

    if (versionsMatch) {
      this.log('info', 'Schema versions match');
      break;
    }

    // Let some time pass before the next check
    await promiseUtils.delay(500);
  }

  return versionsMatch;
};

/**
 * Waits for schema agreements and schedules schema metadata refresh.
 * @param {Connection} connection
 * @param event
 * @returns {Promise<boolean>}
 * @ignore
 * @internal
 */
Client.prototype.handleSchemaAgreementAndRefresh = async function (connection, event) {
  let agreement = false;

  try {
    agreement = await this._waitForSchemaAgreement(connection);
  } catch (err) {
    //we issue a warning but we continue with the normal flow
    this.log('warning', 'There was an error while waiting for the schema agreement between nodes', err);
  }

  if (!this.options.isMetadataSyncEnabled) {
    return agreement;
  }

  // Refresh metadata immediately
  try {
    await this.controlConnection.handleSchemaChange(event, true);
  } catch (err) {
    this.log('warning', 'There was an error while handling schema change', err);
  }

  return agreement;
};

/**
 * Connects and handles the execution of prepared and simple statements.
 * @param {string} query
 * @param {Array} params
 * @param {ExecutionOptions} execOptions
 * @returns {Promise<ResultSet>}
 * @private
 */
Client.prototype._execute = async function (query, params, execOptions) {
  const version = this.controlConnection.protocolVersion;

  if (!execOptions.isPrepared() && params && !Array.isArray(params) &&
    !types.protocolVersion.supportsNamedParameters(version)) {
    // Only Cassandra 2.1 and above supports named parameters
    throw new errors.ArgumentError('Named parameters for simple statements are not supported, use prepare flag');
  }

  let request;

  if (!this.connected) {
    // Micro optimization to avoid an async execution for a simple check
    await this._connect();
  }

  if (!execOptions.isPrepared()) {
    request = await this._createQueryRequest(query, execOptions, params);
  } else {
    const lbp = execOptions.getLoadBalancingPolicy();

    // Use keyspace from query options if protocol supports per-query keyspace, otherwise use connection keyspace.
    const queryKeyspace = types.protocolVersion.supportsKeyspaceInRequest(version) &&
      execOptions.getKeyspace() || this.keyspace;

    const { queryId, meta } = await PrepareHandler.getPrepared(this, lbp, query, queryKeyspace);
    request = await this._createExecuteRequest(query, queryId, execOptions, params, meta);
  }

  return await RequestHandler.send(request, execOptions, this);
};

/**
 * Sets the listeners for the nodes.
 * @private
 */
Client.prototype._setHostListeners = function () {
  function getHostUpListener(emitter, h) {
    return () => emitter.emit('hostUp', h);
  }

  function getHostDownListener(emitter, h) {
    return () => emitter.emit('hostDown', h);
  }

  const self = this;

  // Add status listeners when new nodes are added and emit hostAdd
  this.hosts.on('add', function hostAddedListener(h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
    self.emit('hostAdd', h);
  });

  // Remove all listeners and emit hostRemove
  this.hosts.on('remove', function hostRemovedListener(h) {
    h.removeAllListeners();
    self.emit('hostRemove', h);
  });

  // Add status listeners for existing hosts
  this.hosts.forEach(function (h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
  });
};

/**
 * Sets the distance to each host and when warmup is true, creates all connections to local hosts.
 * @returns {Promise}
 * @private
 */
Client.prototype._warmup = function () {
  const hosts = this.hosts.values();

  return promiseUtils.times(hosts.length, warmupLimit, async (index) => {
    const h = hosts[index];
    const distance = this.profileManager.getDistance(h);

    if (distance === types.distance.ignored) {
      return;
    }

    if (this.options.pooling.warmup && distance === types.distance.local) {
      try {
        await h.warmupPool(this.keyspace);
      } catch (err) {
        // An error while trying to create a connection to one of the hosts.
        // Warn the user and move on.
        this.log('warning', `Connection pool to host ${h.address} could not be created: ${err}`, err);
      }
    } else {
      h.initializePool();
    }
  });
};

/**
 * @returns {Encoder}
 * @private
 */
Client.prototype._getEncoder = function () {
  const encoder = this.controlConnection.getEncoder();
  if (!encoder) {
    throw new errors.DriverInternalError('Encoder is not defined');
  }
  return encoder;
};

/**
 * Returns a BatchRequest instance and fills the routing key information in the provided options.
 * @private
 */
Client.prototype._createBatchRequest = async function (queryItems, info) {
  const firstQuery = queryItems[0];
  if (!firstQuery.meta) {
    return new requests.BatchRequest(queryItems, info);
  }

  await this._setRoutingInfo(info, firstQuery.params, firstQuery.meta);
  return new requests.BatchRequest(queryItems, info);
};

/**
 * Returns an ExecuteRequest instance and fills the routing key information in the provided options.
 * @private
 */
Client.prototype._createExecuteRequest = async function(query, queryId, info, params, meta) {
  params = utils.adaptNamedParamsPrepared(params, meta.columns);
  await this._setRoutingInfo(info, params, meta);
  return new requests.ExecuteRequest(query, queryId, params, info, meta);
};

/**
 * Returns a QueryRequest instance and fills the routing key information in the provided options.
 * @private
 */
Client.prototype._createQueryRequest = async function (query, execOptions, params) {
  await this.metadata.adaptUserHints(this.keyspace, execOptions.getHints());
  const paramsInfo = utils.adaptNamedParamsWithHints(params, execOptions);
  this._getEncoder().setRoutingKeyFromUser(paramsInfo.params, execOptions, paramsInfo.keyIndexes);

  return new requests.QueryRequest(query, paramsInfo.params, execOptions, paramsInfo.namedParameters);
};

/**
 * Sets the routing key based on the parameter values or the provided routing key components.
 * @param {ExecutionOptions} execOptions
 * @param {Array} params
 * @param meta
 * @private
 */
Client.prototype._setRoutingInfo = async function (execOptions, params, meta) {
  const encoder = this._getEncoder();

  if (!execOptions.getKeyspace() && meta.keyspace) {
    execOptions.setKeyspace(meta.keyspace);
  }
  if (execOptions.getRoutingKey()) {
    // Routing information provided by the user
    return encoder.setRoutingKeyFromUser(params, execOptions);
  }
  if (Array.isArray(meta.partitionKeys)) {
    // The partition keys are provided as part of the metadata for modern protocol versions
    execOptions.setRoutingIndexes(meta.partitionKeys);
    return encoder.setRoutingKeyFromMeta(meta, params, execOptions);
  }

  // Older versions of the protocol (v3 and below) don't provide routing information
  try {
    const tableInfo = await this.metadata.getTable(meta.keyspace, meta.table);

    if (!tableInfo) {
      // The schema data is not there, maybe it is being recreated, avoid setting the routing information
      return;
    }

    execOptions.setRoutingIndexes(tableInfo.partitionKeys.map(c => meta.columnsByName[c.name]));
    // Skip parsing metadata next time
    meta.partitionKeys = execOptions.getRoutingIndexes();
    encoder.setRoutingKeyFromMeta(meta, params, execOptions);
  } catch (err) {
    this.log('warning', util.format('Table %s.%s metadata could not be retrieved', meta.keyspace, meta.table));
  }
};

/**
 * Callback used by execution methods.
 * @callback ResultCallback
 * @param {Error} err Error occurred in the execution of the query.
 * @param {ResultSet} [result] Result of the execution of the query.
 */

module.exports = Client;
