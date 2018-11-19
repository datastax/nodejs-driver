"use strict";
const events = require('events');
const util = require('util');

const utils = require('./utils.js');
const errors = require('./errors.js');
const types = require('./types');
const ProfileManager = require('./execution-profile').ProfileManager;
const requests = require('./requests');
const clientOptions = require('./client-options');
const ClientState = require('./metadata/client-state');
const description = require('../package.json').description;
const version = require('../package.json').version;
const DefaultExecutionOptions = require('./execution-options').DefaultExecutionOptions;

// Allow injection of the following modules
/* eslint-disable prefer-const */
let ControlConnection = require('./control-connection');
let RequestHandler = require('./request-handler');
let PrepareHandler = require('./prepare-handler');
/* eslint-enable prefer-const */

/**
 * Max amount of pools being warmup in parallel, when warmup is enabled
 * @private
 */
const warmupLimit = 32;


/**
 * Client options
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
 * @property {String} keyspace The logged keyspace for all the connections created within the {@link Client} instance.
 * @property {Number} refreshSchemaDelay The default window size in milliseconds used to debounce node list and schema
 * refresh metadata requests. Default: 1000.
 * @property {Boolean} isMetadataSyncEnabled Determines whether client-side schema metadata retrieval and update is
 * enabled.
 * <p>Setting this value to <code>false</code> will cause keyspace information not to be automatically loaded, affecting
 * replica calculation per token in the different keyspaces. When disabling metadata synchronization, use
 * [Metadata.refreshKeyspaces()]{@link module:metadata~Metadata#refreshKeyspaces} to keep keyspace information up to
 * date or token-awareness will not work correctly.</p>
 * Default: <code>true</code>.
 * @property {Boolean} prepareOnAllHosts Determines if the driver should prepare queries on all hosts in the cluster.
 * Default: <code>true</code>.
 * @property {Boolean} rePrepareOnUp Determines if the driver should re-prepare all cached prepared queries on a
 * host when it marks it back up.
 * Default: <code>true</code>.
 * @property {Number} maxPrepared Determines the maximum amount of different prepared queries before evicting items
 * from the internal cache. Reaching a high threshold hints that the queries are not being reused, like when
 * hard-coding parameter values inside the queries.
 * Default: <code>500</code>.
 * @property {Object} policies
 * @property {LoadBalancingPolicy} policies.loadBalancing The load balancing policy instance to be used to determine
 * the coordinator per query.
 * @property {RetryPolicy} policies.retry The retry policy.
 * @property {ReconnectionPolicy} policies.reconnection The reconnection policy to be used.
 * @property {AddressTranslator} policies.addressResolution The address resolution policy.
 * @property {SpeculativeExecutionPolicy} policies.speculativeExecution The <code>SpeculativeExecutionPolicy</code>
 * instance to be used to determine if the client should send speculative queries when the selected host takes more
 * time than expected.
 * <p>
 *   Default: <code>[NoSpeculativeExecutionPolicy]{@link
  *   module:policies/speculativeExecution~NoSpeculativeExecutionPolicy}</code>
 * </p>
 * @property {TimestampGenerator} policies.timestampGeneration The client-side
 * [query timestamp generator]{@link module:policies/timestampGeneration~TimestampGenerator}.
 * <p>
 *   Default: <code>[MonotonicTimestampGenerator]{@link module:policies/timestampGeneration~MonotonicTimestampGenerator}
 *   </code>
 * </p>
 * <p>Use <code>null</code> to disable client-side timestamp generation.</p>
 * @property {QueryOptions} queryOptions Default options for all queries.
 * @property {Object} pooling Pooling options.
 * @property {Number} pooling.heartBeatInterval The amount of idle time in milliseconds that has to pass before the
 * driver issues a request on an active connection to avoid idle time disconnections. Default: 30000.
 * @property {Object} pooling.coreConnectionsPerHost Associative array containing amount of connections per host
 * distance.
 * @property {Number} pooling.maxRequestsPerConnection The maximum number of requests per connection. The default
 * value is:
 * <ul>
 *   <li>For modern protocol versions (v3 and above): 2048</li>
 *   <li>For older protocol versions (v1 and v2): 128</li>
 * </ul>
 * @property {Boolean} pooling.warmup Determines if all connections to hosts in the local datacenter must be opened on
 * connect. Default: true.
 * @property {Object} protocolOptions
 * @property {Number} protocolOptions.port The port to use to connect to the Cassandra host. If not set through this
 * method, the default port (9042) will be used instead.
 * @property {Number} protocolOptions.maxSchemaAgreementWaitSeconds The maximum time in seconds to wait for schema
 * agreement between nodes before returning from a DDL query. Default: 10.
 * @property {Number} protocolOptions.maxVersion When set, it limits the maximum protocol version used to connect to
 * the nodes.
 * Useful for using the driver against a cluster that contains nodes with different major/minor versions of Cassandra.
 * @property {Boolean} protocolOptions.noCompact When set to true, enables the NO_COMPACT startup option.
 * <p>
 * When this option is supplied <code>SELECT</code>, <code>UPDATE</code>, <code>DELETE</code>, and <code>BATCH</code>
 * statements on <code>COMPACT STORAGE</code> tables function in "compatibility" mode which allows seeing these tables
 * as if they were "regular" CQL tables.
 * </p>
 * <p>
 * This option only effects interactions with interactions with tables using <code>COMPACT STORAGE</code> and is only
 * supported by C* 3.0.16+, 3.11.2+, 4.0+ and DSE 6.0+.
 * </p>
 * @property {Object} socketOptions
 * @property {Number} socketOptions.connectTimeout Connection timeout in milliseconds. Default: 5000.
 * @property {Number} socketOptions.defunctReadTimeoutThreshold Determines the amount of requests that simultaneously
 * have to timeout before closing the connection. Default: 64.
 * @property {Boolean} socketOptions.keepAlive Whether to enable TCP keep-alive on the socket. Default: true.
 * @property {Number} socketOptions.keepAliveDelay TCP keep-alive delay in milliseconds. Default: 0.
 * @property {Number} socketOptions.readTimeout Per-host read timeout in milliseconds.
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
 * @property {Boolean} socketOptions.tcpNoDelay When set to true, it disables the Nagle algorithm. Default: true.
 * @property {Number} socketOptions.coalescingThreshold Buffer length in bytes use by the write queue before flushing
 * the frames. Default: 8000.
 * @property {AuthProvider} authProvider Provider to be used to authenticate to an auth-enabled cluster.
 * @property {RequestTracker} requestTracker The instance of RequestTracker used to monitor or log requests executed
 * with this instance.
 * @property {Object} sslOptions Client-to-node ssl options. When set the driver will use the secure layer.
 * You can specify cert, ca, ... options named after the Node.js <code>tls.connect()</code> options.
 * <p>
 *   It uses the same default values as Node.js <code>tls.connect()</code> except for <code>rejectUnauthorized</code>
 *   which is set to <code>false</code> by default (for historical reasons). This setting is likely to change
 *   in upcoming versions to enable validation by default.
 * </p>
 * @property {Object} encoding
 * @property {Function} encoding.map Map constructor to use for Cassandra map<k,v> type encoding and decoding.
 * If not set, it will default to Javascript Object with map keys as property names.
 * @property {Function} encoding.set Set constructor to use for Cassandra set<k> type encoding and decoding.
 * If not set, it will default to Javascript Array.
 * @property {Boolean} encoding.copyBuffer Determines if the network buffer should be copied for buffer based data
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
 * @property {Boolean} encoding.useUndefinedAsUnset Valid for Cassandra 2.2 and above. Determines that, if a parameter
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
 * @property {Boolean} encoding.useBigIntAsLong Use [BigInt ECMAScript type](https://tc39.github.io/proposal-bigint/)
 * to represent CQL bigint and counter data types.
 * @property {Boolean} encoding.useBigIntAsVarint Use [BigInt ECMAScript type](https://tc39.github.io/proposal-bigint/)
 * to represent CQL varint data type.
 * @property {Array.<ExecutionProfile>} profiles The array of [execution profiles]{@link ExecutionProfile}.
 * @property {Function} promiseFactory Function to be used to create a <code>Promise</code> from a
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
 *   [paging results documentation]{@link http://docs.datastax.com/en/developer/nodejs-driver/latest/features/paging/}.
 * </p>
 * @property {Boolean} [captureStackTrace] Determines if the stack trace before the query execution should be
 * maintained.
 * <p>
 *   Useful for debugging purposes, it should be set to <code>false</code> under production environment as it adds an
 *   unnecessary overhead to each execution.
 * </p>
 * Default: false.
 * @property {Number} [consistency] [Consistency level]{@link module:types~consistencies}. Default: localOne.
 * @property {Object} [customPayload] Key-value payload to be passed to the server. On the Cassandra side, 
 * implementations of QueryHandler can use this data.
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
 * @property {String} [keyspace] Specifies the keyspace for the query. Used for routing within the driver, this
 * property is suitable when the query operates on a different keyspace than the current {@link Client#keyspace}.
 * <p>
 *   This property should only be set manually by the user when the query operates on a different keyspace than
 *   the current {@link Client#keyspace} and using either batch or non-prepared query executions.
 * </p>
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
 */

/**
 * Creates a new instance of {@link Client}.
 * @classdesc
 * A Client holds connections to a Cassandra cluster, allowing it to be queried.
 * Each Client instance maintains multiple connections to the cluster nodes,
 * provides [policies]{@link module:policies} to choose which node to use for each query,
 * and handles [retries]{@link module:policies/retry} for failed query (when it makes sense), etc...
 * <p>
 * Client instances are designed to be long-lived and usually a single instance is enough
 * per application. As a given Client can only be "logged" into one keyspace at
 * a time (where the "logged" keyspace is the one used by query if the query doesn't
 * explicitly use a fully qualified table name), it can make sense to create one
 * client per keyspace used. This is however not necessary to query multiple keyspaces
 * since it is always possible to use a single session with fully qualified table name
 * in queries.
 * </p>
 * @extends EventEmitter
 * @param {ClientOptions} options The options for this instance.
 * @example <caption>Creating a new client instance</caption>
 * const client = new Client({ contactPoints: ['192.168.1.100'], localDataCenter: 'datacenter1' });
 * client.connect(function (err) {
 *   if (err) return console.error(err);
 *   console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
 * });
 * @example <caption>Executing a query</caption>
 * // calling #execute() can be made without previously calling #connect(), as internally
 * // it will ensure it's connected before attempting to execute the query
 * client.execute('SELECT key FROM system.local', function (err, result) {
 *   if (err) return console.error(err);
 *   const row = result.first();
 *   console.log(row['key']);
 * });
 * @example <caption>Executing a query with promise-based API</caption>
 * const result = await client.execute('SELECT key FROM system.local');
 * const row = result.first();
 * console.log(row['key']);
 * @constructor
 */
function Client(options) {
  events.EventEmitter.call(this);
  this.options = clientOptions.extend({ logEmitter: this.emit.bind(this) }, options);
  Object.defineProperty(this, 'profileManager', { value: new ProfileManager(this.options) });
  Object.defineProperty(this, 'controlConnection', {
    value: new ControlConnection(this.options, this.profileManager), writable: true }
  );
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
 * Tries to connect to one of the [contactPoints]{@link ClientOptions} and discovers the rest the nodes of the cluster.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the client is connected. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * <p>
 *   If the {@link Client} is already connected, it invokes callback immediately (when provided) or the promise is
 *   fulfilled .
 * </p>
 * @example <caption>Callback-based execution</caption>
 * client.connect(function (err) {
 *   if (err) return console.error(err);
 *   console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
 * });
 * @example <caption>Promise-based execution</caption>
 * await client.connect();
 * @param {function} [callback] The callback is invoked when the pool is connected it failed to connect.
 */
Client.prototype.connect = function (callback) {
  return utils.promiseWrapper.call(this, this.options, callback, this._connectCb);
};

/**
 * @param {Function} callback
 * @private
 */
Client.prototype._connectCb = function (callback) {
  if (this.connected) {
    return callback();
  }
  if (this.isShuttingDown) {
    //it is being shutdown, don't allow further calls to connect()
    return callback(new errors.NoHostAvailableError(null, 'Connecting after shutdown is not supported'));
  }
  this.once('connected', callback);
  if (this.connecting) {
    //the listener to connect was added, move on
    return;
  }
  this.connecting = true;
  const self = this;
  this.log('info', util.format("Connecting to cluster using '%s' version %s", description, version));
  utils.series([
    function initControlConnection(next) {
      self.controlConnection.init(next);
    },
    function initLoadBalancingPolicy(next) {
      self.hosts = self.controlConnection.hosts;
      self.profileManager.init(self, self.hosts, next);
    },
    function setKeyspace(next) {
      if (!self.keyspace) {
        return next();
      }
      RequestHandler.setKeyspace(self, next);
    },
    function setPoolOptionsAndWarmup(next) {
      clientOptions.setProtocolDependentDefaults(self.options, self.controlConnection.protocolVersion);

      if (!self.options.pooling.warmup) {
        return next();
      }
      self._warmup(next);
    }
  ], function connectFinished(err) {
    if (err) {
      // We should close the pools (if any) and reset the state to allow successive calls to connect()
      return self.controlConnection.reset(function () {
        self.connected = false;
        self.connecting = false;
        self.emit('connected', err);
      });
    }
    self._setHostListeners();
    // Set the distance of the control connection host relatively to this instance
    self.profileManager.getDistance(self.controlConnection.host);
    self.connected = true;
    self.connecting = false;
    self.emit('connected');
  });
};

/**
 * Executes a query on an available connection.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the execution completes. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * <p>The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag.</p>
 * <p>
 *   Some executions failures can be handled transparently by the driver, according to the
 *   [RetryPolicy]{@link module:policies/retry~RetryPolicy} defined at {@link ClientOptions} or {@link QueryOptions}
 *   level.
 * </p>
 * @param {String} query The query to execute.
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value.
 * @param {QueryOptions} [options] The query options for the execution.
 * @param {ResultCallback} [callback] Executes callback(err, result) when execution completed. When not defined, the
 * method will return a promise.
 * @example <caption>Callback-based API</caption>
 * const query = 'SELECT name, email FROM users WHERE id = ?';
 * client.execute(query, [ id ], { prepare: true }, function (err, result) {
 *   assert.ifError(err);
 *   const row = result.first();
 *   console.log('%s: %s', row.name, row.email);
 * });
 * @example <caption>Promise-based API, using async/await</caption>
 * const query = 'SELECT name, email FROM users WHERE id = ?';
 * const result = await client.execute(query, [ id ], { prepare: true });
 * const row = result.first();
 * console.log('%s: %s', row.name, row.email);
 * @see {@link ExecutionProfile} to reuse a set of options across different query executions.
 */
Client.prototype.execute = function (query, params, options, callback) {
  // set default argument values for optional parameters
  callback = callback || (options ? options : params);
  if (typeof callback === 'function') {
    params = typeof params !== 'function' ? params : null;
  }
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    let execOptions;
    try {
      execOptions = DefaultExecutionOptions.create(options, this);
    }
    catch (e) {
      return cb(e);
    }

    this._innerExecute(query, params, execOptions, cb);
  });
};

/**
 * Executes the query and calls rowCallback for each row as soon as they are received. Calls final callback after all
 * rows have been sent, or when there is an error.
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
 *   hosts if needed.
 * </p>
 * @param {String} query The query to execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value.
 * @param {QueryOptions} [options]
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

  const self = this;
  let rowLength = 0;

  function nextPage() {
    self._innerExecute(query, params, execOptions, pageCallback);
  }

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

  this._innerExecute(query, params, execOptions, pageCallback);
};

/**
 * Executes the query and pushes the rows to the result stream
 *  as soon as they received.
 * Calls callback after all rows have been sent, or when there is an error.
 * <p>
 * The stream is a [Readable Streams2]{@link http://nodejs.org/api/stream.html#stream_class_stream_readable} object
 *  that contains the raw bytes of the field value.
 *  It can be piped downstream and provides automatic pause/resume logic (it buffers when not read).
 * </p>
 * <p>
 *   The query can be prepared (recommended) or not depending on {@link QueryOptions}.prepare flag. Retries on multiple
 *   hosts if needed.
 * </p>
 * @param {String} query The query to prepare and execute
 * @param {Array|Object} [params] Array of parameter values or an associative array (object) containing parameter names
 * as keys and its value
 * @param {QueryOptions} [options]
 * @param {function} [callback], executes callback(err) after all rows have been received or if there is an error
 * @returns {types.ResultStream}
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
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the execution completes. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * @param {Array.<string>|Array.<{query, params}>} queries The queries to execute as an Array of strings or as an array
 * of object containing the query and params
 * @param {QueryOptions} [options]
 * @param {ResultCallback} [callback] Executes callback(err, result) when the batch was executed
 */
Client.prototype.batch = function (queries, options, callback) {
  callback = callback || options;
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    this._batchCb(queries, options, cb);
  });
};

/**
 * @param {Array.<string>|Array.<{query, params}>}queries
 * @param {QueryOptions} options
 * @param {ResultCallback} callback
 * @private
 */
Client.prototype._batchCb = function (queries, options, callback) {
  if (!Array.isArray(queries)) {
    // We should throw (not callback) for an unexpected type
    throw new errors.ArgumentError('Queries should be an Array');
  }
  if (queries.length === 0) {
    return callback(new errors.ArgumentError('Queries array can not be empty'));
  }

  let execOptions;
  try {
    execOptions = DefaultExecutionOptions.create(options, this);
  } catch (e) {
    return callback(e);
  }

  let queryItems;
  let request;

  utils.series([
    next => this.connect(next),
    next => {
      if (execOptions.isPrepared()) {
        return PrepareHandler.getPreparedMultiple(
          this, execOptions.getLoadBalancingPolicy(), queries, this.keyspace, function(err, result) {
            queryItems = result;
            next(err);
          });
      }
      queryItems = new Array(queries.length);
      for (let i = 0; i < queries.length; i++) {
        const item = queries[i];
        if (!item) {
          return next(new errors.ArgumentError(util.format('Invalid query at index %d', i)));
        }
        const query = typeof item === 'string' ? item : item.query;
        if (!query) {
          return next(errors.ArgumentError(util.format('Invalid query at index %d', i)));
        }
        queryItems[i] = { query: query, params: item.params };
      }
      next();
    },
    next => this._createBatchRequest(queryItems, execOptions, (err, r) => {
      request = r;
      next(err);
    }),
    next => RequestHandler.send(request, execOptions, this, next)
  ], callback);
};

/**
 * Gets the host list representing the replicas that contain such partition.
 * @param {String} keyspace
 * @param {Buffer} token
 * @returns {Array}
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
 * @return module:metadata~ClientState
 */
Client.prototype.getState = function () {
  return ClientState.from(this);
};

Client.prototype.log = utils.log;

/**
 * Closes all connections to all hosts.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the client is disconnected. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
 * @param {Function} [callback] Optional callback to be invoked when finished closing all connections.
 */
Client.prototype.shutdown = function (callback) {
  return utils.promiseWrapper.call(this, this.options, callback, this._shutdownCb);
};

/**
 * @param {Function} callback
 * @private
 */
Client.prototype._shutdownCb = function (callback) {
  const self = this;
  function doShutdown() {
    self.connected = false;
    self.isShuttingDown = true;
    const hosts = self.hosts.values();
    // Shutdown the ControlConnection before shutting down the pools
    self.controlConnection.shutdown();
    self.options.policies.speculativeExecution.shutdown();
    if (self.options.requestTracker) {
      self.options.requestTracker.shutdown();
    }
    // go through all the host and shut down their pools
    utils.each(hosts, (h, next) => h.shutdown(false, next), callback);
  }
  this.log('info', 'Shutting down');
  callback = callback || utils.noop;
  if (!this.hosts || !this.connected) {
    // not initialized
    this.connected = false;
    return callback();
  }
  if (this.connecting) {
    this.log('warning', 'Shutting down while connecting');
    // wait until finish connecting for easier troubleshooting
    return this.once('connected', doShutdown);
  }
  doShutdown();
};

/**
 * Waits until that the schema version in all nodes is the same or the waiting time passed.
 * @param {Connection} connection
 * @param {Function} callback
 * @ignore
 */
Client.prototype._waitForSchemaAgreement = function (connection, callback) {
  if (this.hosts.length === 1) {
    return setImmediate(() => callback(null, true));
  }

  const start = process.hrtime();
  const maxWaitSeconds = this.options.protocolOptions.maxSchemaAgreementWaitSeconds;

  this.log('info', 'Waiting for schema agreement');

  let versionsMatch;

  utils.whilst(
    () => !versionsMatch && process.hrtime(start)[0] < maxWaitSeconds,
    next => {
      this.metadata.compareSchemaVersions(connection, (err, agreement) => {
        if (err) {
          return next(err);
        }

        versionsMatch = agreement;

        if (versionsMatch) {
          this.log('info', 'Schema versions match');
          return next();
        }

        // Let some time pass before the next check
        setTimeout(next, 500);
      });
    },
    (err) => callback(err, versionsMatch));
};

/**
 * Waits for schema agreements and schedules schema metadata refresh.
 * @param {Connection} connection
 * @param event
 * @param {Function} callback
 * @ignore
 * @internal
 */
Client.prototype.handleSchemaAgreementAndRefresh = function (connection, event, callback) {
  this._waitForSchemaAgreement(connection, (err, agreement) => {
    if (err) {
      //we issue a warning but we continue with the normal flow
      this.log('warning', 'There was an error while waiting for the schema agreement between nodes', err);
    }
    if (!this.options.isMetadataSyncEnabled) {
      return callback(agreement);
    }

    // schedule metadata refresh immediately and the callback will be invoked once it was refreshed
    this.controlConnection.handleSchemaChange(event, true, (err) => {
      if (err) {
        this.log('warning', 'There was an error while handling schema change', err);
      }
      callback(agreement);
    });
  });
};

/**
 * Connects and handles the execution of prepared and simple statements. All parameters are mandatory.
 * @param {string} query
 * @param {Array} params
 * @param {ExecutionOptions} execOptions
 * @param {Function} callback
 * @private
 */
Client.prototype._innerExecute = function (query, params, execOptions, callback) {
  const version = this.controlConnection.protocolVersion;

  if (!execOptions.isPrepared() && params && !util.isArray(params) && !types.protocolVersion.supportsNamedParameters(version)) {
    // Only Cassandra 2.1 and above supports named parameters
    return callback(
      new errors.ArgumentError('Named parameters for simple statements are not supported, use prepare flag'));
  }

  let request;
  utils.series([
    next => this.connect(next),
    next => {
      if (!execOptions.isPrepared()) {
        return this._createQueryRequest(query, execOptions, params, (err, r) => {
          request = r;
          next(err);
        });
      }

      const lbp = execOptions.getLoadBalancingPolicy();
      PrepareHandler.getPrepared(this, lbp, query, this.keyspace, (err, queryId, meta) => {
        if (err) {
          return next(err);
        }
        this._createExecuteRequest(query, queryId, execOptions, params, meta, (err, r) => {
          request = r;
          next(err);
        });
      });
    },
    next => RequestHandler.send(request, execOptions, this, next)
  ], callback);
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
  //Add status listeners when new nodes are added and emit hostAdd
  this.hosts.on('add', function hostAddedListener(h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
    self.emit('hostAdd', h);
  });
  //Remove all listeners and emit hostRemove
  this.hosts.on('remove', function hostRemovedListener(h) {
    h.removeAllListeners();
    self.emit('hostRemove', h);
  });
  //Add status listeners for existing hosts
  this.hosts.forEach(function (h) {
    h.on('up', getHostUpListener(self, h));
    h.on('down', getHostDownListener(self, h));
  });
};

Client.prototype._warmup = function (callback) {
  const self = this;
  const hosts = this.hosts.values();
  utils.timesLimit(hosts.length, warmupLimit, function warmupEachCallback(i, next) {
    const h = hosts[i];
    const distance = self.profileManager.getDistance(h);
    if (distance !== types.distance.local) {
      //do not warmup pool for remote or ignored hosts
      return next();
    }
    h.warmupPool(function (err) {
      if (err) {
        //An error while trying to create a connection
        //To 1 host is not an issue, warn the user and move on
        self.log('warning', util.format('Connection pool to host %s could not be created: %s', h.address, err));
      }
      next();
    });
  }, callback);
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
Client.prototype._createBatchRequest = function (queryItems, info, callback) {
  const firstQuery = queryItems[0];
  if (!firstQuery.meta) {
    return callback(null, new requests.BatchRequest(queryItems, info));
  }

  this._setRoutingInfo(info, firstQuery.params, firstQuery.meta, function (err) {
    if (err) {
      return callback(err);
    }
    callback(null, new requests.BatchRequest(queryItems, info));
  });
};

/**
 * Returns an ExecuteRequest instance and fills the routing key information in the provided options.
 * @private
 */
Client.prototype._createExecuteRequest = function(query, queryId, info, params, meta, callback) {
  try {
    params = utils.adaptNamedParamsPrepared(params, meta.columns);
  }
  catch (err) {
    return callback(err);
  }

  this._setRoutingInfo(info, params, meta, err => {
    if (err) {
      return callback(err);
    }
    callback(null, new requests.ExecuteRequest(query, queryId, params, info, meta));
  });
};

/**
 * Returns a QueryRequest instance and fills the routing key information in the provided options.
 * @private
 */
Client.prototype._createQueryRequest = function (query, execOptions, params, callback) {
  this.metadata.adaptUserHints(this.keyspace, execOptions.getHints(), (err) => {
    if (err) {
      return callback(err);
    }

    let paramsInfo;
    try {
      paramsInfo = utils.adaptNamedParamsWithHints(params, execOptions);
      this._getEncoder().setRoutingKeyFromUser(paramsInfo.params, execOptions, paramsInfo.keyIndexes);
    } catch (err) {
      return callback(err);
    }

    callback(null, new requests.QueryRequest(query, paramsInfo.params, execOptions, paramsInfo.namedParameters));
  });
};

/**
 * Sets the routing key based on the parameter values or the provided routing key components.
 * @param {ExecutionOptions} execOptions
 * @param {Array} params
 * @param meta
 * @param {Function} callback
 * @private
 */
Client.prototype._setRoutingInfo = function (execOptions, params, meta, callback) {
  const self = this;

  /** Wrapper function as encoding a routing key could throw a TypeError */
  function encodeRoutingKey(fromUser) {
    const encoder = self._getEncoder();
    try {
      if (fromUser) {
        encoder.setRoutingKeyFromUser(params, execOptions);
      } else {
        encoder.setRoutingKeyFromMeta(meta, params, execOptions);
      }
    }
    catch (err) {
      return callback(err);
    }
    callback();
  }

  if (!execOptions.getKeyspace() && meta.keyspace) {
    execOptions.setKeyspace(meta.keyspace);
  }
  if (execOptions.getRoutingKey()) {
    // Routing information provided by the user
    return encodeRoutingKey(true);
  }
  if (Array.isArray(meta.partitionKeys)) {
    // The partition keys are provided as part of the metadata for modern protocol versions
    execOptions.setRoutingIndexes(meta.partitionKeys);
    return encodeRoutingKey();
  }

  // Older versions of the protocol (v3 and below) don't provide routing information
  this.metadata.getTable(meta.keyspace, meta.table, (err, tableInfo) => {
    if (err) {
      this.log('warning', util.format('Table %s.%s metadata could not be retrieved', meta.keyspace, meta.table));
      return callback();
    }
    if (!tableInfo) {
      // The schema data is not there, maybe it is being recreated, avoid setting the routing information
      return callback();
    }
    execOptions.setRoutingIndexes(tableInfo.partitionKeys.map(c => meta.columnsByName[c.name]));
    // Skip parsing metadata next time
    meta.partitionKeys = execOptions.getRoutingIndexes();
    encodeRoutingKey();
  });
};

/**
 * Callback used by execution methods.
 * @callback ResultCallback
 * @param {Error} err Error occurred in the execution of the query.
 * @param {ResultSet} [result] Result of the execution of the query.
 */

module.exports = Client;
