/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const util = require('util');
const BaseClient = require('./client');
const GraphResultSet = require('./graph/result-set');
const encoderExtensions = require('./encoder-extensions');
const errors = require('./errors');
const utils = require('./utils');
const version = require('../package.json').version;
const policies = require('./policies');
const GraphExecutionOptions = require('./graph/options').GraphExecutionOptions;
const Encoder = require('./encoder');

const graphLanguageBytecode = 'bytecode-json';
const graphLanguageGroovyString = 'gremlin-groovy';

/**
 * Creates a new {@link Client} instance.
 * @classdesc
 * Extends the
 * [Client]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/Client.html} class of the
 * [DataStax Node.js Driver for Apache Cassandra]{@link https://github.com/datastax/nodejs-driver}
 * to provide DSE-specific features support such as Graph, geospatial types representation and authentication, in
 * addition to the inherited <code>Client</code> methods used to execute CQL queries (<code>execute()</code>,
 * <code>eachRow()</code>, <code>stream()</code>).
 * <p>
 *   {@link Client} instances are designed to be long-lived and usually a single instance is enough per application.
 * </p>
 * @param {DseClientOptions} options The options used to create the client instance.
 * @example <caption>Creating a new client instance</caption>
 * const dse = require('dse-driver');
 * const client = new dse.Client({
 *   contactPoints: [ 'host1', 'host2' ]
 * });
 * @example <caption>Connecting to the cluster</caption>
 * const client = new dse.Client({ contactPoints: [ 'host1', 'host2' ] });
 * client.connect().then(function () {
 *   console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
 * });
 * @example <caption>Executing a query with the promise-based API and async/await</caption>
 * // calling #execute() can be made without previously calling #connect(), as internally
 * // it will ensure it's connected before attempting to execute the query
 * const result = await client.execute('SELECT key FROM system.local');
 * const row = result.first();
 * console.log(row['key']);
 * @example <caption>Executing a query using callbacks</caption>
 * client.execute('SELECT key FROM system.local', function (err, result) {
 *   if (err) return console.error(err);
 *   const row = result.first();
 *   console.log(row['key']);
 * });
 * @constructor
 * @extends {cassandra.Client}
 */
function Client(options) {
  if (!options) {
    throw new errors.ArgumentError('You must provide a parameter with the Client options');
  }
  encoderExtensions.register(Encoder);
  options = utils.extend({}, options);
  // Retrieve the retry policy for the default profile to determine if it was specified
  this._defaultProfileRetryPolicy = getDefaultProfileRetryPolicy(options);
  // Set the DSE load-balancing policy as default
  options.policies = utils.extend({ loadBalancing: policies.defaultLoadBalancingPolicy() }, options.policies);
  BaseClient.call(this, options);
  // Use graphBaseOptions as a way to gather all defaults that affect graph executions
  this._graphBaseOptions = utils.extend({
    executeAs: this.options.queryOptions.executeAs,
    language: graphLanguageGroovyString,
    source: 'g',
    readTimeout: 0,
    // As the default retry policy might retry non-idempotent queries
    // we should use default retry policy for all graph queries that does not retry
    retry: new policies.retry.FallthroughRetryPolicy()
  }, options.graphOptions, this.profileManager.getDefault().graphOptions);

  if (this._graphBaseOptions.readTimeout === null) {
    this._graphBaseOptions.readTimeout = this.options.socketOptions.readTimeout;
  }
  this._graphSONReader = new encoderExtensions.GraphSONReader();
}

/**
 * DSE client options that extends DataStax driver
 * [ClientOptions]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/global.html#ClientOptions}.
 * @typedef {ClientOptions} DseClientOptions
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
 * [consistency level]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies}
 * defined in the query options for graph read queries.
 * @property {Number} [graphOptions.readTimeout] Overrides the default per-host read timeout (in milliseconds) for all
 * graph queries. Default: <code>0</code>.
 * <p>
 *   Use <code>null</code> to reset the value and use the default on <code>socketOptions.readTimeout</code> .
 * </p>
 * @property {String} [graphOptions.source] The graph traversal source name to use in graph queries. Default:
 * <code>'g'</code>.
 * @property {Number} [graphOptions.writeConsistency] Overrides the
 * [consistency level]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies}
 * defined in the query options for graph write queries.
 */

util.inherits(Client, BaseClient);

Client.prototype.connect = function (callback) {
  if (!this.connected && !this.connecting && !this.isShuttingDown) {
    this.log('info', util.format('Using DSE driver v%s', version));
  }
  return BaseClient.prototype.connect.call(this, callback);
};

/**
 * Executes a graph query.
 * <p>
 *   If a <code>callback</code> is provided, it will invoke the callback when the execution completes. Otherwise,
 *   it will return a <code>Promise</code>.
 * </p>
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
 * const result = await client.executeGraph('g.V()', function (err, result) {
 *   const vertex = result.first();
 *   console.log(vertex.label);
 * });
 * @example <caption>Using result.forEach()</caption>
 * const result = await client.executeGraph('g.V().hasLabel("person")');
 * result.forEach(function(vertex) {
 *   console.log(vertex.type); // vertex
 *   console.log(vertex.label); // person
 * });
 * @example <caption>Using ES6 for...of</caption>
 * const result = await client.executeGraph('g.E()');
 * for (let edge of result) {
 *   console.log(edge.label); // created
 * });
 * @see {@link ExecutionProfile} to reuse a set of options across different query executions.
 */
Client.prototype.executeGraph = function (query, parameters, options, callback) {
  callback = callback || (options ? options : parameters);
  if (typeof callback === 'function') {
    parameters = typeof parameters !== 'function' ? parameters : null;
  }
  return utils.promiseWrapper.call(this, this.options, callback, function handler(cb) {
    this._executeGraphCb(query, parameters, options, cb);
  });
};

/**
 *
 * @param {String} query
 * @param {Object|null} parameters
 * @param {GraphQueryOptions|null} options
 * @param {Function} callback
 * @private
 */
Client.prototype._executeGraphCb = function (query, parameters, options, callback) {
  if (util.isArray(parameters)) {
    return callback(new TypeError('Parameters must be a Object instance as an associative array'));
  }
  parameters = this._setGraphParameters(parameters);

  let execOptions;
  try {
    execOptions = new GraphExecutionOptions(options, this, this._graphBaseOptions, this._defaultProfileRetryPolicy);
  }
  catch (e) {
    return callback(e);
  }

  if (execOptions.getGraphSource() !== 'a') {
    return this._executeGraphQuery(query, parameters, execOptions, callback);
  }

  const self = this;
  this._getAnalyticsMaster(function getPreferredHostCallback(host) {
    execOptions.setPreferredHost(host);
    return self._executeGraphQuery(query, parameters, execOptions, callback);
  });
};

/**
 * @param {String} query
 * @param {Object} parameters
 * @param {GraphExecutionOptions} execOptions
 * @param {Function} callback
 * @private
 */
Client.prototype._executeGraphQuery = function (query, parameters, execOptions, callback) {
  const self = this;
  this._innerExecute(query, parameters, execOptions, function (err, result) {
    if (err) {
      return callback(err);
    }

    let rowParser = null;

    if (execOptions.getGraphLanguage() === graphLanguageBytecode) {
      rowParser = function graphSONRowParser(row) {
        return self._graphSONReader.read(JSON.parse(row['gremlin']));
      };
    }
    callback(null, new GraphResultSet(result, rowParser));
  });
};

/**
 * @param {Function} callback Function that gets called with the preferred host (no error parameter).
 * @private
 */
Client.prototype._getAnalyticsMaster = function (callback) {
  const self = this;
  this.execute('CALL DseClientTool.getAnalyticsGraphServer()', null, null, function (err, result) {
    if (err) {
      self.log('verbose', 'Error querying graph analytics server, query will not be routed optimally', err);
      return callback(null);
    }
    if (result.rows.length === 0) {
      self.log('verbose', 'Empty response querying graph analytics server, query will not be routed optimally');
      return callback(null);
    }
    const resultField = result.rows[0]['result'];
    if (!resultField || !resultField['location']) {
      self.log('verbose',
        'Unexpected response querying graph analytics server, query will not be routed optimally',
        result.rows[0]);
      return callback(null);
    }
    const hostName = resultField['location'].substr(0, resultField['location'].lastIndexOf(':'));
    const addressTranslator = self.options.policies.addressResolution;
    addressTranslator.translate(hostName, self.options.protocolOptions.port, function translateCallback(endpoint) {
      callback(self.hosts.get(endpoint));
    });
  });
};

/**
 * Gets the retry policy for the default profile from the raw options
 * @private
 * @returns {RetryPolicy|null}
 */
function getDefaultProfileRetryPolicy(options) {
  if (!util.isArray(options.profiles)) {
    return null;
  }
  let retryPolicy = null;
  for (let i = 0; i < options.profiles.length; i++) {
    const profile = options.profiles[i];
    if (profile.name === 'default') {
      retryPolicy = profile.retry;
      break;
    }
  }
  return retryPolicy;
}

/**
 * @param parameters
 * @returns {Array.<String>}
 * @private
 */
Client.prototype._setGraphParameters = function (parameters) {
  if (!parameters || typeof parameters === 'function') {
    return null;
  }
  return [ JSON.stringify(parameters) ];
};

module.exports = Client;
