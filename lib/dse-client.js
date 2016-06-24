/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var BaseClient = cassandra.Client;
var GraphResultSet = require('./graph/result-set');
var encoderExtensions = require('./encoder-extensions');
var Long = cassandra.types.Long;
var utils = require('./utils');
var version = require('../package.json').version;
var policies = require('./policies');
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
 * @example
 * const dse = require('dse-driver');
 * const client = new dse.Client({
 *   contactPoints: ['h1', 'h2'],
 *   keyspace: 'ks1',
 *   graphOptions: { name: 'graph1' }
 * });
 * const query = 'SELECT email, last_name FROM users WHERE key=?';
 * client.execute(query, ['guy'], function(err, result) {
 *   assert.ifError(err);
 *   console.log('User email ' + result.first().email);
 * });
 * @constructor
 * @extends {Client}
 */
function Client(options) {
  if (!options) {
    throw new cassandra.errors.ArgumentError('You must provide a parameter with the Client options');
  }
  encoderExtensions.register(cassandra.Encoder);
  options = utils.extend({}, options);
  // Retrieve the retry policy for the default profile to determine if it was specified
  this._defaultProfileRetryPolicy = getDefaultProfileRetryPolicy(options);
  // Set the DSE load-balancing policy as default
  options.policies = utils.extend({ loadBalancing: policies.defaultLoadBalancingPolicy() }, options.policies);
  BaseClient.call(this, options);
  this._graphOptions = utils.extend({
    language: 'gremlin-groovy',
    source: 'g',
    readTimeout: 0,
    // As the default retry policy might retry non-idempotent queries
    // we should use default retry policy for all graph queries that does not retry
    retry: new policies.retry.FallthroughRetryPolicy()
  }, options.graphOptions, this.profileManager.getDefault().graphOptions);
  if (this._graphOptions.readTimeout === null) {
    this._graphOptions.readTimeout = this.options.socketOptions.readTimeout;
  }
}

/**
 * DSE client options that extends DataStax driver
 * [ClientOptions]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/global.html#ClientOptions}.
 * @typedef {ClientOptions} DseClientOptions
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

/**
 * Graph options that extends {@link QueryOptions}.
 * <p>
 *   Consider using [execution profiles]{@link ExecutionProfile} if you plan to reuse options across different
 *   query executions.
 * </p>
 * @typedef {QueryOptions} GraphQueryOptions
 * @property {String?} [graphLanguage] The graph language to use in graph queries.
 * @property {String?} [graphName] The graph name to be used in the query. You can use <code>null</code> to clear the
 * value from the <code>DseClientOptions</code> and execute a query without a default graph.
 * @property {Number?} [graphReadConsistency] Specifies the
 * [consistency level]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies}
 * to be used for the graph read queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the READ part of the graph query.
 * </p>
 * @property {String?} [graphSource] The graph traversal source name to use in graph queries.
 * @property {Number?} [graphWriteConsistency] Specifies the
 * [consistency level]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies} to
 * be used for the graph write queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the WRITE part of the graph query.
 * </p>
 * @property {RetryPolicy?} [retry] Sets the retry policy to be used for the graph query execution.
 * <p>
 *   When not specified in the {@link GraphQueryOptions} or in the {@link ExecutionProfile}, it will use by default
 *   a retry policy that does not retry graph executions.
 * </p>
 */

util.inherits(Client, BaseClient);

/**
 * Tries to connect to one of the contactPoints and discover the nodes of the cluster.
 * <p>
 *   If the {@link Client} is already connected, it immediately invokes callback.
 * </p>
 * @param {function} callback The callback that is invoked when a connection pool is created to at least one node
 * or it has failed to connect.
 */
Client.prototype.connect = function (callback) {
  if (!this.connected && !this.connecting && !this.isShuttingDown) {
    this.log('info', util.format('Using DSE driver v%s with core driver v%s', version, cassandra.version));
  }
  BaseClient.prototype.connect.call(this, callback);
};

/**
 * Executes a graph query.
 * @param {String} query The gremlin query.
 * @param {Object|null} [parameters] An associative array containing the key and values of the parameters.
 * @param {GraphQueryOptions|null} [options] The graph query options.
 * @param {Function} callback Function to execute when the response is retrieved, taking two arguments:
 * <code>err</code> and <code>result</code>.
 * @example
 * // Getting the first item (vertex, edge, scalar value, ...)
 * client.executeGraph('g.V()', function (err, result) {
 *   assert.ifError(err);
 *   const vertex = result.first();
 *   console.log(vertex.label);
 *  });
 * @example
 * // Using result.forEach()
 * client.executeGraph('g.V().hasLabel("person")', function (err, result) {
 *   assert.ifError(err);
 *   result.forEach(function(vertex) {
 *     console.log(vertex.type); // vertex
 *     console.log(vertex.label); // person
 *   });
 * });
 * @example
 * // Using ES6 for...of
 * client.executeGraph('g.E()', function (err, result) {
 *   assert.ifError(err);
 *   for (let edge of result) {
 *     console.log(edge.label); // created
 *     // ...
 *   });
 * });
 */
Client.prototype.executeGraph = function (query, parameters, options, callback) {
  callback = callback || (options ? options : parameters);
  if (util.isArray(parameters)) {
    return callback(new TypeError('Parameters must be a Object instance as an associative array'));
  }
  parameters = this._setGraphParameters(parameters);
  options = this._setGraphOptions(options);
  if (!options || options.graphSource !== 'a') {
    return this._executeGraphQuery(query, parameters, options, callback);
  }
  var self = this;
  this._getAnalyticsMaster(function getPreferredHostCallback(host) {
    options.preferredHost = host;
    return self._executeGraphQuery(query, parameters, options, callback);
  });
};

/**
 * @param {String} query
 * @param {Object} parameters
 * @param {QueryOptions} options
 * @param {Function} callback
 * @private
 */
Client.prototype._executeGraphQuery = function (query, parameters, options, callback) {
  this.execute(query, parameters, options, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, new GraphResultSet(result));
  });
};

/**
 * @param {Function} callback Function that gets called with the preferred host (no error parameter).
 * @private
 */
Client.prototype._getAnalyticsMaster = function (callback) {
  var self = this;
  this.execute('CALL DseClientTool.getAnalyticsGraphServer()', null, null, function (err, result) {
    if (err) {
      self.log('verbose', 'Error querying graph analytics server, query will not be routed optimally', err);
      return callback(null);
    }
    if (result.rows.length === 0) {
      self.log('verbose', 'Empty response querying graph analytics server, query will not be routed optimally');
      return callback(null);
    }
    var resultField = result.rows[0]['result'];
    if (!resultField || !resultField['location']) {
      self.log('verbose',
        'Unexpected response querying graph analytics server, query will not be routed optimally',
        result.rows[0]);
      return callback(null);
    }
    var hostName = resultField['location'].substr(0, resultField['location'].lastIndexOf(':'));
    var addressTranslator = self.options.policies.addressResolution;
    addressTranslator.translate(hostName, self.options.protocolOptions.port, function translateCallback(endpoint) {
      callback(self.hosts.get(endpoint));
    });
  });
};

/**
 * @param {GraphQueryOptions} options
 * @return {GraphQueryOptions}
 * @private
 */
Client.prototype._setGraphOptions = function (options) {
  var profile = this.profileManager.getProfile(options && options.executionProfile);
  if (!profile) {
    // Let the core driver deal with specified profile not been found
    return options;
  }
  var defaultGraphOptions = this._getDefaultGraphOptions(profile);
  if (!options || typeof options === 'function') {
    return defaultGraphOptions;
  }

  // Check if the user is using a parameter that would make the custom payload different from the
  // payload for the profile (ie: the user specified only the profile / or nothing at all)
  var noGraphPayloadOptions =
    !options.customPayload &&
    !options.graphLanguage &&
    !options.graphSource &&
    options.graphName === undefined &&
    options.graphReadConsistency === undefined &&
    options.graphWriteConsistency === undefined &&
    options.readTimeout === undefined;

  options = utils.extend({
    graphSource: defaultGraphOptions.graphSource,
    readTimeout: defaultGraphOptions.readTimeout,
    // The default retry policy for graph should not retry
    retry: defaultGraphOptions.retry
  }, options);

  // If there are no changes to custom payload, return.
  if (noGraphPayloadOptions) {
    // Reuse the same customPayload instance and avoid reconstruct it
    options.customPayload = defaultGraphOptions.customPayload;
    return options;
  }

  options.customPayload = options.customPayload || {};
  this._setPayloadKey(options, 'graph-language', options.graphLanguage);
  this._setPayloadKey(options, 'graph-source', options.graphSource);
  this._setPayloadKey(options, 'graph-name', options.graphName);
  this._setPayloadKey(options, 'graph-read-consistency', utils.getConsistencyName(options.graphReadConsistency));
  this._setPayloadKey(options, 'graph-write-consistency', utils.getConsistencyName(options.graphWriteConsistency));
  this._setPayloadKey(options, 'request-timeout', options.readTimeout > 0 ? options.readTimeout : null, longBuffer);
  return options;
};

/**
 * Gets the default options with the custom payload for a given profile.
 * @param {ExecutionProfile} profile
 * @returns {DseClientOptions}
 * @private
 */
Client.prototype._getDefaultGraphOptions = function (profile) {
  this._defaultGraphOptions = this._defaultGraphOptions || {};
  var options = this._defaultGraphOptions[profile.name];
  if (!options) {
    // this._graphOptions contains default profile options plus the default graph options
    var baseOptions = this._graphOptions;
    var profileOptions = profile.graphOptions || utils.emptyObject;
    var defaultProfile = this.profileManager.getDefault();
    options = this._defaultGraphOptions[profile.name] = {
      customPayload: {
        'graph-language': utf8Buffer(baseOptions.language),
        'graph-source': utf8Buffer(profileOptions.source || baseOptions.source)
      },
      graphSource: profileOptions.source || baseOptions.source
    };
    if (profile !== defaultProfile) {
      options.retry = profile.retry || baseOptions.retry;
    }
    else {
      // Based on an implementation detail of the execution profiles, the retry policy for the default profile is
      // always loaded (required), but that doesn't mean that it was specified by the user.
      // If it wasn't specified by the user, use the default retry policy for graph statements.
      options.retry = this._defaultProfileRetryPolicy || baseOptions.retry;
    }
    var name = utils.ifUndefined(profileOptions.name, baseOptions.name);
    if (name) {
      options.customPayload['graph-name'] = utf8Buffer(name);
    }
    var readConsistency = utils.ifUndefined(profileOptions.readConsistency, baseOptions.readConsistency);
    if (readConsistency !== undefined) {
      options.customPayload['graph-read-consistency'] =
        utf8Buffer(utils.getConsistencyName(readConsistency));
    }
    var writeConsistency = utils.ifUndefined(profileOptions.writeConsistency, baseOptions.writeConsistency);
    if (writeConsistency !== undefined) {
      options.customPayload['graph-write-consistency'] =
        utf8Buffer(utils.getConsistencyName(writeConsistency));
    }
    options.readTimeout = utils.ifUndefined3(profile.readTimeout, defaultProfile.readTimeout, baseOptions.readTimeout);
    if (options.readTimeout > 0) {
      // Write the graph read timeout payload
      options.customPayload['request-timeout'] = longBuffer(options.readTimeout);
    }
  }
  return options;
};

/**
 * @param options
 * @param {String} key
 * @param {String|Number|null} value
 * @param {Function} [converter]
 * @private
 */
Client.prototype._setPayloadKey = function (options, key, value, converter) {
  converter = converter || utf8Buffer;
  if (value === null) {
    // Use null to avoid set payload for a key
    return;
  }
  if (value !== undefined) {
    options.customPayload[key] = converter(value);
    return;
  }
  var profile = this.profileManager.getProfile(options && options.executionProfile);
  var profileOptions = this._getDefaultGraphOptions(profile);
  if (profileOptions.customPayload[key]) {
    options.customPayload[key] = profileOptions.customPayload[key];
  }
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
  var retryPolicy = null;
  for (var i = 0; i < options.profiles.length; i++) {
    var profile = options.profiles[i];
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

function utf8Buffer(value) {
  if (typeof value !== 'string') {
    throw new TypeError('Value must be a string');
  }
  return new Buffer(value, 'utf8');
}

function longBuffer(value) {
  value = Long.fromNumber(value);
  return Long.toBuffer(value);
}

module.exports = Client;
