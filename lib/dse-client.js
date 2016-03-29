'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var GraphResultSet = require('./graph/result-set');
var encoderExtensions = require('./encoder-extensions');
var utils = require('./utils');
/**
 * Creates a new {@link DseClient} instance.
 * @classdesc
 * Extends the
 * [Client]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/Client.html} class of the
 * [DataStax Node.js Driver for Apache Cassandra]{@link https://github.com/datastax/nodejs-driver}
 * to provide DSE-specific features support such as Graph, geospatial types representation and authentication, in
 * addition to the inherited <code>Client</code> methods used to execute CQL queries (<code>execute()</code>,
 * <code>eachRow()</code>, <code>stream()</code>).
 * <p>
 *   {@link DseClient} instances are designed to be long-lived and usually a single instance is enough per application.
 * </p>
 * @param {DseClientOptions} options The options used to create the client instance.
 * @example
 * const dse = require('dse-driver');
 * const client = new dse.DseClient({
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
function DseClient(options) {
  if (!options) {
    throw new cassandra.errors.ArgumentError('You must provide a parameter with the DseClient options');
  }
  encoderExtensions.register(cassandra.Encoder);
  cassandra.Client.call(this, options);
  this._graphOptions = utils.extend({
    language: 'gremlin-groovy',
    source: 'default',
    readTimeout: 32000,
    masterCallExpiration: 30
  }, options.graphOptions);
  /**
   * Timestamp in seconds for the last RPC to get the graph analytics master
   * @type {number}
   * @private
   */
  this._graphMasterTimestamp = 0;
  this._graphMasterHost = null;
}

/**
 * DSE client options that extends DataStax driver
 * [ClientOptions]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/global.html#ClientOptions}.
 * @typedef {ClientOptions} DseClientOptions
 * @property {Object} graphOptions
 * @property {String} [graphOptions.language] The graph language to use in graph queries. Default:
 * <code>'gremlin-groovy'</code>.
 * @property {String} graphOptions.name The graph name to be used in all graph queries.
 * <p/>
 * This property is required but there is no default value for it. This value can be overridden at query level.
 * </p>
 * @property {Number} [graphOptions.readConsistency] Overrides the
 * [consistency level]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/module-types.html#~consistencies} defined in
 * the query options for graph read queries.
 * @property {Number} [graphOptions.readTimeout] Overrides the
 * [default per-host read timeout]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/global.html#ClientOptions} in
 * milliseconds for all graph queries. Default: <code>32000</code>.
 * @property {String} [graphOptions.source] The graph traversal source name to use in graph queries. Default:
 * <code>'default'</code>.
 * @property {Number} [graphOptions.writeConsistency] Overrides the
 * [consistency level]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/module-types.html#~consistencies} defined in
 * the query options for graph write queries.
 */

/**
 * Graph options that extends DataStax driver
 * [QueryOptions]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/global.html#QueryOptions}.
 * @typedef {QueryOptions} GraphQueryOptions
 * @property {String?} [graphLanguage] The graph language to use in graph queries.
 * @property {String?} [graphName] The graph name to be used in the query. You can use <code>null</code> to clear the
 * value from the <code>DseClientOptions</code> and execute a query without a default graph.
 * @property {Number?} [graphReadConsistency] Specifies the
 * [consistency level]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/module-types.html#~consistencies} to be used
 * for the graph read queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the READ part of the graph query.
 * </p>
 * @property {String?} [graphSource] The graph traversal source name to use in graph queries.
 * @property {Number?} [graphWriteConsistency] Specifies the
 * [consistency level]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/module-types.html#~consistencies} to be used
 * for the graph write queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the WRITE part of the graph query.
 * </p>
 */

util.inherits(DseClient, cassandra.Client);

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
DseClient.prototype.executeGraph = function (query, parameters, options, callback) {
  if (typeof parameters === 'function') {
    //noinspection JSValidateTypes
    callback = parameters;
    options = null;
    parameters = null;
  }
  else if (typeof options === 'function') {
    //noinspection JSValidateTypes
    callback = options;
    options = null;
  }
  if (util.isArray(parameters)) {
    return callback(new TypeError('Parameters must be a Object instance as an associative array'));
  }
  parameters = this._setGraphParameters(parameters);
  options = this._setGraphOptions(options);
  if (options.graphSource !== 'a') {
    return this._executeGraphQuery(query, parameters, options, callback);
  }
  var self = this;
  this._getAnalyticsMaster(function getPreferredHostCallback(host) {
    //noinspection JSUndefinedPropertyAssignment
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
DseClient.prototype._executeGraphQuery = function (query, parameters, options, callback) {
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
DseClient.prototype._getAnalyticsMaster = function (callback) {
  var expirationDate = this._graphMasterTimestamp + this._graphOptions.masterCallExpiration * 1000;
  if (this._graphMasterHost && expirationDate > Date.now()) {
    return callback(this._graphMasterHost);
  }
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
    addressTranslator.translate(hostName, self.options.protocolOptions.port, function (endpoint) {
      //Cache the response
      self._graphMasterTimestamp = Date.now();
      self._graphMasterHost = self.hosts.get(endpoint);
      callback(self._graphMasterHost);
    });
  });
};

/**
 * @param {GraphQueryOptions} options
 * @return {GraphQueryOptions}
 * @private
 */
DseClient.prototype._setGraphOptions = function (options) {
  if (!this._defaultGraphOptions) {
    //allocate once and reuse
    this._defaultGraphOptions = {
      customPayload: {
        'graph-language': utf8Buffer(this._graphOptions.language),
        'graph-source': utf8Buffer(this._graphOptions.source)
      },
      graphSource: this._graphOptions.source
    };
    if (this._graphOptions.name) {
      this._defaultGraphOptions.customPayload['graph-name'] = utf8Buffer(this._graphOptions.name);
    }
    if (this._graphOptions.readConsistency !== undefined) {
      this._defaultGraphOptions.customPayload['graph-read-consistency'] =
        utf8Buffer(utils.getConsistencyName(this._graphOptions.readConsistency));
    }
    if (this._graphOptions.writeConsistency !== undefined) {
      this._defaultGraphOptions.customPayload['graph-write-consistency'] =
        utf8Buffer(utils.getConsistencyName(this._graphOptions.writeConsistency));
    }
    if (this._graphOptions.readTimeout) {
      this._defaultGraphOptions.readTimeout = this._graphOptions.readTimeout;
    }
  }
  if (!options) {
    return this._defaultGraphOptions;
  }
  //Add the properties from the default options to the current options instance
  options = utils.extend({
    readTimeout: this._graphOptions.readTimeout,
    graphSource: this._graphOptions.source
  }, options);
  if (!options.customPayload) {
    var noGraphPayloadOptions = !options.graphLanguage &&
      options.graphSource === this._graphOptions.source &&
      options.graphName === undefined &&
      options.graphReadConsistency === undefined &&
      options.graphWriteConsistency === undefined;
    if (noGraphPayloadOptions) {
      //its safe to reuse the same customPayload instance
      options.customPayload = this._defaultGraphOptions.customPayload;
      return options;
    }
    options.customPayload = {};
  }
  this._setPayloadKey(options, 'graph-language', options.graphLanguage);
  this._setPayloadKey(options, 'graph-source', options.graphSource);
  this._setPayloadKey(options, 'graph-name', options.graphName);
  this._setPayloadKey(options, 'graph-read-consistency', utils.getConsistencyName(options.graphReadConsistency));
  this._setPayloadKey(options, 'graph-write-consistency', utils.getConsistencyName(options.graphWriteConsistency));
  return options;
};

/**
 * @param options
 * @param {String} key
 * @param {String} value
 * @private
 */
DseClient.prototype._setPayloadKey = function (options, key, value) {
  if (value) {
    options.customPayload[key] = utf8Buffer(value);
    return;
  }
  if (value === null) {
    //null is used to clear a default value
    return;
  }
  if (this._defaultGraphOptions.customPayload[key]) {
    options.customPayload[key] = this._defaultGraphOptions.customPayload[key];
  }
};

/**
 * @param parameters
 * @returns {Array.<String>}
 * @private
 */
DseClient.prototype._setGraphParameters = function (parameters) {
  if (!parameters) {
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

module.exports = DseClient;