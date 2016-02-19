'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var GraphResultSet = require('./graph/result-set');
var encoderExtensions = require('./encoder-extensions');
/**
 * Creates a new {@link DseClient} instance.
 * @classdesc
 * Extends the
 * [Client]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/Client.html} class of the
 * [DataStax Node.js Driver for Apache Cassandra]{@link https://github.com/datastax/nodejs-driver}
 * to provide DSE-specific features support such as Graph, geospatial types representation and authentication, in
 * addition to the inherited <code>Client</code> methods used to execute CQL queries (<code>execute()</code>,
 * <code>eachRow()</code>, <code>stream()</code>).
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
 *   console.log('User email ' + result.rows[0].email);
 * });
 * @constructor
 * @extends {Client}
 */
function DseClient(options) {
  if (!options) {
    throw new cassandra.errors.ArgumentError('You must provide a parameter with the DseClient options');
  }
  encoderExtensions.register(cassandra.Encoder);
  this._graphOptions = extend({
    language: 'gremlin-groovy',
    source: 'default'
  }, options.graphOptions);
  cassandra.Client.call(this, options);
}

/**
 * DSE client options that extends DataStax driver
 * [ClientOptions]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/global.html#ClientOptions}.
 * @typedef {Object} DseClientOptions
 * @property {Object} graphOptions
 * @property {String} [graphOptions.language] The graph language to use in graph queries. Default: 'gremlin-groovy'.
 * @property {String} [graphOptions.source] The graph traversal source name to use in graph queries. Default: 'default'.
 * @property {String} graphOptions.name The graph name to be used in all graph queries.
 * <p/>
 * This property is required but there is no default value for it. This value can be overridden at query level.
 * </p>
 * @property {String} [graphOptions.alias] The graph rebinding name to use in graph queries.
 */

/**
 * Graph options that extends DataStax driver
 * [QueryOptions]{@link http://docs.datastax.com/en/drivers/nodejs/3.0/global.html#QueryOptions}.
 * @typedef {Object} GraphQueryOptions
 * @property {String} [graphLanguage] The graph language to use in graph queries.
 * @property {String} [graphSource] The graph traversal source name to use in graph queries.
 * @property {String} [graphName] The graph name to be used in the query. You can use <code>null</code> to clear the
 * value from the {DseClientOptions} and execute a query without a default graph.
 * @property {String} [graphAlias] The graph rebinding name to use in graph queries..
 */

util.inherits(DseClient, cassandra.Client);

/**
 * Executes a graph query.
 * @param {String} query The gremlin query.
 * @param {Object} [parameters] An associative array containing the key and values of the parameters.
 * @param {GraphQueryOptions} [options] The graph query options.
 * @param {Function} callback Function to execute when the response is retrieved, taking two arguments:
 * <code>err</code> and <code>result</code>.
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
  options = this._setGraphPayload(options);
  if (util.isArray(parameters)) {
    return callback(new TypeError('Parameters must be a Object instance as an associative array'));
  }
  parameters = this._setGraphParameters(parameters);
  this.execute(query, parameters, options, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, new GraphResultSet(result));
  });
};

/**
 * @param {GraphQueryOptions} options
 * @private
 */
DseClient.prototype._setGraphPayload = function (options) {
  if (!this._defaultGraphOptions) {
    //allocate once and reuse
    this._defaultGraphOptions = {
      customPayload: {
        'graph-language': utf8Buffer(this._graphOptions.language),
        'graph-source': utf8Buffer(this._graphOptions.source)
      }
    };
    if (this._graphOptions.name) {
      this._defaultGraphOptions.customPayload['graph-name'] = utf8Buffer(this._graphOptions.name)
    }
    if (this._graphOptions.alias) {
      this._defaultGraphOptions.customPayload['graph-alias'] = utf8Buffer(this._graphOptions.alias)
    }
  }
  if (!options) {
    return this._defaultGraphOptions;
  }
  options = extend({}, options);
  var noGraphOptions = !options.graphLanguage &&
    !options.graphSource &&
    !options.graphAlias &&
    typeof options.graphName === 'undefined';
  if (noGraphOptions && !options.customPayload) {
    //reuse the same customPayload instance
    options.customPayload = this._defaultGraphOptions.customPayload;
    return options;
  }
  if (!options.customPayload) {
    options.customPayload = {};
  }
  this._setPayloadKey(options, 'graph-language', options.graphLanguage);
  this._setPayloadKey(options, 'graph-source', options.graphSource);
  this._setPayloadKey(options, 'graph-name', options.graphName);
  this._setPayloadKey(options, 'graph-alias', options.graphAlias);
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

function extend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
}

function utf8Buffer(value) {
  if (typeof value !== 'string') {
    throw new TypeError('Value must be a string');
  }
  return new Buffer(value, 'utf8');
}

module.exports = DseClient;