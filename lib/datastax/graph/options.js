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
const util = require('util');
const types = require('../../types');
const utils = require('../../utils');
const { DefaultExecutionOptions, proxyExecuteKey } = require('../../execution-options');
const Long = types.Long;

let consistencyNames;

const graphProtocol = Object.freeze({
  graphson1: 'graphson-1.0',
  graphson2: 'graphson-2.0',
  graphson3: 'graphson-3.0'
});

const payloadKeys = Object.freeze({
  language :'graph-language',
  source: 'graph-source',
  name: 'graph-name',
  results: 'graph-results',
  writeConsistency: 'graph-write-consistency',
  readConsistency: 'graph-read-consistency',
  timeout: 'request-timeout'
});

/**
 * Graph options that extends {@link QueryOptions}.
 * <p>
 *   Consider using [execution profiles]{@link ExecutionProfile} if you plan to reuse options across different
 *   query executions.
 * </p>
 * @typedef {QueryOptions} module:datastax/graph~GraphQueryOptions
 * @property {String} [graphLanguage] The graph language to use in graph queries.
 * @property {String} [graphResults] The protocol to use for serializing and deserializing graph results.
 * <p>
 *   Note that this value should rarely be set by users and will otherwise be unset. When unset the server resolves
 *   the protocol based on the <code>graphLanguage</code> specified.
 * </p>
 * @property {String} [graphName] The graph name to be used in the query. You can use <code>null</code> to clear the
 * value from the <code>DseClientOptions</code> and execute a query without a default graph.
 * @property {Number} [graphReadConsistency] Specifies the
 * [consistency level]{@link module:types~consistencies}
 * to be used for the graph read queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the READ part of the graph query.
 * </p>
 * @property {String} [graphSource] The graph traversal source name to use in graph queries.
 * @property {Number} [graphWriteConsistency] Specifies the [consistency level]{@link module:types~consistencies} to
 * be used for the graph write queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the WRITE part of the graph query.
 * </p>
 * @property {RetryPolicy} [retry] Sets the retry policy to be used for the graph query execution.
 * <p>
 *   When not specified in the {@link GraphQueryOptions} or in the {@link ExecutionProfile}, it will use by default
 *   a retry policy that does not retry graph executions.
 * </p>
 */

/**
 * Gets the default options with the custom payload for a given profile.
 * @param {ProfileManager} profileManager
 * @param baseOptions
 * @param {RetryPolicy|null} defaultRetryPolicy
 * @param {ExecutionProfile} profile
 * @returns {DseClientOptions}
 * @private
 */
function getDefaultGraphOptions(profileManager, baseOptions, defaultRetryPolicy, profile) {
  return profileManager.getOrCreateGraphOptions(profile, function createDefaultOptions() {
    const profileOptions = profile.graphOptions || utils.emptyObject;
    const defaultProfile = profileManager.getDefault();
    const options = {
      customPayload: {
        [payloadKeys.language]: utils.allocBufferFromString(profileOptions.language || baseOptions.language),
        [payloadKeys.source]: utils.allocBufferFromString(profileOptions.source || baseOptions.source)
      },
      graphLanguage: profileOptions.language || baseOptions.language,
      graphResults: profileOptions.results || baseOptions.results,
      graphSource: profileOptions.source || baseOptions.source,
      graphName: utils.ifUndefined(profileOptions.name, baseOptions.name)
    };

    if (profile !== defaultProfile) {
      options.retry = profile.retry || baseOptions.retry;
    } else {
      // Based on an implementation detail of the execution profiles, the retry policy for the default profile is
      // always loaded (required), but that doesn't mean that it was specified by the user.
      // If it wasn't specified by the user, use the default retry policy for graph statements.
      options.retry = defaultRetryPolicy || baseOptions.retry;
    }

    if (baseOptions.executeAs) {
      options.customPayload[proxyExecuteKey] = utils.allocBufferFromString(baseOptions.executeAs);
    }

    if (options.graphName) {
      options.customPayload[payloadKeys.name] = utils.allocBufferFromString(options.graphName);
    }

    const graphResults = utils.ifUndefined(profileOptions.results, baseOptions.graphResults);
    if (graphResults !== undefined) {
      options.customPayload[payloadKeys.results] = utils.allocBufferFromString(graphResults);
    }

    const readConsistency = utils.ifUndefined(profileOptions.readConsistency, baseOptions.readConsistency);
    if (readConsistency !== undefined) {
      options.customPayload[payloadKeys.readConsistency] =
        utils.allocBufferFromString(getConsistencyName(readConsistency));
    }

    const writeConsistency = utils.ifUndefined(profileOptions.writeConsistency, baseOptions.writeConsistency);
    if (writeConsistency !== undefined) {
      options.customPayload[payloadKeys.writeConsistency] =
        utils.allocBufferFromString(getConsistencyName(writeConsistency));
    }

    options.readTimeout = utils.ifUndefined3(profile.readTimeout, defaultProfile.readTimeout, baseOptions.readTimeout);
    if (options.readTimeout > 0) {
      // Write the graph read timeout payload
      options.customPayload[payloadKeys.timeout] = longBuffer(options.readTimeout);
    }

    return options;
  });
}

/**
 * Sets the payload key. If the value is not provided, it uses the value from the default profile options.
 * @param {Object} payload
 * @param {QueryOptions} profileOptions
 * @param {String} key
 * @param {String|Number|null} value
 * @param {Function} [converter]
 * @private
 */
function setPayloadKey(payload, profileOptions, key, value, converter) {
  converter = converter || utils.allocBufferFromString;
  if (value === null) {
    // Use null to avoid set payload for a key
    return;
  }

  if (value !== undefined) {
    payload[key] = converter(value);
    return;
  }

  if (profileOptions.customPayload[key]) {
    payload[key] = profileOptions.customPayload[key];
  }
}

function longBuffer(value) {
  value = Long.fromNumber(value);
  return Long.toBuffer(value);
}

/**
 * Gets the name in upper case of the consistency level.
 * @param {Number} consistency
 * @private
 */
function getConsistencyName(consistency) {
  // eslint-disable-next-line
  if (consistency == undefined) {
    //null or undefined => undefined
    return undefined;
  }
  loadConsistencyNames();
  const name = consistencyNames[consistency];
  if (!name) {
    throw new Error(util.format(
      'Consistency %s not found, use values defined as properties in types.consistencies object', consistency
    ));
  }
  return name;
}

function loadConsistencyNames() {
  if (consistencyNames) {
    return;
  }
  consistencyNames = {};
  const propertyNames = Object.keys(types.consistencies);
  for (let i = 0; i < propertyNames.length; i++) {
    const name = propertyNames[i];
    consistencyNames[types.consistencies[name]] = name.toUpperCase();
  }
  //Using java constants naming conventions
  consistencyNames[types.consistencies.localQuorum] = 'LOCAL_QUORUM';
  consistencyNames[types.consistencies.eachQuorum] = 'EACH_QUORUM';
  consistencyNames[types.consistencies.localSerial] = 'LOCAL_SERIAL';
  consistencyNames[types.consistencies.localOne] = 'LOCAL_ONE';
}

/**
 * Represents a wrapper around the options related to a graph execution.
 * @internal
 * @ignore
 */
class GraphExecutionOptions extends DefaultExecutionOptions {

  /**
   * Creates a new instance of GraphExecutionOptions.
   * @param {GraphQueryOptions} queryOptions The user provided query options.
   * @param {Client} client the client instance.
   * @param graphBaseOptions The default graph base options.
   * @param {RetryPolicy} defaultProfileRetryPolicy
   */
  constructor(queryOptions, client, graphBaseOptions, defaultProfileRetryPolicy) {

    queryOptions = queryOptions || utils.emptyObject;
    super(queryOptions, client, null);

    this._defaultGraphOptions = getDefaultGraphOptions(
      client.profileManager, graphBaseOptions, defaultProfileRetryPolicy, this.getProfile());

    this._preferredHost = null;
    this._graphSubProtocol = queryOptions.graphResults || this._defaultGraphOptions.graphResults;
    this._graphLanguage = queryOptions.graphLanguage || this._defaultGraphOptions.graphLanguage;
  }

  setPreferredHost(host) {
    this._preferredHost = host;
  }

  getPreferredHost() {
    return this._preferredHost;
  }

  getGraphSource() {
    return this.getRawQueryOptions().graphSource || this._defaultGraphOptions.graphSource;
  }

  getGraphLanguage() {
    return this._graphLanguage;
  }

  setGraphLanguage(value) {
    this._graphLanguage = value;
  }

  getGraphName() {
    return utils.ifUndefined(this.getRawQueryOptions().graphName, this._defaultGraphOptions.graphName);
  }

  getGraphSubProtocol() {
    return this._graphSubProtocol;
  }

  setGraphSubProtocol(protocol) {
    this._graphSubProtocol = protocol;
  }

  /** Graph executions have a specific default read timeout */
  getReadTimeout() {
    return this.getRawQueryOptions().readTimeout || this._defaultGraphOptions.readTimeout;
  }

  /** Graph executions have a specific default retry policy */
  getRetryPolicy() {
    return this.getRawQueryOptions().retry || this._defaultGraphOptions.retry;
  }

  getRowParser() {
    const factory = this.getRawQueryOptions().rowParserFactory;

    if (!factory) {
      return null;
    }

    return factory(this.getGraphSubProtocol());
  }

  getQueryWriter() {
    const factory = this.getRawQueryOptions().queryWriterFactory;

    if (!factory) {
      return null;
    }

    return factory(this.getGraphSubProtocol());
  }

  setGraphPayload() {
    const options = this.getRawQueryOptions();
    const defaultOptions = this._defaultGraphOptions;

    // Clone the existing custom payload (if any)
    const payload = Object.assign({}, this.getCustomPayload());

    // Override the payload for DSE Graph exclusive options
    setPayloadKey(payload, defaultOptions, payloadKeys.language,
      this.getGraphLanguage() !== this._defaultGraphOptions.graphLanguage ? this.getGraphLanguage() : undefined);
    setPayloadKey(payload, defaultOptions, payloadKeys.source, options.graphSource);
    setPayloadKey(payload, defaultOptions, payloadKeys.name, options.graphName);
    setPayloadKey(payload, defaultOptions, payloadKeys.readConsistency,
      getConsistencyName(options.graphReadConsistency));
    setPayloadKey(payload, defaultOptions, payloadKeys.writeConsistency,
      getConsistencyName(options.graphWriteConsistency));

    // Use the read timeout defined by the user or the one default to graph executions
    setPayloadKey(payload, defaultOptions, payloadKeys.timeout,
      this.getReadTimeout() > 0 ? this.getReadTimeout() : null, longBuffer);

    // Graph result is always set
    payload[payloadKeys.results] = defaultOptions.graphResults === this.getGraphSubProtocol()
      ? defaultOptions.customPayload[payloadKeys.results] : utils.allocBufferFromString(this.getGraphSubProtocol());

    this.setCustomPayload(payload);
  }
}

module.exports = {
  GraphExecutionOptions,
  graphProtocol,
  payloadKeys
};