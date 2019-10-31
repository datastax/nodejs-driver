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

const utils = require('../../utils');
const policies = require('../../policies');
const GraphResultSet = require('./result-set');
const GraphSONReader = require('./graphson-reader');
const GraphExecutionOptions = require('./options').GraphExecutionOptions;

const graphLanguageBytecode = 'bytecode-json';
const graphLanguageGroovyString = 'gremlin-groovy';

/**
 * Internal class that contains the logic for executing a graph traversal.
 * @ignore
 */
class GraphExecutor {

  /**
   * Creates a new instance of GraphExecutor.
   * @param {Client} client
   * @param {ClientOptions} rawOptions
   * @param {Function} handler
   */
  constructor(client, rawOptions, handler) {
    this._client = client;
    this._handler = handler;

    // Retrieve the retry policy for the default profile to determine if it was specified
    this._defaultProfileRetryPolicy = client.profileManager.getDefaultConfiguredRetryPolicy();

    // Use graphBaseOptions as a way to gather all defaults that affect graph executions
    this._graphBaseOptions = utils.extend({
      executeAs: client.options.queryOptions.executeAs,
      language: graphLanguageGroovyString,
      source: 'g',
      readTimeout: 0,
      // As the default retry policy might retry non-idempotent queries
      // we should use default retry policy for all graph queries that does not retry
      retry: new policies.retry.FallthroughRetryPolicy()
    }, rawOptions.graphOptions, client.profileManager.getDefault().graphOptions);

    if (this._graphBaseOptions.readTimeout === null) {
      this._graphBaseOptions.readTimeout = client.options.socketOptions.readTimeout;
    }

    this._graphSONReader = new GraphSONReader();
  }

  /**
   * Executes the graph traversal.
   * @param {String} query
   * @param {Object} parameters
   * @param {ClientOptions} options
   * @param {Function} callback
   */
  send(query, parameters, options, callback) {
    if (Array.isArray(parameters)) {
      return callback(new TypeError('Parameters must be a Object instance as an associative array'));
    }
    parameters = GraphExecutor._setGraphParameters(parameters);

    let execOptions;
    try {
      execOptions = new GraphExecutionOptions(options, this._client, this._graphBaseOptions, this._defaultProfileRetryPolicy);
    }
    catch (e) {
      return callback(e);
    }

    if (execOptions.getGraphSource() !== 'a') {
      return this._executeGraphQuery(query, parameters, execOptions, callback);
    }

    this._getAnalyticsMaster(host => {
      execOptions.setPreferredHost(host);
      return this._executeGraphQuery(query, parameters, execOptions, callback);
    });
  }

  _executeGraphQuery(query, parameters, execOptions, callback) {
    this._handler.call(this._client, query, parameters, execOptions, (err, result) => {
      if (err) {
        return callback(err);
      }

      let rowParser = null;

      if (execOptions.getGraphLanguage() === graphLanguageBytecode) {
        rowParser = (row) => this._graphSONReader.read(JSON.parse(row['gremlin']));
      }

      callback(null, new GraphResultSet(result, rowParser));
    });
  }

  _getAnalyticsMaster(callback) {
    this._client.execute('CALL DseClientTool.getAnalyticsGraphServer()', null, null, (err, result) => {
      if (err) {
        this._client.log('verbose', 'Error querying graph analytics server, query will not be routed optimally', err);
        return callback(null);
      }

      if (result.rows.length === 0) {
        this._client.log('verbose',
          'Empty response querying graph analytics server, query will not be routed optimally');
        return callback(null);
      }

      const resultField = result.rows[0]['result'];
      if (!resultField || !resultField['location']) {
        this._client.log('verbose',
          'Unexpected response querying graph analytics server, query will not be routed optimally',
          result.rows[0]);
        return callback(null);
      }

      const hostName = resultField['location'].substr(0, resultField['location'].lastIndexOf(':'));
      const addressTranslator = this._client.options.policies.addressResolution;

      addressTranslator.translate(hostName, this._client.options.protocolOptions.port, (endpoint) =>
        callback(this._client.hosts.get(endpoint)));
    });
  }

  static _setGraphParameters(parameters) {
    if (!parameters || typeof parameters === 'function') {
      return null;
    }
    return [ JSON.stringify(parameters) ];
  }
}

module.exports = GraphExecutor;