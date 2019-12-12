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
const { GraphExecutionOptions } = require('./options');

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
   * @param {GraphQueryOptions} options
   */
  async send(query, parameters, options) {
    if (Array.isArray(parameters)) {
      throw new TypeError('Parameters must be a Object instance as an associative array');
    }

    parameters = GraphExecutor._setGraphParameters(parameters);

    const execOptions = new GraphExecutionOptions(
      options, this._client, this._graphBaseOptions, this._defaultProfileRetryPolicy);

    if (execOptions.getGraphSource() === 'a') {
      const host = await this._getAnalyticsMaster();
      execOptions.setPreferredHost(host);
    }

    return await this._executeGraphQuery(query, parameters, execOptions);
  }

  /**
   * Sends the graph traversal.
   * @param {string} query
   * @param {object} parameters
   * @param {GraphExecutionOptions} execOptions
   * @returns {Promise<GraphResultSet>}
   * @private
   */
  async _executeGraphQuery(query, parameters, execOptions) {
    const result = await this._handler.call(this._client, query, parameters, execOptions);

    let rowParser = null;

    if (execOptions.getGraphLanguage() === graphLanguageBytecode) {
      rowParser = (row) => this._graphSONReader.read(JSON.parse(row['gremlin']));
    }

    return new GraphResultSet(result, rowParser);
  }

  /**
   * Uses the RPC call to obtain the analytics master host.
   * @returns {Promise<Host|null>}
   * @private
   */
  async _getAnalyticsMaster() {
    try {
      const result = await this._client.execute('CALL DseClientTool.getAnalyticsGraphServer()', utils.emptyArray);

      if (result.rows.length === 0) {
        this._client.log('verbose',
          'Empty response querying graph analytics server, query will not be routed optimally');
        return null;
      }

      const resultField = result.rows[0]['result'];
      if (!resultField || !resultField['location']) {
        this._client.log('verbose',
          'Unexpected response querying graph analytics server, query will not be routed optimally',
          result.rows[0]);
        return null;
      }

      const hostName = resultField['location'].substr(0, resultField['location'].lastIndexOf(':'));
      const addressTranslator = this._client.options.policies.addressResolution;

      return await new Promise(resolve => {
        addressTranslator.translate(hostName, this._client.options.protocolOptions.port, (endpoint) =>
          resolve(this._client.hosts.get(endpoint)));
      });
    } catch (err) {
      this._client.log('verbose', 'Error querying graph analytics server, query will not be routed optimally', err);
      return null;
    }
  }

  static _setGraphParameters(parameters) {
    if (!parameters || typeof parameters === 'function') {
      return null;
    }
    return [ JSON.stringify(parameters) ];
  }
}

module.exports = GraphExecutor;