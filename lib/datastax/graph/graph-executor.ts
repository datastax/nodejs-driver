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
const { GraphSON2Reader, GraphSON2Writer, GraphSON3Reader, GraphSON3Writer } = require('./graph-serializer');
const getCustomTypeSerializers = require('./custom-type-serializers');
const { GraphExecutionOptions, graphProtocol } = require('./options');

const graphLanguageGroovyString = 'gremlin-groovy';
const graphEngineCore = 'Core';

const graphSON2Reader = new GraphSON2Reader({ serializers: getCustomTypeSerializers() });
const graphSON2Writer = new GraphSON2Writer({ serializers: getCustomTypeSerializers() });
const graphSON3Reader = new GraphSON3Reader({ serializers: getCustomTypeSerializers() });
const graphSON3Writer = new GraphSON3Writer({ serializers: getCustomTypeSerializers() });

const rowParsers = new Map([
  [ graphProtocol.graphson2, getRowParser(graphSON2Reader) ],
  [ graphProtocol.graphson3, getRowParser(graphSON3Reader) ]
]);

const defaultWriters = new Map([
  [ graphProtocol.graphson1, x => JSON.stringify(x) ],
  [ graphProtocol.graphson2, getDefaultWriter(graphSON2Writer) ],
  [ graphProtocol.graphson3, getDefaultWriter(graphSON3Writer) ]
]);

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
  }

  /**
   * Executes the graph traversal.
   * @param {String|Object} query
   * @param {Object} parameters
   * @param {GraphQueryOptions} options
   */
  async send(query, parameters, options) {
    if (Array.isArray(parameters)) {
      throw new TypeError('Parameters must be a Object instance as an associative array');
    }

    if (!query) {
      throw new TypeError('Query must be defined');
    }

    const execOptions = new GraphExecutionOptions(
      options, this._client, this._graphBaseOptions, this._defaultProfileRetryPolicy);

    if (execOptions.getGraphSource() === 'a') {
      const host = await this._getAnalyticsMaster();
      execOptions.setPreferredHost(host);
    }

    // A query object that allows to plugin any executable thing
    const isQueryObject = typeof query === 'object' && query.graphLanguage && query.value && query.queryWriterFactory;

    if (isQueryObject) {
      // Use the provided graph language to override the current
      execOptions.setGraphLanguage(query.graphLanguage);
    }

    this._setGraphProtocol(execOptions);
    execOptions.setGraphPayload();
    parameters = GraphExecutor._buildGraphParameters(parameters, execOptions.getGraphSubProtocol());

    if (typeof query !== 'string') {
      // Its a traversal that needs to be converted
      // Transforming the provided query into a traversal requires the protocol to be set first.
      // Query writer factory can be defined in the options or in the query object
      let queryWriter = execOptions.getQueryWriter();

      if (isQueryObject) {
        queryWriter = query.queryWriterFactory(execOptions.getGraphSubProtocol());
      } else if (!queryWriter) {
        queryWriter = GraphExecutor._writerFactory(execOptions.getGraphSubProtocol());
      }

      query = queryWriter(!isQueryObject ? query : query.value);
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

    // Instances of rowParser transform Row instances into Traverser instances.
    // Traverser instance is an object with the following form { object: any, bulk: number }
    const rowParser = execOptions.getRowParser() || GraphExecutor._rowParserFactory(execOptions.getGraphSubProtocol());

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

  /**
   * Resolves what protocol should be used for decoding graph results for the given execution.
   *
   * <p>Resolution is done in the following manner if graphResults is not set:</p>
   *
   * <ul>
   *   <li>If graph name is set, and associated keyspace's graph engine is set to "Core", use {@link
    *       graphProtocol#graphson3}.
   *   <li>Else, if the graph language is not 'gremlin-groovy', use {@link graphProtocol#graphson2}
   *   <li>Otherwise, use {@link graphProtocol#graphson1}
   * </ul>
   * @param {GraphExecutionOptions} execOptions
   */
  _setGraphProtocol(execOptions) {
    let protocol = execOptions.getGraphSubProtocol();

    if (protocol) {
      return;
    }

    if (execOptions.getGraphName()) {
      const keyspace = this._client.metadata.keyspaces[execOptions.getGraphName()];
      if (keyspace && keyspace.graphEngine === graphEngineCore) {
        protocol = graphProtocol.graphson3;
      }
    }

    if (!protocol) {
      // Decide the minimal version supported by the graph language
      if (execOptions.getGraphLanguage() === graphLanguageGroovyString) {
        protocol = graphProtocol.graphson1;
      } else {
        protocol = graphProtocol.graphson2;
      }
    }

    execOptions.setGraphSubProtocol(protocol);
  }

  /**
   * Only GraphSON1 parameters are supported.
   * @param {Array|function|null} parameters
   * @param {string} protocol
   * @returns {string[]|null}
   * @private
   */
  static _buildGraphParameters(parameters, protocol) {
    if (!parameters || typeof parameters !== 'object') {
      return null;
    }

    const queryWriter = GraphExecutor._writerFactory(protocol);

    return [
      (protocol !== graphProtocol.graphson1 && protocol !== graphProtocol.graphson2)
        ? queryWriter(new Map(Object.entries(parameters)))
        : queryWriter(parameters)
    ];
  }

  static _rowParserFactory(protocol) {
    const handler = rowParsers.get(protocol);

    if (!handler) {
      // Default to no row parser
      return null;
    }

    return handler;
  }

  static _writerFactory(protocol) {
    const handler = defaultWriters.get(protocol);

    if (!handler) {
      throw new Error(`No writer defined for protocol ${protocol}`);
    }

    return handler;
  }
}

function getRowParser(reader) {
  return row => {
    const item = reader.read(JSON.parse(row['gremlin']));
    return { object: item['result'], bulk: item['bulk'] || 1 };
  };
}

function getDefaultWriter(writer) {
  return value => writer.write(value);
}

module.exports = GraphExecutor;