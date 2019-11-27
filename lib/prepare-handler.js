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

const errors = require('./errors');
const utils = require('./utils');
const RequestHandler = require('./request-handler');
const promiseUtils = require('./promise-utils');

/**
 * Encapsulates the logic for dealing with the different prepare request and response flows, including failover when
 * trying to prepare a query.
 */
class PrepareHandler {
  /**
   * Creates a new instance of PrepareHandler
   * @param {Client} client
   * @param {LoadBalancingPolicy} loadBalancing
   */
  constructor(client, loadBalancing) {
    this._client = client;
    this._loadBalancing = loadBalancing;
    this.logEmitter = client.options.logEmitter;
    this.log = utils.log;
  }

  /**
   * @param {Client} client
   * @param {LoadBalancingPolicy} loadBalancing
   * @param {String} query
   * @param {String} keyspace
   * @returns {Promise<{queryId, meta}>}
   * @static
   */
  static async getPrepared(client, loadBalancing, query, keyspace) {
    const info = client.metadata.getPreparedInfo(keyspace, query);
    if (info.queryId) {
      return info;
    }

    if (info.preparing) {
      // It's already being prepared
      return await promiseUtils.fromEvent(info, 'prepared');
    }

    const instance = new PrepareHandler(client, loadBalancing);
    return await instance._prepare(info, query, keyspace);
  }

  /**
   * @param {Client} client
   * @param {LoadBalancingPolicy} loadBalancing
   * @param {Array} queries
   * @param {String} keyspace
   * @static
   */
  static async getPreparedMultiple(client, loadBalancing, queries, keyspace) {
    const result = [];

    for (const item of queries) {
      let query;

      if (item) {
        query = typeof item === 'string' ? item : item.query;
      }

      if (typeof query !== 'string') {
        throw new errors.ArgumentError('Query item should be a string');
      }

      const { queryId, meta } = await PrepareHandler.getPrepared(client, loadBalancing, query, keyspace);
      result.push({ query, params: utils.adaptNamedParamsPrepared(item.params, meta.columns), queryId, meta });
    }

    return result;
  }

  /**
   * Prepares the query on a single host or on all hosts depending on the options.
   * Uses the info 'prepared' event to emit the result.
   * @param {Object} info
   * @param {String} query
   * @param {String} keyspace
   * @returns {Promise<{queryId, meta}>}
   */
  async _prepare(info, query, keyspace) {
    info.preparing = true;
    let iterator;

    try {
      iterator = await promiseUtils.newQueryPlan(this._loadBalancing, keyspace, null);
      return await this._prepareWithQueryPlan(info, iterator, query, keyspace);
    } catch (err) {
      info.preparing = false;
      err.query = query;
      info.emit('prepared', err);

      throw err;
    }
  }

  /**
   * Uses the query plan to prepare the query on the first host and optionally on the rest of the hosts.
   * @param {Object} info
   * @param {Iterator} iterator
   * @param {String} query
   * @param {String} keyspace
   * @returns {Promise<{queryId, meta}>}
   * @private
   */
  async _prepareWithQueryPlan(info, iterator, query, keyspace) {
    const triedHosts = {};

    while (true) {
      const host = RequestHandler.getNextHost(iterator, this._client.profileManager, triedHosts);

      if (host === null) {
        throw new errors.NoHostAvailableError(triedHosts);
      }

      try {
        const connection = await host.borrowConnectionAsync(keyspace, null);
        const response = await connection.prepareOnceAsync(query, keyspace);

        if (this._client.options.prepareOnAllHosts) {
          await this._prepareOnAllHostsAsync(host, iterator, query, keyspace);
        }

        // Set the prepared metadata
        info.preparing = false;
        info.queryId = response.id;
        info.meta = response.meta;
        this._client.metadata.setPreparedById(info);
        info.emit('prepared', null, info);

        return info;

      } catch (err) {
        triedHosts[host.address] = err;

        if (!err.isSocketError && !(err instanceof errors.OperationTimedOutError)) {
          // There's no point in retrying syntax errors and other response errors
          throw err;
        }
      }
    }
  }

  /**
   * Prepares all queries on a single host.
   * @param {Host} host
   * @param {Array} allPrepared
   */
  static async prepareAllQueries(host, allPrepared) {
    const anyKeyspaceQueries = [];
    //TODO: Consider use of Map
    const queriesByKeyspace = {};
    allPrepared.forEach(info => {
      let arr;
      if (info.keyspace) {
        arr = queriesByKeyspace[info.keyspace];

        if (!arr) {
          arr = [];
          queriesByKeyspace[info.keyspace] = arr;
        }
      } else {
        arr = anyKeyspaceQueries;
      }

      arr.push(info.query);
    });

    for (const keyspace of Object.keys(queriesByKeyspace)) {
      await PrepareHandler._borrowAndPrepare(host, keyspace, queriesByKeyspace[keyspace]);
    }

    await PrepareHandler._borrowAndPrepare(host, null, anyKeyspaceQueries);
  }

  /**
   * Borrows a connection from the host and prepares the queries provided.
   * @param {Host} host
   * @param {String} keyspace
   * @param {Array} queries
   * @private
   */
  static async _borrowAndPrepare(host, keyspace, queries) {
    if (queries.length === 0) {
      return;
    }

    const connection = await host.borrowConnectionAsync(keyspace, null);

    for (const query of queries) {
      await connection.prepareOnceAsync(query, keyspace);
    }
  }

  /**
   * Prepares the provided query on all hosts, except the host provided.
   * @param {Host} hostToAvoid
   * @param {Iterator} iterator
   * @param {String} query
   * @param {String} keyspace
   * @private
   */
  _prepareOnAllHostsAsync(hostToAvoid, iterator, query, keyspace) {
    const promises = utils
      .iteratorToArray(iterator)
      .map(async host => {
        if (host.address === hostToAvoid.address) {
          return;
        }

        try {
          const connection = await host.borrowConnectionAsync(keyspace, null);
          await connection.prepareOnceAsync(query, keyspace);
        } catch (err) {
          // Don't mind
          this.log('verbose', `Unexpected error while preparing query (${query}) on ${host.address}`, err);
        }
      });

    return Promise.all(promises);
  }
}

module.exports = PrepareHandler;