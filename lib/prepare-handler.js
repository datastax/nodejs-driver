'use strict';

const util = require('util');
const errors = require('./errors');
const utils = require('./utils');
const RequestHandler = require('./request-handler');

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
   * @param {Function} callback
   * @static
   */
  static getPrepared(client, loadBalancing, query, keyspace, callback) {
    const info = client.metadata.getPreparedInfo(keyspace, query);
    if (info.queryId) {
      return callback(null, info.queryId, info.meta);
    }
    info.once('prepared', callback);
    if (info.preparing) {
      // It's already being prepared
      return;
    }
    const instance = new PrepareHandler(client, loadBalancing);
    instance._prepare(info, query, keyspace);
  }

  /**
   * @param {Client} client
   * @param {LoadBalancingPolicy} loadBalancing
   * @param {Array} queries
   * @param {String} keyspace
   * @param {Function} callback
   * @static
   */
  static getPreparedMultiple(client, loadBalancing, queries, keyspace, callback) {
    const result = new Array(queries.length);
    utils.forEachOf(queries, function eachQuery(item, index, next) {
      let query;
      if (item) {
        query = typeof item === 'string' ? item : item.query;
      }
      if (typeof query !== 'string') {
        return next(new errors.ArgumentError('Query item should be a string'));
      }

      PrepareHandler.getPrepared(client, loadBalancing, query, keyspace, function getPrepareCb(err, id, meta) {
        if (err) {
          return next(err);
        }
        result[index] = {
          query: query,
          params: utils.adaptNamedParamsPrepared(item.params, meta.columns),
          queryId: id,
          meta: meta
        };
        next();
      });
    }, function eachEnded(err) {
      if (err) {
        return callback(err);
      }
      callback(null, result);
    });
  }

  /**
   * Prepares the query on a single host or on all hosts depending on the options.
   * Uses the info 'prepared' event to emit the result.
   * @param {Object} info
   * @param {String} query
   * @param {String} keyspace
   */
  _prepare(info, query, keyspace) {
    info.preparing = true;
    const self = this;
    this._loadBalancing.newQueryPlan(keyspace, null, function (err, iterator) {
      if (err) {
        info.preparing = false;
        return info.emit('prepared', err);
      }
      self._prepareWithQueryPlan(info, iterator, null, query, keyspace);
    });
  }

  /**
   * @param {Object} info
   * @param {Iterator} iterator
   * @param {Object|null} triedHosts
   * @param {String} query
   * @param {String} keyspace
   * @private
   */
  _prepareWithQueryPlan(info, iterator, triedHosts, query, keyspace) {
    triedHosts = triedHosts || {};
    const self = this;
    RequestHandler.borrowNextConnection(iterator, triedHosts, this._client.profileManager, keyspace,
      function borrowCallback(err, connection, host) {
        if (err) {
          return self._onPrepareError(err, host, triedHosts, info, iterator, query, keyspace);
        }
        connection.prepareOnce(query, function prepareOnceCallback(err, response) {
          if (err) {
            return self._onPrepareError(err, host, triedHosts, info, iterator, query, keyspace);
          }
          if (self._client.options.prepareOnAllHosts) {
            return self._prepareOnAllHosts(info, response, host, iterator, query, keyspace);
          }
          self._onPrepareSuccess(info, response);
        });
      });
  }

  _onPrepareSuccess(info, response) {
    info.preparing = false;
    info.queryId = response.id;
    info.meta = response.meta;
    this._client.metadata.setPreparedById(info);
    info.emit('prepared', null, info.queryId, info.meta);
  }

  _onPrepareError(err, host, triedHosts, info, iterator, query, keyspace) {
    if (err.isSocketError || err instanceof errors.OperationTimedOutError) {
      const self = this;
      triedHosts[host.address] = err;
      return self._prepareWithQueryPlan(info, iterator, triedHosts, query, keyspace);
    }
    info.preparing = false;
    err.query = query;
    return info.emit('prepared', err);
  }

  /**
   * Prepares all queries on a single host.
   * @param {Host} host
   * @param {Array} allPrepared
   * @param {Function} callback
   */
  static prepareAllQueries(host, allPrepared, callback) {
    const anyKeyspaceQueries = [];
    const queriesByKeyspace = {};
    allPrepared.forEach(function (info) {
      let arr = anyKeyspaceQueries;
      if (info.keyspace) {
        arr = queriesByKeyspace[info.keyspace] = (queriesByKeyspace[info.keyspace] || []);
      }
      arr.push(info.query);
    });
    utils.eachSeries(Object.keys(queriesByKeyspace), function eachKeyspace(keyspace, next) {
      PrepareHandler._borrowAndPrepare(host, keyspace, queriesByKeyspace[keyspace], next);
    }, function (err) {
      if (err) {
        return callback(err);
      }
      PrepareHandler._borrowAndPrepare(host, null, anyKeyspaceQueries, callback);
    });
  }

  /**
   * Borrows a connection from the host and prepares the queries provided.
   * @param {Host} host
   * @param {String} keyspace
   * @param {Array} queries
   * @param {Function} callback
   * @private
   */
  static _borrowAndPrepare(host, keyspace, queries, callback) {
    if (queries.length === 0) {
      return callback();
    }

    host.borrowConnection(keyspace, null, function borrowCallback(err, connection) {
      if (err) {
        return callback(err);
      }
      utils.each(queries, function prepareEach(query, next) {
        connection.prepareOnce(query, next);
      }, callback);
    });
  }

  /**
   * Prepares the provided query on all hosts, except the host provided.
   * @param {Object} info
   * @param {Object} response
   * @param {Host} hostToAvoid
   * @param {Iterator} iterator
   * @param {String} query
   * @param {String} keyspace
   * @private
   */
  _prepareOnAllHosts(info, response, hostToAvoid, iterator, query, keyspace) {
    const self = this;
    utils.each(utils.iteratorToArray(iterator), function (host, next) {
      if (host.address === hostToAvoid.address) {
        return next();
      }

      host.borrowConnection(keyspace, null, function borrowCallback(err, connection) {
        if (err) {
          // Don't mind about issues with the pool in this case
          return next();
        }
        connection.prepareOnce(query, function (err) {
          if (err) {
            // There has been error
            self.log('verbose', util.format('Unexpected error while preparing query (%s) on %s', query, host.address));
          }
          return next();
        });
      });
    }, function eachEnded() {
      self._onPrepareSuccess(info, response);
    });
  }
}

module.exports = PrepareHandler;