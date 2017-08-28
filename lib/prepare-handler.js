'use strict';

var util = require('util');
var errors = require('./errors');
var utils = require('./utils');
var RequestHandler = require('./request-handler');

/**
 * Encapsulates the logic for dealing with the different prepare request and response flows, including failover when
 * trying to prepare a query.
 * @param {Client} client
 * @param {LoadBalancingPolicy} loadBalancing
 * @constructor
 */
function PrepareHandler(client, loadBalancing) {
  this._client = client;
  this.logEmitter = client.options.logEmitter;
  this._loadBalancing = loadBalancing;
}

/**
 * @param {Client} client
 * @param {LoadBalancingPolicy} loadBalancing
 * @param {String} query
 * @param {String} keyspace
 * @param {Function} callback
 * @static
 */
PrepareHandler.getPrepared = function (client, loadBalancing, query, keyspace, callback) {
  var info = client.metadata.getPreparedInfo(keyspace, query);
  if (info.queryId) {
    return callback(null, info.queryId, info.meta);
  }
  info.once('prepared', callback);
  if (info.preparing) {
    // It's already being prepared
    return;
  }
  var instance = new PrepareHandler(client, loadBalancing);
  instance._prepare(info, query, keyspace);
};

/**
 * @param {Client} client
 * @param {LoadBalancingPolicy} loadBalancing
 * @param {Array} queries
 * @param {String} keyspace
 * @param {Function} callback
 * @static
 */
PrepareHandler.getPreparedMultiple = function (client, loadBalancing, queries, keyspace, callback) {
  var result = new Array(queries.length);
  utils.forEachOf(queries, function eachQuery(item, index, next) {
    var query;
    if (item) {
      query = typeof item === 'string' ? item : item.query;
    }
    if (typeof query !== 'string') {
      return next(new errors.ArgumentError('Query item should be a string'));
    }
    PrepareHandler.getPrepared(client, loadBalancing, query, keyspace, function getPrepareCb(err, id, meta) {
      result[index] = {
        query: query,
        params: item.params,
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
};

/**
 * Prepares the query on a single host or on all hosts depending on the options.
 * Uses the info 'prepared' event to emit the result.
 * @param {Object} info
 * @param {String} query
 * @param {String} keyspace
 */
PrepareHandler.prototype._prepare = function (info, query, keyspace) {
  info.preparing = true;
  var self = this;
  this._loadBalancing.newQueryPlan(keyspace, null, function (err, iterator) {
    if (err) {
      info.preparing = false;
      return info.emit('prepared', err);
    }
    self._prepareWithQueryPlan(info, iterator, null, query, keyspace);
  });
};

/**
 * @param {Object} info
 * @param {Iterator} iterator
 * @param {Object|null} triedHosts
 * @param {String} query
 * @param {String} keyspace
 * @private
 */
PrepareHandler.prototype._prepareWithQueryPlan = function (info, iterator, triedHosts, query, keyspace) {
  triedHosts = triedHosts || {};
  var self = this;
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
};

PrepareHandler.prototype._onPrepareSuccess = function (info, response) {
  info.preparing = false;
  info.queryId = response.id;
  info.meta = response.meta;
  this._client.metadata.setPreparedById(info);
  info.emit('prepared', null, info.queryId, info.meta);
};

PrepareHandler.prototype._onPrepareError = function (err, host, triedHosts, info, iterator, query, keyspace) {
  if (err.isSocketError || err instanceof errors.OperationTimedOutError) {
    var self = this;
    triedHosts[host.address] = err;
    return self._prepareWithQueryPlan(info, iterator, triedHosts, query, keyspace);
  }
  info.preparing = false;
  err.query = query;
  return info.emit('prepared', err);
};

/**
 * Prepares all queries on a single host.
 * @param {Host} host
 * @param {Array} allPrepared
 * @param {Function} callback
 */
PrepareHandler.prepareAllQueries = function (host, allPrepared, callback) {
  var anyKeyspaceQueries = [];
  var queriesByKeyspace = {};
  allPrepared.forEach(function (info) {
    var arr = anyKeyspaceQueries;
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
};

/**
 * Borrows a connection from the host and prepares the queries provided.
 * @param {Host} host
 * @param {String} keyspace
 * @param {Array} queries
 * @param {Function} callback
 * @private
 */
PrepareHandler._borrowAndPrepare = function (host, keyspace, queries, callback) {
  if (queries.length === 0) {
    return callback();
  }
  RequestHandler.borrowFromHost(host, keyspace, function borrowCallback(err, connection) {
    if (err) {
      return callback(err);
    }
    utils.each(queries, function prepareEach(query, next) {
      connection.prepareOnce(query, next);
    }, callback);
  });
};

PrepareHandler.prototype.log = utils.log;

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
PrepareHandler.prototype._prepareOnAllHosts = function (info, response, hostToAvoid, iterator, query, keyspace) {
  var self = this;
  utils.each(utils.iteratorToArray(iterator), function (host, next) {
    if (host.address === hostToAvoid.address) {
      return next();
    }
    RequestHandler.borrowFromHost(host, keyspace, function borrowCallback(err, connection) {
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
};

module.exports = PrepareHandler;