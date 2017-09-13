'use strict';
var util = require('util');

var errors = require('./errors');
var types = require('./types');
var utils = require('./utils');
var RequestExecution = require('./request-execution');

/**
 * Handles a request to Cassandra, dealing with host fail-over and retries on error
 * @param {Request} request
 * @param {QueryOptions} options
 * @param {Client} client Client instance used to retrieve and set the keyspace.
 * @constructor
 */
function RequestHandler(request, options, client) {
  this.client = client;
  this.loadBalancingPolicy = options.executionProfile.loadBalancing;
  this.retryPolicy = options.retry;
  this._speculativeExecutionPlan = client.options.policies.speculativeExecution.newPlan(
    client.keyspace, request.query || request.queries);
  this.logEmitter = client.options.logEmitter;
  this.request = request;
  this.requestOptions = options;
  this.stackContainer = null;
  this.triedHosts = {};
  // start at -1 as first request does not count.
  this.speculativeExecutions = -1;
  this._hostIterator = null;
  this._callback = null;
  this._newExecutionTimeout = null;
  this._executions = [];
}

/**
 * Borrows a connection iterating from the query plan one or more times, until finding an open connection with the
 * keyspace set.
 * It invokes the callback with the err, connection and host as parameters.
 * The error can only be a NoHostAvailableError instance.
 * @param {Iterator} iterator
 * @param {Object} triedHosts
 * @param {ProfileManager} profileManager
 * @param {String} keyspace
 * @param {Function} callback
 */
RequestHandler.borrowNextConnection = function(iterator, triedHosts, profileManager, keyspace, callback) {
  triedHosts = triedHosts || {};
  var host = getNextHost(iterator, profileManager, triedHosts);
  if (host === null) {
    return callback(new errors.NoHostAvailableError(triedHosts));
  }

  RequestHandler.borrowFromHost(host, keyspace, function borrowFromHostCallback(err, connection) {
    if (err) {
      triedHosts[host.address] = err;
      if (connection) {
        host.removeFromPool(connection);
      }
      // The error occurred on a different tick, so there is no risk of issuing a large number sync recursive calls
      return RequestHandler.borrowNextConnection(iterator, triedHosts, profileManager, keyspace, callback);
    }
    triedHosts[host.address] = null;
    callback(null, connection, host);
  });
};

/**
 * Borrows a connection from the provided host, changing the current keyspace, if necessary.
 * @param {Host} host
 * @param {String} keyspace
 * @param {Function} callback
 */
RequestHandler.borrowFromHost = function (host, keyspace, callback) {
  host.borrowConnection(function (err, connection) {
    if (err) {
      return callback(err);
    }
    if (!keyspace || keyspace === connection.keyspace) {
      // Connection is ready to be used
      return callback(null, connection);
    }
    connection.changeKeyspace(keyspace, function (err) {
      if (err) {
        return callback(err, connection);
      }
      callback(null, connection);
    });
  });
};

/**
 * Gets the next host from the query plan.
 * @param {Iterator} iterator
 * @param {ProfileManager} profileManager
 * @param {Object} triedHosts
 * @return {Host|null}
 * @private
 */
function getNextHost(iterator, profileManager, triedHosts) {
  var host;
  // Get a host that is UP in a sync loop
  while (true) {
    var item = iterator.next();
    if (item.done) {
      return null;
    }
    host = item.value;
    // set the distance relative to the client first
    var distance = profileManager.getDistance(host);
    if (distance === types.distance.ignored) {
      //If its marked as ignore by the load balancing policy, move on.
      continue;
    }
    if (host.isUp()) {
      break;
    }
    triedHosts[host.address] = 'Host considered as DOWN';
  }
  return host;
}

/**
 * Gets a connection from the next host according to the query plan or a NoHostAvailableError.
 * @param {Function} callback
 */
RequestHandler.prototype.getNextConnection = function (callback) {
  RequestHandler.borrowNextConnection(
    this._hostIterator, this.triedHosts, this.client.profileManager, this.client.keyspace, callback);
};

RequestHandler.prototype.log = utils.log;

/**
 * Gets an available connection and sends the request
 * @param {Function} callback
 */
RequestHandler.prototype.send = function (callback) {
  if (this.requestOptions.captureStackTrace) {
    Error.captureStackTrace(this.stackContainer = {});
  }
  var self = this;
  this.loadBalancingPolicy.newQueryPlan(this.client.keyspace, this.requestOptions, function newPlanCb(err, iterator) {
    if (err) {
      return callback(err);
    }
    self._hostIterator = iterator;
    self._callback = callback;
    self._startNewExecution();
  });
};

RequestHandler.prototype._startNewExecution = function () {
  var execution = new RequestExecution(this);
  this._executions.push(execution);
  var self = this;
  execution.start(function hostAcquired(host) {
    // This function is called when a connection to a host was successfully acquired and
    // the execution was not yet cancelled
    if (!self.requestOptions.isIdempotent) {
      return;
    }
    var delay = self._speculativeExecutionPlan.nextExecution(host);
    if (typeof delay !== 'number' || delay < 0) {
      return;
    }
    if (delay === 0) {
      // Multiple parallel executions
      return process.nextTick(function startNextInParallel() {
        // Unlike timers process.nextTick() handlers can't be cleared so we must be sure that the
        // the previous execution wasn't cancelled before issuing the next one.
        if (execution.wasCancelled()) {
          return;
        }
        self._startNewExecution();
      });
    }
    self._newExecutionTimeout = setTimeout(function startNextAfterDelay() {
      self._startNewExecution();
    }, delay);
  });
};

/**
 * Sets the keyspace in any connection that is already opened.
 * @param {Client} client
 * @param {Function} callback
 */
RequestHandler.setKeyspace = function (client, callback) {
  var connection;
  var hosts = client.hosts.values();
  for (var i = 0; i < hosts.length; i++) {
    var host = hosts[i];
    connection = host.getActiveConnection();
    if (connection) {
      break;
    }
  }
  if (!connection) {
    return callback(new errors.DriverInternalError('No active connection found'));
  }
  connection.changeKeyspace(client.keyspace, callback);
};

/**
 * @param {Error} err
 * @param {ResultSet} [result]
 */
RequestHandler.prototype.setCompleted = function (err, result) {
  if (this._newExecutionTimeout !== null) {
    clearTimeout(this._newExecutionTimeout);
  }
  // Mark all executions as cancelled
  for (var i = 0; i < this._executions.length; i++) {
    this._executions[i].cancel();
  }
  if (err) {
    if (this.requestOptions.captureStackTrace) {
      utils.fixStack(this.stackContainer.stack, err);
    }
    return this._callback(err);
  }
  if (result.info.warnings) {
    // Log the warnings from the response
    result.info.warnings.forEach(function (message, i, warnings) {
      this.log('warning', util.format(
        'Received warning (%d of %d) "%s" for "%s"',
        i + 1,
        warnings.length,
        message,
        this.request.query || 'batch'));
    }, this);
  }
  this._callback(null, result);
};

/**
 * @param {NoHostAvailableError} err
 * @param {RequestExecution} sender
 */
RequestHandler.prototype.handleNoHostAvailable = function (err, sender) {
  // Remove the execution
  var index = this._executions.indexOf(sender);
  this._executions.splice(index, 1);
  if (this._executions.length === 0) {
    // There aren't any other executions, we should report back to the user that there isn't
    // a host available for executing the request
    this.setCompleted(err);
  }
};

module.exports = RequestHandler;
