'use strict';
const util = require('util');

const errors = require('./errors');
const types = require('./types');
const utils = require('./utils');
const RequestExecution = require('./request-execution');

/**
 * Handles a BATCH, QUERY and EXECUTE request to the server, dealing with host fail-over and retries on error
 */
class RequestHandler {
  /**
   * Creates a new instance of RequestHandler.
   * @param {Request} request
   * @param {QueryOptions} options
   * @param {Client} client Client instance used to retrieve and set the keyspace.
   */
  constructor(request, options, client) {
    this.client = client;
    this.loadBalancingPolicy = options.executionProfile.loadBalancing;
    this.retryPolicy = options.retry;
    this._speculativeExecutionPlan = client.options.policies.speculativeExecution.newPlan(
      client.keyspace, request.query || request.queries);
    this.logEmitter = client.options.logEmitter;
    this.log = utils.log;
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
  static borrowNextConnection(iterator, triedHosts, profileManager, keyspace, callback) {
    triedHosts = triedHosts || {};
    const host = RequestHandler._getNextHost(iterator, profileManager, triedHosts);
    if (host === null) {
      return callback(new errors.NoHostAvailableError(triedHosts));
    }

    RequestHandler.borrowFromHost(host, keyspace, function borrowFromHostCallback(err, connection) {
      if (err) {
        triedHosts[host.address] = err;
        if (connection) {
          host.removeFromPool(connection);
        }
        // Issue on next tick to avoid large numbers of sync recursive calls
        return process.nextTick(() =>
          RequestHandler.borrowNextConnection(iterator, triedHosts, profileManager, keyspace, callback));
      }
      triedHosts[host.address] = null;
      callback(null, connection, host);
    });
  }

  /**
   * Borrows a connection from the provided host, changing the current keyspace, if necessary.
   * @param {Host} host
   * @param {String} keyspace
   * @param {Function} callback
   */
  static borrowFromHost(host, keyspace, callback) {
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
  }

  /**
   * Gets the next host from the query plan.
   * @param {Iterator} iterator
   * @param {ProfileManager} profileManager
   * @param {Object} triedHosts
   * @return {Host|null}
   * @private
   */
  static _getNextHost(iterator, profileManager, triedHosts) {
    let host;
    // Get a host that is UP in a sync loop
    while (true) {
      const item = iterator.next();
      if (item.done) {
        return null;
      }
      host = item.value;
      // set the distance relative to the client first
      const distance = profileManager.getDistance(host);
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
   * Sends a new BATCH, QUERY or EXECUTE request.
   * @param {Request} request
   * @param {QueryOptions} options
   * @param {Client} client Client instance used to retrieve and set the keyspace.
   * @param {Function} callback
   */
  static send(request, options, client, callback) {
    const instance = new RequestHandler(request, options, client);
    instance.send(callback);
  }

  /**
   * Gets a connection from the next host according to the query plan or a NoHostAvailableError.
   * @param {Function} callback
   */
  getNextConnection(callback) {
    RequestHandler.borrowNextConnection(
      this._hostIterator, this.triedHosts, this.client.profileManager, this.client.keyspace, callback);
  }

  /**
   * Gets an available connection and sends the request
   * @param {Function} callback
   */
  send(callback) {
    if (this.requestOptions.captureStackTrace) {
      Error.captureStackTrace(this.stackContainer = {});
    }
    const self = this;
    this.loadBalancingPolicy.newQueryPlan(this.client.keyspace, this.requestOptions, function newPlanCb(err, iterator) {
      if (err) {
        return callback(err);
      }
      self._hostIterator = iterator;
      self._callback = callback;
      self._startNewExecution();
    });
  }

  _startNewExecution() {
    const execution = new RequestExecution(this);
    this._executions.push(execution);
    const self = this;
    execution.start(function hostAcquired(host) {
      // This function is called when a connection to a host was successfully acquired and
      // the execution was not yet cancelled
      if (!self.requestOptions.isIdempotent) {
        return;
      }
      const delay = self._speculativeExecutionPlan.nextExecution(host);
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
      self._newExecutionTimeout = setTimeout(() => self._startNewExecution(), delay);
    });
  }

  /**
   * Sets the keyspace in any connection that is already opened.
   * @param {Client} client
   * @param {Function} callback
   */
  static setKeyspace(client, callback) {
    let connection;
    const hosts = client.hosts.values();
    for (let i = 0; i < hosts.length; i++) {
      const host = hosts[i];
      connection = host.getActiveConnection();
      if (connection) {
        break;
      }
    }
    if (!connection) {
      return callback(new errors.DriverInternalError('No active connection found'));
    }
    connection.changeKeyspace(client.keyspace, callback);
  }

  /**
   * @param {Error} err
   * @param {ResultSet} [result]
   */
  setCompleted(err, result) {
    if (this._newExecutionTimeout !== null) {
      clearTimeout(this._newExecutionTimeout);
    }
    // Mark all executions as cancelled
    for (let i = 0; i < this._executions.length; i++) {
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
  }

  /**
   * @param {NoHostAvailableError} err
   * @param {RequestExecution} sender
   */
  handleNoHostAvailable(err, sender) {
    // Remove the execution
    const index = this._executions.indexOf(sender);
    this._executions.splice(index, 1);
    if (this._executions.length === 0) {
      // There aren't any other executions, we should report back to the user that there isn't
      // a host available for executing the request
      this.setCompleted(err);
    }
  }
}

module.exports = RequestHandler;
