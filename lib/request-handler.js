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
   * @param {ExecutionOptions} execOptions
   * @param {Client} client Client instance used to retrieve and set the keyspace.
   */
  constructor(request, execOptions, client) {
    this.client = client;
    this._speculativeExecutionPlan = client.options.policies.speculativeExecution.newPlan(
      client.keyspace, request.query || request.queries);
    this.logEmitter = client.options.logEmitter;
    this.log = utils.log;
    this.request = request;
    this.executionOptions = execOptions;
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

    host.borrowConnection(keyspace, null, function borrowFromHostCallback(err, connection) {
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
   * @param {ExecutionOptions} execOptions
   * @param {Client} client Client instance used to retrieve and set the keyspace.
   * @param {Function} callback
   */
  static send(request, execOptions, client, callback) {
    const instance = new RequestHandler(request, execOptions, client);
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
    if (this.executionOptions.getCaptureStackTrace()) {
      Error.captureStackTrace(this.stackContainer = {});
    }

    const lbp = this.executionOptions.getLoadBalancingPolicy();
    const fixedHost = this.executionOptions.getFixedHost();
    const self = this;
    this._callback = callback;

    if (fixedHost) {
      // if host is configured bypass load balancing policy and use
      // a single host plan.
      self._hostIterator = utils.arrayIterator([fixedHost]);
      self._startNewExecution();
    } else {
      lbp.newQueryPlan(this.client.keyspace, this.executionOptions, function newPlanCb(err, iterator) {
        if (err) {
          return self._callback(err);
        }
        self._hostIterator = iterator;
        self._startNewExecution();
      });
    }
  }

  _startNewExecution(isSpecExec) {
    if (isSpecExec) {
      this.client.metrics.onSpeculativeExecution();
    }

    const execution = new RequestExecution(this);
    this._executions.push(execution);
    const self = this;
    execution.start(function hostAcquired(host) {
      // This function is called when a connection to a host was successfully acquired and
      // the execution was not yet cancelled
      if (!self.executionOptions.isIdempotent()) {
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

          self._startNewExecution(true);
        });
      }

      self._newExecutionTimeout = setTimeout(() => self._startNewExecution(true), delay);
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
      if (this.executionOptions.getCaptureStackTrace()) {
        utils.fixStack(this.stackContainer.stack, err);
      }

      // The error already has the stack information, there is no value in maintaining the call stack
      // for the callback invocation
      return process.nextTick(() => this._callback(err));
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

    // Invoke the callback in the next tick allowing stack unwinding, that way we can continue
    // processing the read queue before executing user code.
    // Additionally, we prevent the optimizing compiler to optimize read and write functions together.
    // FFR: We found corner cases where maintaining the call stack when invoking the user callback impacted the overall
    // performance of the driver. These corner cases appeared when adding more logic to the completion of the
    // request/response operation, that by itself had a negligible processing cost, but had a significant
    // performance penalty when integrated.
    process.nextTick(() => this._callback(null, result));
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
