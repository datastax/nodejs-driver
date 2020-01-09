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

const errors = require('./errors');
const types = require('./types');
const utils = require('./utils');
const RequestExecution = require('./request-execution');
const promiseUtils = require('./promise-utils');

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
    this._resolveCallback = null;
    this._rejectCallback = null;
    this._newExecutionTimeout = null;
    /** @type {RequestExecution[]} */
    this._executions = [];
  }

  /**
   * Sends a new BATCH, QUERY or EXECUTE request.
   * @param {Request} request
   * @param {ExecutionOptions} execOptions
   * @param {Client} client Client instance used to retrieve and set the keyspace.
   * @returns {Promise<ResultSet>}
   */
  static send(request, execOptions, client) {
    const instance = new RequestHandler(request, execOptions, client);
    return instance.send();
  }

  /**
   * Gets a connection from the next host according to the query plan or throws a NoHostAvailableError.
   * @returns {{host, connection}}
   * @throws {NoHostAvailableError}
   */
  getNextConnection() {
    let host;
    let connection;
    const iterator = this._hostIterator;

    // Get a host that is UP in a sync loop
    while (true) {
      const item = iterator.next();
      if (item.done) {
        throw new errors.NoHostAvailableError(this.triedHosts);
      }

      host = item.value;

      // Set the distance relative to the client first
      const distance = this.client.profileManager.getDistance(host);
      if (distance === types.distance.ignored) {
        //If its marked as ignore by the load balancing policy, move on.
        continue;
      }

      if (!host.isUp()) {
        this.triedHosts[host.address] = 'Host considered as DOWN';
        continue;
      }

      try {
        connection = host.borrowConnection();
        this.triedHosts[host.address] = null;
        break;
      } catch (err) {
        this.triedHosts[host.address] = err;
      }
    }

    return { connection, host };
  }

  /**
   * Gets an available connection and sends the request
   * @returns {Promise<ResultSet>}
   */
  send() {
    if (this.executionOptions.getCaptureStackTrace()) {
      Error.captureStackTrace(this.stackContainer = {});
    }

    return new Promise((resolve, reject) => {
      this._resolveCallback = resolve;
      this._rejectCallback = reject;

      const lbp = this.executionOptions.getLoadBalancingPolicy();
      const fixedHost = this.executionOptions.getFixedHost();

      if (fixedHost) {
        // if host is configured bypass load balancing policy and use
        // a single host plan.
        this._hostIterator = utils.arrayIterator([fixedHost]);
        promiseUtils.toBackground(this._startNewExecution());
      } else {
        lbp.newQueryPlan(this.client.keyspace, this.executionOptions, (err, iterator) => {
          if (err) {
            return reject(err);
          }

          this._hostIterator = iterator;
          promiseUtils.toBackground(this._startNewExecution());
        });
      }
    });
  }

  /**
   * Starts a new execution on the next host of the query plan.
   * @param {Boolean} [isSpecExec]
   * @returns {Promise<void>}
   * @private
   */
  async _startNewExecution(isSpecExec) {
    if (isSpecExec) {
      this.client.metrics.onSpeculativeExecution();
    }

    let host;
    let connection;

    try {
      ({ host, connection } = this.getNextConnection());
    } catch (err) {
      return this.handleNoHostAvailable(err, null);
    }

    if (isSpecExec && this._executions.length >= 0 && this._executions[0].wasCancelled()) {
      // This method was called on the next tick and could not be cleared, the previous execution was cancelled so
      // there's no point in launching a new execution.
      return;
    }

    if (this.client.keyspace && this.client.keyspace !== connection.keyspace) {
      try {
        await connection.changeKeyspace(this.client.keyspace);
      } catch (err) {
        this.triedHosts[host.address] = err;
        // The error occurred asynchronously
        // We can blindly re-try to obtain a different host/connection.
        return this._startNewExecution(isSpecExec);
      }
    }

    const execution = new RequestExecution(this, host, connection);
    this._executions.push(execution);
    execution.start();

    if (this.executionOptions.isIdempotent()) {
      this._scheduleSpeculativeExecution(host);
    }
  }

  /**
   * Schedules next speculative execution, if any.
   * @param {Host!} host
   * @private
   */
  _scheduleSpeculativeExecution(host) {
    const delay = this._speculativeExecutionPlan.nextExecution(host);
    if (typeof delay !== 'number' || delay < 0) {
      return;
    }

    if (delay === 0) {
      // Parallel speculative execution
      return process.nextTick(() => {
        promiseUtils.toBackground(this._startNewExecution(true));
      });
    }

    // Create timer for speculative execution
    this._newExecutionTimeout = setTimeout(() =>
      promiseUtils.toBackground(this._startNewExecution(true)), delay);
  }

  /**
   * Sets the keyspace in any connection that is already opened.
   * @param {Client} client
   * @returns {Promise}
   */
  static setKeyspace(client) {
    let connection;

    for (const host of client.hosts.values()) {
      connection = host.getActiveConnection();
      if (connection) {
        break;
      }
    }

    if (!connection) {
      throw new errors.DriverInternalError('No active connection found');
    }

    return connection.changeKeyspace(client.keyspace);
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
    for (const execution of this._executions) {
      execution.cancel();
    }

    if (err) {
      if (this.executionOptions.getCaptureStackTrace()) {
        utils.fixStack(this.stackContainer.stack, err);
      }

      // Reject the promise
      return this._rejectCallback(err);
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

    // We used to invoke the callback on next tick to allow stack unwinding and prevent the optimizing compiler to
    // optimize read and write functions together.
    // As we are resolving a Promise then() and catch() are always scheduled in the microtask queue
    // We can invoke the resolve method directly.
    this._resolveCallback(result);
  }

  /**
   * @param {NoHostAvailableError} err
   * @param {RequestExecution|null} execution
   */
  handleNoHostAvailable(err, execution) {
    if (execution !== null) {
      // Remove the execution
      const index = this._executions.indexOf(execution);
      this._executions.splice(index, 1);
    }

    if (this._executions.length === 0) {
      // There aren't any other executions, we should report back to the user that there isn't
      // a host available for executing the request
      this.setCompleted(err);
    }
  }

  /**
   * Gets a long lived closure that can fetch the next page.
   * @returns {Function}
   */
  getNextPageHandler() {
    const request = this.request;
    const execOptions = this.executionOptions;
    const client = this.client;

    return function nextPageHandler(pageState) {
      execOptions.setPageState(pageState);
      return new RequestHandler(request, execOptions, client).send();
    };
  }
}

module.exports = RequestHandler;
