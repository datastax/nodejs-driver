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

const { Stream } = require('stream');
const utils = require('../utils');

/**
 * Utilities for concurrent query execution with the DataStax Node.js Driver.
 * @module concurrent
 */

/**
 * Executes multiple queries concurrently at the defined concurrency level.
 * @static
 * @param {Client} client The {@link Client} instance.
 * @param {String|Array<{query, params}>} query The query to execute per each parameter item.
 * @param {Array<Array>|Stream|Object} parameters An {@link Array} or a readable {@link Stream} composed of {@link Array}
 * items representing each individual set of parameters. Per each item in the {@link Array} or {@link Stream}, an
 * execution is going to be made.
 * @param {Object} [options] The execution options.
 * @param {String} [options.executionProfile] The execution profile to be used.
 * @param {Number} [options.concurrencyLevel=100] The concurrency level to determine the maximum amount of in-flight
 * operations at any given time
 * @param {Boolean} [options.raiseOnFirstError=true] Determines whether execution should stop after the first failed
 * execution and the corresponding exception will be raised.
 * @param {Boolean} [options.collectResults=false] Determines whether each individual
 * [ResultSet]{@link module:types~ResultSet} instance should be collected in the grouped result.
 * @param {Number} [options.maxErrors=100] The maximum amount of errors to be collected before ignoring the rest of
 * the error results.
 * @returns {Promise<ResultSetGroup>} A <code>Promise</code> of {@link ResultSetGroup} that is resolved when all the
 * executions completed and it's rejected when <code>raiseOnFirstError</code> is <code>true</code> and there is one
 * or more failures.
 * @example <caption>Using a fixed query and an Array of Arrays as parameters</caption>
 * const query = 'INSERT INTO table1 (id, value) VALUES (?, ?)';
 * const parameters = [[1, 'a'], [2, 'b'], [3, 'c'], ]; // ...
 * const result = await executeConcurrent(client, query, parameters);
 * @example <caption>Using a fixed query and a readable stream</caption>
 * const stream = csvStream.pipe(transformLineToArrayStream);
 * const result = await executeConcurrent(client, query, stream);
 * @example <caption>Using a different queries</caption>
 * const queryAndParameters = [
 *   { query: 'INSERT INTO videos (id, name, user_id) VALUES (?, ?, ?)',
 *     params: [ id, name, userId ] },
 *   { query: 'INSERT INTO user_videos (user_id, id, name) VALUES (?, ?, ?)',
 *     params: [ userId, id, name ] },
 *   { query: 'INSERT INTO latest_videos (id, name, user_id) VALUES (?, ?, ?)',
 *     params: [ id, name, userId ] },
 * ];
 *
 * const result = await executeConcurrent(client, queryAndParameters);
 */
function executeConcurrent(client, query, parameters, options) {
  if (!client) {
    throw new TypeError('Client instance is not defined');
  }

  if (typeof query === 'string') {
    if (Array.isArray(parameters)) {
      return new ArrayBasedExecutor(client, query, parameters, options).execute();
    }

    if (parameters instanceof Stream) {
      return new StreamBasedExecutor(client, query, parameters, options).execute();
    }

    throw new TypeError('parameters should be an Array or a Stream instance');
  }

  if (Array.isArray(query)) {
    options = parameters;
    return new ArrayBasedExecutor(client, null, query, options).execute();
  }

  throw new TypeError('A string query or query and parameters array should be provided');
}

/**
 * Wraps the functionality to execute given an Array.
 * @ignore
 */
class ArrayBasedExecutor {

  /**
   * @param {Client} client
   * @param {String} query
   * @param {Array<Array>|Array<{query, params}>} parameters
   * @param {Object} [options] The execution options.
   * @private
   */
  constructor(client, query, parameters, options) {
    this._client = client;
    this._query = query;
    this._parameters = parameters;
    options = options || utils.emptyObject;
    this._raiseOnFirstError = options.raiseOnFirstError !== false;
    this._concurrencyLevel = Math.min(options.concurrencyLevel || 100, this._parameters.length);
    this._queryOptions = { prepare: true, executionProfile: options.executionProfile };
    this._result = new ResultSetGroup(options);
    this._stop = false;
  }

  execute() {
    const promises = new Array(this._concurrencyLevel);

    for (let i = 0; i < this._concurrencyLevel; i++) {
      promises[i] = this._executeOneAtATime(i, 0);
    }

    return Promise.all(promises).then(() => this._result);
  }

  _executeOneAtATime(initialIndex, iteration) {
    const index = initialIndex + this._concurrencyLevel * iteration;

    if (index >= this._parameters.length || this._stop) {
      return Promise.resolve();
    }

    const item = this._parameters[index];
    let query;
    let params;

    if (this._query === null) {
      query = item.query;
      params = item.params;
    } else {
      query = this._query;
      params = item;
    }

    return this._client.execute(query, params, this._queryOptions)
      .then(rs => this._result.setResultItem(index, rs))
      .catch(err => this._setError(index, err))
      .then(() => this._executeOneAtATime(initialIndex, iteration + 1));
  }

  _setError(index, err) {
    this._result.setError(index, err);

    if (this._raiseOnFirstError) {
      this._stop = true;
      throw err;
    }
  }
}

/**
 * Wraps the functionality to execute given a Stream.
 * @ignore
 */
class StreamBasedExecutor {

  /**
   * @param {Client} client
   * @param {String} query
   * @param {Stream} stream
   * @param {Object} [options] The execution options.
   * @private
   */
  constructor(client, query, stream, options) {
    this._client = client;
    this._query = query;
    this._stream = stream;
    options = options || utils.emptyObject;
    this._raiseOnFirstError = options.raiseOnFirstError !== false;
    this._concurrencyLevel = options.concurrencyLevel || 100;
    this._queryOptions = { prepare: true, executionProfile: options.executionProfile };
    this._inFlight = 0;
    this._index = 0;
    this._result = new ResultSetGroup(options);
    this._resolveCallback = null;
    this._rejectCallback = null;
    this._readEnded = false;
  }

  execute() {
    return new Promise((resolve, reject) => {
      this._resolveCallback = resolve;
      this._rejectCallback = reject;

      this._stream
        .on('data', params => this._executeOne(params))
        .on('error', err => this._setReadEnded(err))
        .on('end', () => this._setReadEnded());
    });
  }

  _executeOne(params) {
    if (!Array.isArray(params)) {
      return this._setReadEnded(new TypeError('Stream should be in objectMode and should emit Array instances'));
    }

    if (this._readEnded) {
      // Read ended abruptly because of incorrect format or error event being emitted.
      // We shouldn't consider additional items.
      return;
    }

    const index = this._index++;
    this._inFlight++;

    this._client.execute(this._query, params, this._queryOptions)
      .then(rs => {
        this._result.setResultItem(index, rs);
        this._inFlight--;
      })
      .catch(err => {
        this._inFlight--;
        this._setError(index, err);
      })
      .then(() => {
        if (this._stream.isPaused()) {
          this._stream.resume();
        }

        if (this._readEnded && this._inFlight === 0) {
          // When read ended and there are no more in-flight requests
          // We yield the result to the user.
          // It could have ended prematurely when there is a read error
          // or there was an execution error and raiseOnFirstError is true
          // In that case, calling the resolve callback has no effect
          this._resolveCallback(this._result);
        }
      });

    if (this._inFlight >= this._concurrencyLevel) {
      this._stream.pause();
    }
  }

  /**
   * Marks the stream read process as ended.
   * @param {Error} [err] The stream read error.
   * @private
   */
  _setReadEnded(err) {
    if (!this._readEnded) {
      this._readEnded = true;

      if (err) {
        // There was an error while reading from the input stream.
        // This should be surfaced as a failure
        this._rejectCallback(err);
      } else if (this._inFlight === 0) {
        // Ended signaled and there are no more pending messages.
        this._resolveCallback(this._result);
      }
    }
  }

  _setError(index, err) {
    this._result.setError(index, err);

    if (this._raiseOnFirstError) {
      this._readEnded = true;
      this._rejectCallback(err);
    }
  }
}

/**
 * Represents results from different related executions.
 */
class ResultSetGroup {

  /**
   * Creates a new instance of {@link ResultSetGroup}.
   * @ignore
   */
  constructor(options) {
    this._collectResults = options.collectResults;
    this._maxErrors = options.maxErrors || 100;
    this.totalExecuted = 0;
    this.errors = [];

    if (this._collectResults) {
      /**
       * Gets an {@link Array} containing the [ResultSet]{@link module:types~ResultSet} instances from each execution.
       * <p>
       *   Note that when <code>collectResults</code> is set to <code>false</code>, accessing this property will
       *   throw an error.
       * </p>
       * @type {Array}
       */
      this.resultItems = [];
    } else {
      Object.defineProperty(this, 'resultItems', { enumerable: false, get: () => {
        throw new Error('Property resultItems can not be accessed when collectResults is set to false');
      }});
    }
  }

  /** @ignore */
  setResultItem(index, rs) {
    this.totalExecuted++;

    if (this._collectResults) {
      this.resultItems[index] = rs;
    }
  }

  /**
   * Internal method to set the error of an execution.
   * @ignore
   */
  setError(index, err) {
    this.totalExecuted++;

    if (this.errors.length < this._maxErrors) {
      this.errors.push(err);
    }

    if (this._collectResults) {
      this.resultItems[index] = err;
    }
  }
}

exports.executeConcurrent = executeConcurrent;
exports.ResultSetGroup = ResultSetGroup;