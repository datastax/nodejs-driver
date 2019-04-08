'use strict';

const utils = require('../utils');

/**
 * Utilities for Concurrent Query Execution with the DataStax Node.js Driver.
 * @module concurrent
 */

/**
 * Executes multiple queries concurrently at the defined concurrency level.
 * @static
 * @param {Client} client The {@link Client} instance.
 * @param {String} query The query to execute per each parameter item.
 * @param {Array<Array>|Stream} parameters An {@link Array} or a readable {@link Stream} composed of {@link Array}
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
 */
function executeConcurrent(client, query, parameters, options) {
  //TODO: Validate parameters
  if (Array.isArray(parameters)) {
    return new ArrayBasedExecutor(client, query, parameters, options).execute();
  }
  return new StreamBasedExecutor(client, query, parameters, options).execute();
}

/**
 * Wraps the functionality to execute given an Array.
 * @ignore
 */
class ArrayBasedExecutor {

  /**
   * @param {Client} client
   * @param {String} query
   * @param {Array<Array>} parameters
   * @param {Object} [options] The execution options.
   * @private
   */
  constructor(client, query, parameters, options) {
    this._client = client;
    this._query = query;
    this._parameters = parameters;
    this._options = options || utils.emptyObject;
    this._concurrencyLevel = Math.min(this._options.concurrencyLevel || 100, this._parameters.length);
    this._queryOptions = { prepare: true, executionProfile: this._options.executionProfile };
    this._result = new ResultSetGroup(this._options);
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

    const params = this._parameters[index];
    return this._client.execute(this._query, params, this._queryOptions)
      .then(rs => this._result.setResultItem(index, rs))
      .catch(err => this._setError(index, err))
      .then(() => this._executeOneAtATime(initialIndex, iteration + 1));
  }

  _setError(index, err) {
    this._result.setError(index, err);

    if (this._options.raiseOnFirstError) {
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
    this._options = options || utils.emptyObject;
    this._concurrencyLevel = Math.min(this._options.concurrencyLevel || 100, this._parameters.length);
    this._queryOptions = { prepare: true, executionProfile: this._options.executionProfile };
    this._inFlight = 0;
    this._result = new ResultSetGroup(this._options);
    this._resolveCallback = null;
    this._rejectCallback = null;
    this._readEnded = false;
    this._readError = null;
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
    this._inFlight++;

    this._client.execute(this._query, params, this._queryOptions)
      .then(rs => {
        this._result.setResultItem(rs);
        this._inFlight--;
      })
      .catch(err => {
        this._inFlight--;
        this._setError(err);
      })
      .then(() => {
        if (this._stream.isPaused()) {
          this._stream.resume();
        }

        if (this._readEnded && this._inFlight === 0) {
          // TODO: Validate readError or validate result error
          this._resolveCallback(this._result);
        }
      });

    if (this._inFlight >= this._concurrencyLevel) {
      this._stream.pause();
    }
  }

  _setReadEnded(err) {
    if (!this._readEnded) {
      this._readEnded = true;
      this._readError = err;
    }
  }

  _setError(err) {
    //TODO: fail or continue
    this._result.setError(err);
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
    this._options = options;
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