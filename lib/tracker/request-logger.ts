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

const events = require('events');
const RequestTracker = require('./request-tracker');
const errors = require('../errors');
const { format } = require('util');

const nanosToMillis = 1000000;
const defaultMessageMaxQueryLength = 500;
const defaultMaxParameterValueLength = 50;
const defaultMaxErrorStackTraceLength = 200;

/**
 * A request tracker that logs the requests executed through the session, according to a set of
 * configurable options.
 * @implements {module:tracker~RequestTracker}
 * @alias module:tracker~RequestLogger
 * @example <caption>Logging slow queries</caption>
 * const requestLogger = new RequestLogger({ slowThreshold: 1000 });
 * requestLogger.emitter.on('show', message => console.log(message));
 * // Add the requestLogger to the client options
 * const client = new Client({ contactPoints, requestTracker: requestLogger });
 */
class RequestLogger extends RequestTracker {

  /**
   * Creates a new instance of {@link RequestLogger}.
   * @param {Object} options
   * @param {Number} [options.slowThreshold] The threshold in milliseconds beyond which queries are considered 'slow'
   * and logged as such by the driver.
   * @param {Number} [options.requestSizeThreshold] The threshold in bytes beyond which requests are considered 'large'
   * and logged as such by the driver.
   * @param {Boolean} [options.logNormalRequests] Determines whether it should emit 'normal' events for every
   * EXECUTE, QUERY and BATCH request executed successfully, useful only for debugging. This option can be modified
   * after the client is connected using the property {@link RequestLogger#logNormalRequests}.
   * @param {Boolean} [options.logErroredRequests] Determines whether it should emit 'failure' events for every
   * EXECUTE, QUERY and BATCH request execution that resulted in an error. This option can be modified
   * after the client is connected using the property {@link RequestLogger#logErroredRequests}.
   * @param {Number} [options.messageMaxQueryLength] The maximum amount of characters that are logged from the query
   * portion of the message. Defaults to 500.
   * @param {Number} [options.messageMaxParameterValueLength] The maximum amount of characters of each query parameter
   * value that will be included in the message. Defaults to 50.
   * @param {Number} [options.messageMaxErrorStackTraceLength] The maximum amount of characters of the stack trace
   * that will be included in the message. Defaults to 200.
   */
  constructor(options) {
    super();
    if (!options) {
      throw new errors.ArgumentError('RequestLogger options parameter is required');
    }

    this._options = options;

    /**
     * Determines whether it should emit 'normal' events for every EXECUTE, QUERY and BATCH request executed
     * successfully, useful only for debugging
     * @type {Boolean}
     */
    this.logNormalRequests = this._options.logNormalRequests;

    /**
     * Determines whether it should emit 'failure' events for every EXECUTE, QUERY and BATCH request execution that
     * resulted in an error
     * @type {Boolean}
     */
    this.logErroredRequests = this._options.logErroredRequests;

    /**
     * The object instance that emits <code>'slow'</code>, <code>'large'</code>, <code>'normal'</code> and
     * <code>'failure'</code> events.
     * @type {EventEmitter}
     */
    this.emitter = new events.EventEmitter();
  }

  /**
   * Logs message if request execution was deemed too slow, large or if normal requests are logged.
   * @override
   */
  onSuccess(host, query, parameters, execOptions, requestLength, responseLength, latency) {
    if (this._options.slowThreshold > 0 && toMillis(latency) > this._options.slowThreshold) {
      this._logSlow(host, query, parameters, execOptions, requestLength, responseLength, latency);
    }
    else if (this._options.requestSizeThreshold > 0 && requestLength > this._options.requestSizeThreshold) {
      this._logLargeRequest(host, query, parameters, execOptions, requestLength, responseLength, latency);
    }
    else if (this.logNormalRequests) {
      this._logNormalRequest(host, query, parameters, execOptions, requestLength, responseLength, latency);
    }
  }

  /**
   * Logs message if request execution was too large and/or encountered an error.
   * @override
   */
  onError(host, query, parameters, execOptions, requestLength, err, latency) {
    if (this._options.requestSizeThreshold > 0 && requestLength > this._options.requestSizeThreshold) {
      this._logLargeErrorRequest(host, query, parameters, execOptions, requestLength, err, latency);
    }
    else if (this.logErroredRequests) {
      this._logErrorRequest(host, query, parameters, execOptions, requestLength, err, latency);
    }
  }

  _logSlow(host, query, parameters, execOptions, requestLength, responseLength, latency) {
    const message = format('[%s] Slow request, took %d ms (%s): %s', host.address, Math.floor(toMillis(latency)),
      getPayloadSizes(requestLength, responseLength), getStatementInfo(query, parameters, execOptions, this._options));
    this.emitter.emit('slow', message);
  }

  _logLargeRequest(host, query, parameters, execOptions, requestLength, responseLength, latency) {
    const message = format('[%s] Request exceeded length, %s (took %d ms): %s', host.address,
      getPayloadSizes(requestLength, responseLength), ~~toMillis(latency),
      getStatementInfo(query, parameters, execOptions, this._options));
    this.emitter.emit('large', message);
  }

  _logNormalRequest(host, query, parameters, execOptions, requestLength, responseLength, latency) {
    const message = format('[%s] Request completed normally, took %d ms (%s): %s', host.address, ~~toMillis(latency),
      getPayloadSizes(requestLength, responseLength), getStatementInfo(query, parameters, execOptions, this._options));
    this.emitter.emit('normal', message);
  }

  _logLargeErrorRequest(host, query, parameters, execOptions, requestLength, err, latency) {
    const maxStackTraceLength = this._options.messageMaxErrorStackTraceLength || defaultMaxErrorStackTraceLength;
    const message = format('[%s] Request exceeded length and execution failed, %s (took %d ms): %s; error: %s',
      host.address, getPayloadSizes(requestLength), ~~toMillis(latency),
      getStatementInfo(query, parameters, execOptions, this._options), err.stack.substr(0, maxStackTraceLength));

    // Use 'large' event and not 'failure' as this log is caused by exceeded length
    this.emitter.emit('large', message);
  }

  _logErrorRequest(host, query, parameters, execOptions, requestLength, err, latency) {
    const maxStackTraceLength = this._options.messageMaxErrorStackTraceLength || defaultMaxErrorStackTraceLength;
    const message = format('[%s] Request execution failed, took %d ms (%s): %s; error: %s', host.address,
      ~~toMillis(latency), getPayloadSizes(requestLength),
      getStatementInfo(query, parameters, execOptions, this._options), err.stack.substr(0, maxStackTraceLength));

    // Avoid using 'error' as its a special event
    this.emitter.emit('failure', message);
  }
}

function toMillis(latency) {
  return latency[0] * 1000 + latency[1] / nanosToMillis;
}

function getStatementInfo(query, parameters, execOptions, options) {
  const maxQueryLength = options.messageMaxQueryLength || defaultMessageMaxQueryLength;
  const maxParameterLength = options.messageMaxParameterValueLength || defaultMaxParameterValueLength;

  if (Array.isArray(query)) {
    return getBatchStatementInfo(query, execOptions, maxQueryLength, maxParameterLength);
  }

  // String concatenation is usually faster than Array#join() in V8
  let message = query.substr(0, maxQueryLength);
  const remaining = maxQueryLength - message.length - 1;
  message += getParametersInfo(parameters, remaining, maxParameterLength);

  if (!execOptions.isPrepared()) {
    // This part of the message is not accounted for in "maxQueryLength"
    message += ' (not prepared)';
  }

  return message;
}

function getBatchStatementInfo(queries, execOptions, maxQueryLength, maxParameterLength) {
  // This part of the message is not accounted for in "maxQueryLength"
  let message = (execOptions.isBatchLogged() ? 'LOGGED ' : '') + 'BATCH w/ ' + queries.length +
    (!execOptions.isPrepared() ? ' not prepared' : '') + ' queries (';
  let remaining = maxQueryLength;
  let i;

  for (i = 0; i < queries.length && remaining > 0; i++) {
    let q = queries[i];
    const params = q.params;
    if (typeof q !== 'string') {
      q = q.query;
    }

    if (i > 0) {
      message += ',';
      remaining--;
    }

    const queryLength = Math.min(remaining, q.length);
    message += q.substr(0, queryLength);
    remaining -= queryLength;

    if (remaining <= 0) {
      break;
    }

    const parameters = getParametersInfo(params, remaining, maxParameterLength);
    remaining -= parameters.length;
    message += parameters;
  }

  message += i < queries.length ? ',...)' : ')';
  return message;
}

function getParametersInfo(params, remaining, maxParameterLength) {
  if (remaining <= 3) {
    // We need at least 3 chars to describe the parameters
    // its OK to add more chars in an effort to be descriptive
    return ' [...]';
  }

  if (!params) {
    return ' []';
  }

  let paramStringifier = (index, length) => formatParam(params[index], length);
  if (!Array.isArray(params)) {
    const obj = params;
    params = Object.keys(params);
    paramStringifier = (index, length) => {
      const key = params[index];
      let result = key.substr(0, length);
      const rem = length - result.length - 1;
      if (rem <= 0) {
        return result;
      }
      result += ":" + formatParam(obj[key], rem);
      return result;
    };
  }

  let message = ' [';
  let i;
  for (i = 0; remaining > 0 && i < params.length; i++) {
    if (i > 0) {
      message += ',';
      remaining--;
    }

    const paramString = paramStringifier(i, Math.min(maxParameterLength, remaining));
    remaining -= paramString.length;
    message += paramString;
  }

  if (i < params.length) {
    message += '...';
  }

  message += ']';
  return message;
}

function formatParam(value, maxLength) {
  if (value === undefined) {
    return 'undefined';
  }

  if (value === null) {
    return 'null';
  }

  return value.toString().substr(0, maxLength);
}

function getPayloadSizes(requestLength, responseLength) {
  let message = 'request size ' + formatSize(requestLength);
  if (responseLength !== undefined) {
    message += ' / response size ' + formatSize(responseLength);
  }
  return message;
}

function formatSize(length) {
  return length > 1000 ? Math.round(length / 1024) + ' KB' : length + ' bytes';
}

module.exports = RequestLogger;