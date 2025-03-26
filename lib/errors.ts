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
import Long from "long";
import util from "util";

/**
 * Contains the error classes exposed by the driver.
 * @module errors
 */

/**
 * Base Error
 * @private
 */
class DriverError extends Error {
  info: string;
  isSocketError: boolean;
  innerError: any;
  requestNotWritten?: boolean;

  constructor(message: string) {
    super(message);
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.info = 'Cassandra Driver Error';
    this.message = message;
  }
}

/**
 * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
 */
class NoHostAvailableError extends DriverError {
  innerErrors: object;

  /**
   * Represents an error when a query cannot be performed because no host is available or could be reached by the driver.
   * @param {Object} innerErrors An object map containing the error per host tried
   * @param {String} [message]
   * @constructor
   */
  constructor(innerErrors: object, message?: string) {
    super(message);
    this.innerErrors = innerErrors;
    this.info = 'Represents an error when a query cannot be performed because no host is available or could be reached by the driver.';
    if (!message) {
      this.message = 'All host(s) tried for query failed.';
      if (innerErrors) {
        const hostList = Object.keys(innerErrors);
        if (hostList.length > 0) {
          const host = hostList[0];
          this.message += util.format(' First host tried, %s: %s. See innerErrors.', host, innerErrors[host]);
        }
      }
    }
  }
}

/**
 * Represents an error message from the server
 */
class ResponseError extends DriverError {
  code: number;
  consistencies: number;
  required: number;
  alive: number;
  received: number;
  blockFor: number;
  failures: number;
  reasons: object;
  isDataPresent: any;
  writeType: any;
  queryId: any;
  keyspace: any;
  functionName: any;
  argTypes: any[];
  table: any;

  /**
   * Represents an error message from the server
   * @param {Number} code Cassandra exception code
   * @param {String} message
   * @constructor
   */
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
    this.info = 'Represents an error message from the server';
  }
}

/**
 * Represents a bug inside the driver or in a Cassandra host.
 */
class DriverInternalError extends DriverError {
  /**
   * Represents a bug inside the driver or in a Cassandra host.
   * @param {String} message
   * @constructor
   */
  constructor(message: string) {
    super(message);
    this.info = 'Represents a bug inside the driver or in a Cassandra host.';
  }
}

/**
 * Represents an error when trying to authenticate with auth-enabled host
 */
class AuthenticationError extends DriverError {
  additionalInfo: ResponseError;
  /**
   * Represents an error when trying to authenticate with auth-enabled host
   * @param {String} message
   * @constructor
   */
  constructor(message: string) {
    super(message);
    this.info = 'Represents an authentication error from the driver or from a Cassandra node.';
  }
}

/**
 * Represents an error that is raised when one of the arguments provided to a method is not valid
 */
class ArgumentError extends DriverError {
  /**
   * Represents an error that is raised when one of the arguments provided to a method is not valid
   * @param {String} message
   * @constructor
   */
  constructor(message: string) {
    super(message);
    this.info = 'Represents an error that is raised when one of the arguments provided to a method is not valid.';
  }
}

/**
 * Represents a client-side error that is raised when the client didn't hear back from the server within
 * {@link ClientOptions.socketOptions.readTimeout}.
 */
class OperationTimedOutError extends DriverError {
  host?: string;

  /**
   * Represents a client-side error that is raised when the client didn't hear back from the server within
   * {@link ClientOptions.socketOptions.readTimeout}.
   * @param {String} message The error message.
   * @param {String} [host] Address of the server host that caused the operation to time out.
   * @constructor
   */
  constructor(message: string, host?: string) {
    super(message);
    this.info = 'Represents a client-side error that is raised when the client did not hear back from the server ' +
      'within socketOptions.readTimeout';
    /**
     * When defined, it gets the address of the host that caused the operation to time out.
     * @type {String|undefined}
     */
    this.host = host;
  }
}

/**
 * Represents an error that is raised when a feature is not supported in the driver or in the current Cassandra version.
 */
class NotSupportedError extends DriverError {

  /**
   * Represents an error that is raised when a feature is not supported in the driver or in the current Cassandra version.
   * @param message
   * @constructor
   */
  constructor(message: string) {
    super(message);
    this.info = 'Represents a feature that is not supported in the driver or in the Cassandra version.';
  }
}

/**
 * Represents a client-side error indicating that all connections to a certain host have reached
 * the maximum amount of in-flight requests supported.
 */
class BusyConnectionError extends DriverError {
  /**
   * Represents a client-side error indicating that all connections to a certain host have reached
   * the maximum amount of in-flight requests supported.
   * @param {String} address
   * @param {Number} maxRequestsPerConnection
   * @param {Number} connectionLength
   * @constructor
   */
  constructor(address: string, maxRequestsPerConnection: number, connectionLength: number) {
    const message = util.format('All connections to host %s are busy, %d requests are in-flight on %s',
      address, maxRequestsPerConnection, connectionLength === 1 ? 'a single connection' : 'each connection');
    super(message);
    this.info = 'Represents a client-side error indicating that all connections to a certain host have reached ' +
      'the maximum amount of in-flight requests supported (pooling.maxRequestsPerConnection)';
  }
}

/**
 * Represents a run-time exception when attempting to decode a vint and the JavaScript Number doesn't have enough space to fit the value that was decoded
 */
class VIntOutOfRangeException extends DriverError {
  /**
   * Represents a run-time exception when attempting to decode a vint and the JavaScript Number doesn't have enough space to fit the value that was decoded
   * @param {Long} long 
   */
  constructor(long: Long) {
    const message = `Value ${long.toString()} is out of range for a JavaScript Number`;
    super(message);
    this.info = 'Represents a run-time exception when attempting to decode a vint and the JavaScript Number doesn\'t have enough space to fit the value that was decoded';
  }
}

export default {
  ArgumentError,
  AuthenticationError,
  BusyConnectionError,
  DriverError,
  OperationTimedOutError,
  DriverInternalError,
  NoHostAvailableError,
  NotSupportedError,
  ResponseError,
  VIntOutOfRangeException
};

export {
  ArgumentError,
  AuthenticationError,
  BusyConnectionError,
  DriverError,
  OperationTimedOutError,
  DriverInternalError,
  NoHostAvailableError,
  NotSupportedError,
  ResponseError,
  VIntOutOfRangeException
};