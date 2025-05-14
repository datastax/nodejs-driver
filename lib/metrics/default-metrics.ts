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
import EventEmitter from "events";
import type { AuthenticationError, OperationTimedOutError, ResponseError } from "../errors";
import ClientMetrics from "./client-metrics";



//TODO: The fields like errors were not exposed. I believe we should.
/**
 * A default implementation of [ClientMetrics]{@link module:metrics~ClientMetrics} that exposes the driver events as
 * Node.js events.
 * <p>
 *   An instance of [DefaultMetrics]{@link module:metrics~DefaultMetrics} is configured by default in the client,
 *   you can access this instance using [Client#metrics]{@link Client#metrics} property.
 * </p>
 * @implements {module:metrics~ClientMetrics}
 * @alias module:metrics~DefaultMetrics
 * @example <caption>Listening to events emitted</caption>
 * defaultMetrics.errors.on('increment', err => totalErrors++);
 * defaultMetrics.errors.clientTimeout.on('increment', () => clientTimeoutErrors++);
 * defaultMetrics.speculativeRetries.on('increment', () => specExecsCount++);
 * defaultMetrics.responses.on('increment', latency => myHistogram.record(latency));
 */
class DefaultMetrics extends ClientMetrics {
  errors: EventEmitter & {
    authentication: EventEmitter;
    clientTimeout: EventEmitter;
    connection: EventEmitter;
    other: EventEmitter;
    readTimeout: EventEmitter;
    unavailable: EventEmitter;
    writeTimeout: EventEmitter;
  };
  retries: EventEmitter & {
    clientTimeout: EventEmitter;
    other: EventEmitter;
    readTimeout: EventEmitter;
    unavailable: EventEmitter;
    writeTimeout: EventEmitter;
  };
  speculativeExecutions: EventEmitter & {
    increment: EventEmitter;
  };
  ignoredErrors: EventEmitter;
  responses: EventEmitter & {
    success: EventEmitter;
  };
  /**
   * Creates a new instance of [DefaultMetrics]{@link module:metrics~DefaultMetrics}.
   */
  constructor() {
    super();

    /**
     * Emits all the error events.
     * <p>Use each of the properties to measure events of specific errors.</p>
     * @type {EventEmitter}
     * @property {EventEmitter} authentication Emits the authentication timeout error events.
     * @property {EventEmitter} clientTimeout Emits the client timeout error events.
     * @property {EventEmitter} connection Emits the connection error events.
     * @property {EventEmitter} readTimeout Emits the read timeout error events obtained from the server.
     * @property {EventEmitter} other Emits the error events, that are not part of the other categories.
     * @property {EventEmitter} unavailable Emits the unavailable error events obtained from the server.
     * @property {EventEmitter} writeTimeout Emits the write timeout error events obtained from the server
     */
    // @ts-ignore
    this.errors = new EventEmitter();
    this.errors.authentication = new EventEmitter();
    this.errors.clientTimeout = new EventEmitter();
    this.errors.connection = new EventEmitter();
    this.errors.other = new EventEmitter();
    this.errors.readTimeout = new EventEmitter();
    this.errors.unavailable = new EventEmitter();
    this.errors.writeTimeout = new EventEmitter();

    /**
     * Emits all the retry events.
     * <p>Use each of the properties to measure events of specific retries.</p>
     * @type {EventEmitter}
     * @property {EventEmitter} clientTimeout Emits when an execution is retried as a result of an client timeout.
     * @property {EventEmitter} other Emits the error events, that are not part of the other categories.
     * @property {EventEmitter} readTimeout Emits an execution is retried as a result of an read timeout error from the
     * server (coordinator to replica).
     * @property {EventEmitter} unavailable Emits an execution is retried as a result of an unavailable error from the
     * server.
     * @property {EventEmitter} writeTimeout Emits an execution is retried as a result of a write timeout error from the
     * server (coordinator to replica).
     */
    // @ts-ignore
    this.retries = new EventEmitter();
    this.retries.clientTimeout = new EventEmitter();
    this.retries.other = new EventEmitter();
    this.retries.readTimeout = new EventEmitter();
    this.retries.unavailable = new EventEmitter();
    this.retries.writeTimeout = new EventEmitter();

    /**
     * Emits events when a speculative execution is started.
     * @type {EventEmitter}
     */
    // @ts-ignore
    this.speculativeExecutions = new EventEmitter();

    /**
     * Emits events when an error is ignored by the retry policy.
     * @type {EventEmitter}
     */
    // @ts-ignore
    this.ignoredErrors = new EventEmitter();

    /**
     * Emits events when a response message is obtained.
     * @type {EventEmitter}
     * @property {EventEmitter} success Emits when a response was obtained as the result of a successful execution.
     */
    // @ts-ignore
    this.responses = new EventEmitter();
    this.responses.success = new EventEmitter();
  }

  /** @override */
  onAuthenticationError(e: Error | AuthenticationError) {
    this.errors.authentication.emit('increment', e);
    this.errors.emit('increment', e);}

  /** @override */
  onConnectionError(e: Error) {
    this.errors.connection.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onReadTimeoutError(e : ResponseError) {
    this.errors.readTimeout.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onWriteTimeoutError(e: ResponseError) {
    this.errors.writeTimeout.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onUnavailableError(e: Error) {
    this.errors.unavailable.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onClientTimeoutError(e: OperationTimedOutError) {
    this.errors.clientTimeout.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onOtherError(e: Error) {
    this.errors.other.emit('increment', e);
    this.errors.emit('increment', e);
  }

  /** @override */
  onClientTimeoutRetry(e: Error) {
    this.retries.clientTimeout.emit('increment', e);
    this.retries.emit('increment', e);
  }

  /** @override */
  onOtherErrorRetry(e: Error) {
    this.retries.other.emit('increment', e);
    this.retries.emit('increment', e);
  }

  /** @override */
  onReadTimeoutRetry(e: Error) {
    this.retries.readTimeout.emit('increment', e);
    this.retries.emit('increment', e);
  }

  /** @override */
  onUnavailableRetry(e: Error) {
    this.retries.unavailable.emit('increment', e);
    this.retries.emit('increment', e);
  }

  /** @override */
  onWriteTimeoutRetry(e: Error) {
    this.retries.writeTimeout.emit('increment', e);
    this.retries.emit('increment', e);
  }

  /** @override */
  onIgnoreError(e: Error) {
    this.ignoredErrors.emit('increment', e);
  }

  /** @override */
  onSpeculativeExecution() {
    this.speculativeExecutions.emit('increment');
  }

  /** @override */
  onSuccessfulResponse(latency: number[]) {
    this.responses.success.emit('increment', latency);
  }

  /** @override */
  onResponse(latency: number[]) {
    this.responses.emit('increment', latency);
  }
}

export default DefaultMetrics;