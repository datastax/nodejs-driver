'use strict';

const ClientMetrics = require('./client-metrics');
const EventEmitter = require('events');

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
 * defaultMetrics.errors.on('increment', () => totalErrors++);
 * defaultMetrics.errors.clientTimeout.on('increment', () => clientTimeoutErrors++);
 * defaultMetrics.speculativeRetries.on('increment', () => specExecsCount++);
 */
class DefaultMetrics extends ClientMetrics {
  /**
   * Creates a new instance of [DefaultMetrics]{@link module:metrics~DefaultMetrics}.
   */
  constructor() {
    super();

    //TODO: Implement methods

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
    this.speculativeExecutions = new EventEmitter();

    /**
     * Emits events when an error is ignored by the retry policy.
     * @type {EventEmitter}
     */
    this.ignoredErrors = new EventEmitter();
  }

  /** @override */
  onReadTimeoutError(e) {
    this.errors.readTimeout.emit('increment', e);
    this.errors.emit('increment', e);
  }
}

module.exports = DefaultMetrics;