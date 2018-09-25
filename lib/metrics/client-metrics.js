'use strict';

/**
 * Represents a base class that is used to measure events from the server and the client as seen by the driver.
 * @alias module:metrics~ClientMetrics
 * @interface
 */
class ClientMetrics {
  //TODO: Find usages of each
  /**
   * Method invoked when there is a connection error.
   * @param {Error} e The error encountered.
   */
  onConnectionError(e) {}

  /**
   * Method invoked when an authentication error is obtained from the server.
   * @param {ResponseError} e The error encountered.
   */
  onAuthenticationError(e) {}

  /**
   * Method invoked when a read timeout error is obtained from the server.
   * @param {ResponseError} e The error encountered.
   */
  onReadTimeoutError(e) {}

  /**
   * Method invoked when a write timeout error is obtained from the server.
   * @param {ResponseError} e The error encountered.
   */
  onWriteTimeoutError(e) {}

  /**
   * Method invoked when an unavailable error is obtained from the server.
   * @param {ResponseError} e The error encountered.
   */
  onUnavailableError(e) {}

  /**
   * Method invoked when an error (different than a server or client timeout, authentication or connection error) is
   * encountered when executing a request.
   * @param {OperationTimedOutError} e The timeout error.
   */
  onClientTimeoutError(e) {}

  /**
   * Method invoked when an error (different than a server or client timeout, authentication or connection error) is
   * encountered when executing a request.
   * @param {Error} e The error encountered.
   */
  onOtherError(e) {}

  /**
   * Method invoked when an execution is retried as a result of a client-level timeout.
   */
  onClientTimeoutRetry() {}

  /**
   * Method invoked when an execution is retried as a result of a read timeout from the server (coordinator to replica).
   */
  onReadTimeoutRetry() {}

  /**
   * Method invoked when an execution is retried as a result of an unavailable error from the server.
   */
  onUnavailableRetry() {}

  /**
   * Method invoked when an execution is retried as a result of a write timeout from the server (coordinator to
   * replica).
   */
  onWriteTimeoutRetry() {}

  /**
   * Method invoked when an error (other than a server or client timeout) is retried.
   * @param {Error} e The error that caused the retry.
   */
  onOtherErrorRetry(e) {}

  /**
   * Method invoked when an error is marked as ignored by the retry policy.
   */
  onIgnoreError(e) {}

  /**
   * Method invoked when a speculative execution is started.
   */
  onSpeculativeExecution() {}
}

module.exports = ClientMetrics;