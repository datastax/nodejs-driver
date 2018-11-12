'use strict';

/**
 * Tracks request execution for a {@link Client}.
 * <p>
 *   A {@link RequestTracker} can be configured in the client options. The <code>Client</code> will execute
 *   {@link RequestTracker#onSuccess} or {@link RequestTracker#onError} for every query or batch
 *   executed (QUERY, EXECUTE and BATCH requests).
 * </p>
 * @interface
 * @alias module:tracker~RequestTracker
 */
class RequestTracker {

  /**
   * Invoked each time a query or batch request succeeds.
   * @param {Host} host The node that acted as coordinator of the request.
   * @param {String|Array} query In the case of prepared or unprepared query executions, the provided
   * query string. For batch requests, an Array containing the queries and parameters provided.
   * @param {Array|Object|null} parameters In the case of prepared or unprepared query executions, the provided
   * parameters.
   * @param {ExecutionInfo} info The information related to the execution of the request.
   * @param {Number} requestLength Length of the body of the request.
   * @param {Number} responseLength Length of the body of the response.
   * @param {Array<Number>} latency An array containing [seconds, nanoseconds] tuple, where nanoseconds is the
   * remaining part of the real time that can't be represented in second precision (see <code>process.hrtime()</code>).
   */
  onSuccess(host, query, parameters, info, requestLength, responseLength, latency) {

  }

  /**
   * Invoked each time a query or batch request fails.
   * @param {Host} host The node that acted as coordinator of the request.
   * @param {String|Array} query In the case of prepared or unprepared query executions, the provided
   * query string. For batch requests, an Array containing the queries and parameters provided.
   * @param {Array|Object|null} parameters In the case of prepared or unprepared query executions, the provided
   * parameters.
   * @param {ExecutionInfo} info The information related to the execution of the request.
   * @param {Number} requestLength Length of the body of the request. When the failure occurred before the request was
   * written to the wire, the length will be <code>0</code>.
   * @param {Error} err The error that caused that caused the request to fail.
   * @param {Array<Number>} latency An array containing [seconds, nanoseconds] tuple, where nanoseconds is the
   * remaining part of the real time that can't be represented in second precision (see <code>process.hrtime()</code>).
   */
  onError(host, query, parameters, info, requestLength, err, latency) {

  }

  /**
   * Invoked when the Client is being shutdown.
   */
  shutdown() {

  }
}

module.exports = RequestTracker;