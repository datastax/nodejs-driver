'use strict';

const RequestTracker = require('./request-tracker');

/**
 * A request tracker that logs the requests executed through the session, according to a set of
 * configurable options.
 * @implements {module:tracker~RequestTracker}
 * @alias module:tracker~RequestLogger
 */
class RequestLogger extends RequestTracker {

  /**
   * Logs a message when a request execution was deemed slow or too large.
   * @override
   */
  onSuccess(host, query, parameters, queryOptions, requestLength, responseLength, latency) {
    // TODO: Implement
  }

  /**
   * Logs a message when a request execution was deemed too large.
   * @override
   */
  onError(host, query, parameters, queryOptions, requestLength, err, latency) {
    // TODO: Implement
  }
}

module.exports = RequestLogger;