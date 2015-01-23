/**
 * Contains driver tuning policies to determine [load balancing]{@link module:policies/loadBalancing},
 *  [retrying]{@link module:policies/retry} queries, and [reconnecting]{@link module:policies/reconnection} to a node.
 * @module policies
 */
exports.loadBalancing = require('./load-balancing.js');
exports.reconnection = require('./reconnection.js');
exports.retry = require('./retry.js');