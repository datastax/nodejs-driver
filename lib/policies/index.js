/**
 * Contains driver tuning policies to determine [load balancing]{@link module:policies/loadBalancing},
 *  [retrying]{@link module:policies/retry} queries, [reconnecting]{@link module:policies/reconnection} to a node
 *  and [address resolution]{@link module:policies/addressResolution}.
 * @module policies
 */
exports.loadBalancing = require('./load-balancing');
exports.reconnection = require('./reconnection');
exports.retry = require('./retry');
exports.addressResolution = require('./address-resolution');