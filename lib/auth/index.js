'use strict';
/**
 * DSE Authentication module.
 * <p>
 *   Contains the classes used for connecting to a DSE cluster secured with DseAuthenticator.
 * </p>
 * @module auth
 */
exports.DseGssapiAuthProvider = require('./dse-gssapi-auth-provider');
exports.DsePlainTextAuthProvider = require('./dse-plain-text-auth-provider');