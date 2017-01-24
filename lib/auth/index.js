/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
/**
 * DSE Authentication module.
 * <p>
 *   Contains the classes used for connecting to a DSE cluster secured with DseAuthenticator.
 * </p>
 * @module auth
 */
var baseProvider = require('./provider.js');
exports.AuthProvider = baseProvider.AuthProvider;
exports.Authenticator = baseProvider.Authenticator;
exports.PlainTextAuthProvider = require('./plain-text-auth-provider.js');
exports.DseGssapiAuthProvider = require('./dse-gssapi-auth-provider');
exports.DsePlainTextAuthProvider = require('./dse-plain-text-auth-provider');