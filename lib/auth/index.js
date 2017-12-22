'use strict';
/**
 * Authentication module.
 * @module auth
 */
const baseProvider = require('./provider.js');
exports.AuthProvider = baseProvider.AuthProvider;
exports.Authenticator = baseProvider.Authenticator;
exports.PlainTextAuthProvider = require('./plain-text-auth-provider.js');