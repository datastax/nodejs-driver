/**
 * Authentication module.
 * @module auth
 */
var baseProvider = require('./provider.js');
exports.AuthProvider = baseProvider.AuthProvider;
exports.Authenticator = baseProvider.Authenticator;
exports.PlainTextAuthProvider = require('./plain-text-auth-provider.js');