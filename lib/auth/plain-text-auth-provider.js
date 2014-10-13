var util = require('util');

var provider = require('./provider.js');
var AuthProvider = provider.AuthProvider;
var Authenticator = provider.Authenticator;
/** @module auth */
/**
 * Provides {@link Authenticator} instances to be used when connecting to a host.
 */
function PlainTextAuthProvider(username, password) {
  this.username = username;
  this.password = password;
}

util.inherits(PlainTextAuthProvider, AuthProvider);

/**
 * Returns an {@link PlainTextAuthenticator} instance to be used when connecting to a host.
 * @returns {PlainTextAuthenticator}
 */
PlainTextAuthProvider.prototype.newAuthenticator = function () {
  return new PlainTextAuthenticator(this.username, this.password);
};

/**
 * @constructor
 */
function PlainTextAuthenticator(username, password) {
  this.username = username;
  this.password = password;
}

util.inherits(PlainTextAuthenticator, Authenticator);

PlainTextAuthenticator.prototype.initialResponse = function (callback) {
  var initialToken = Buffer.concat([
    new Buffer([0]),
    new Buffer(this.username, 'utf8'),
    new Buffer([0]),
    new Buffer(this.password, 'utf8')
  ]);
  callback(null, initialToken);
};

PlainTextAuthenticator.prototype.evaluateChallenge = function (challenge, callback) {
  //noop
  callback();
};

module.exports = PlainTextAuthProvider;