'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var BaseDseAuthenticator = require('./base-dse-authenticator');

var mechanism = new Buffer('PLAIN');
var initialServerChallenge = 'PLAIN-START';

/**
 * Creates a new instance of <code>DsePlainTextAuthProvider</code>.
 * @classdesc
 * AuthProvider that provides plain text authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @param {String} username
 * @param {String} password
 * @extends AuthProvider
 * @alias module:auth~DsePlainTextAuthProvider
 * @example
 * const client = new dse.DseClient({
 *  contactPoints: ['h1', 'h2'],
 *  authProvider: new dse.auth.DsePlainTextAuthProvider('user', 'p@ssword1');
 * });
 * @constructor
 */
function DsePlainTextAuthProvider(username, password) {
  this.username = username;
  this.password = password;
}

util.inherits(DsePlainTextAuthProvider, cassandra.auth.AuthProvider);

/**
 * Returns an Authenticator instance to be used by the driver when connecting to a host.
 * @param {String} endpoint The IP address and port number in the format ip:port.
 * @param {String} name Authenticator name.
 * @override
 * @returns {Authenticator}
 */
DsePlainTextAuthProvider.prototype.newAuthenticator = function (endpoint, name) {
  return new PlainTextAuthenticator(name, this.username, this.password);
};

/**
 * @param {String} authenticatorName
 * @param {String} username
 * @param {String} password
 * @extends BaseDseAuthenticator
 * @constructor
 * @private
 */
function PlainTextAuthenticator(authenticatorName, username, password) {
  BaseDseAuthenticator.call(this, authenticatorName);
  this.username = new Buffer(username);
  this.password = new Buffer(password);
}

//noinspection JSCheckFunctionSignatures
util.inherits(PlainTextAuthenticator, BaseDseAuthenticator);

/** @override */
PlainTextAuthenticator.prototype.getMechanism = function () {
  return mechanism;
};

/** @override */
PlainTextAuthenticator.prototype.getInitialServerChallenge = function () {
  return new Buffer(initialServerChallenge);
};

/** @override */
PlainTextAuthenticator.prototype.evaluateChallenge = function (challenge, callback) {
  if (!challenge || challenge.toString() !== initialServerChallenge) {
    return callback(new Error('Incorrect SASL challenge from server'));
  }
  callback(null, Buffer.concat([
    new Buffer([0]),
    this.username,
    new Buffer([0]),
    this.password
  ]));
};

module.exports = DsePlainTextAuthProvider;