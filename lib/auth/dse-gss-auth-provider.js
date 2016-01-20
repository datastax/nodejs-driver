'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var BaseDseAuthenticator = require('./base-dse-authenticator');
var GssClient = require('./gss-client');

var mechanism = new Buffer('GSSAPI');
var initialServerChallenge = 'GSSAPI-START';
var emptyBuffer = new Buffer(0);

/**
 * AuthProvider that provides GSSAPI authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @param {String} host The host.
 * @param {String} [user] The user principal. (defaults to whatever is in the ticket cache.)
 * @param {String} [service] The service to use. (defaults to 'dse')
 * @constructor
 */
function DseGssAuthProvider(host, user, service) {
  //load the kerberos at construction time
  try {
    this._kerberos = require('kerberos');
  }
  catch (err) {
    if (err.code === 'MODULE_NOT_FOUND') {
      var newErr = new Error('You must install module "kerberos" to use GSSAPI auth provider: ' +
        'https://www.npmjs.com/package/kerberos');
      newErr.code = err.code;
      throw newErr;
    }
    throw err;
  }
  this.host = host;
  this.user = user;
  this.service = service;
}

util.inherits(DseGssAuthProvider, cassandra.auth.AuthProvider);

/** @override */
DseGssAuthProvider.prototype.newAuthenticator = function (endpoint, name) {
  return new GssAuthenticator(this._kerberos, this.host, name, this.user, this.service);
};

/**
 * @param {Object} kerberosModule
 * @param {String} host
 * @param {String} authenticatorName
 * @param {String} [user]
 * @param {String} [service]
 * @extends Authenticator
 * @private
 */
function GssAuthenticator(kerberosModule, host, authenticatorName, user, service) {
  this.user = user;
  this.authenticatorName = authenticatorName;
  this.client = GssClient.createNew(kerberosModule, host, user, service);
}

util.inherits(GssAuthenticator, BaseDseAuthenticator);

GssAuthenticator.prototype.getMechanism = function () {
  return mechanism;
};

GssAuthenticator.prototype.getInitialServerChallenge = function () {
  return new Buffer(initialServerChallenge);
};

//noinspection JSUnusedGlobalSymbols
/**
 * Obtain an initial response token for initializing the SASL handshake.
 * @param {Function} callback
 */
GssAuthenticator.prototype.initialResponse = function (callback) {
  var self = this;
  //initialize the GSS client
  this.client.init(function (err) {
    if (err) {
      return callback(err);
    }
    if (!self._isDseAuthenticator()) {
      //fallback
      return self.evaluateChallenge(self.getInitialServerChallenge(), callback);
    }
    //send the mechanism as a first auth message
    callback(null, self.getMechanism());
  });
};

/**
 * Evaluates a challenge received from the Server. Generally, this method should callback with
 * no error and no additional params when authentication is complete from the client perspective.
 * @param {Buffer} challenge
 * @param {Function} callback
 * @override
 */
GssAuthenticator.prototype.evaluateChallenge = function (challenge, callback) {
  if (!challenge || challenge.toString() === initialServerChallenge) {
    challenge = emptyBuffer;
  }
  this.client.evaluateChallenge(challenge, callback);
};

//noinspection JSUnusedLocalSymbols,JSUnusedGlobalSymbols
/**
 * @override
 */
GssAuthenticator.prototype.onAuthenticationSuccess = function (token) {
  this.client.shutdown(function noop() { });
};


module.exports = DseGssAuthProvider;