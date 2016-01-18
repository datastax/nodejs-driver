'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');

var dseAuthenticatorName = 'com.datastax.bdp.cassandra.auth.DseAuthenticator';

/**
 * Base class for Authenticator implementations that want to make use of
 * the authentication scheme negotiation in the DseAuthenticator
 * @param {String} authenticatorName
 * @extends Authenticator
 * @constructor
 */
function BaseDseAuthenticator(authenticatorName) {
  this.authenticatorName = authenticatorName;
}

util.inherits(BaseDseAuthenticator, cassandra.auth.Authenticator);

/**
 * Return a Buffer containing the required SASL mechanism.
 * @abstract
 * @returns {Buffer}
 */
BaseDseAuthenticator.prototype.getMechanism = function () {
  throw new Error('Not implemented');
};

/**
 * Return a byte array containing the expected successful server challenge.
 * @abstract
 * @returns {Buffer}
 */
BaseDseAuthenticator.prototype.getInitialServerChallenge = function () {
  throw new Error('Not implemented');
};

/**
 * @param {Function} callback
 * @override
 */
BaseDseAuthenticator.prototype.initialResponse = function (callback) {
  if (!this._isDseAuthenticator()) {
    //fallback
    return this.evaluateChallenge(this.getInitialServerChallenge(), callback);
  }
  //send the mechanism as a first auth message
  callback(null, this.getMechanism());
};

/**
 * Determines if the name of the authenticator matches DSE 5+
 * @protected
 * @ignore
 */
BaseDseAuthenticator.prototype._isDseAuthenticator = function () {
  return this.authenticatorName === dseAuthenticatorName;
};

module.exports = BaseDseAuthenticator;