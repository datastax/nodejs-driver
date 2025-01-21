/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';
const util = require('util');
const { Authenticator } = require('./provider');

const dseAuthenticatorName = 'com.datastax.bdp.cassandra.auth.DseAuthenticator';

/**
 * Base class for Authenticator implementations that want to make use of
 * the authentication scheme negotiation in the DseAuthenticator
 * @param {String} authenticatorName
 * @extends Authenticator
 * @constructor
 * @ignore
 */
function BaseDseAuthenticator(authenticatorName) {
  this.authenticatorName = authenticatorName;
}

util.inherits(BaseDseAuthenticator, Authenticator);

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