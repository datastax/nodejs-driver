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
const { AuthProvider, Authenticator } = require('./provider');
const utils = require('../utils');

/**
 * @class
 * @classdesc Provides plain text [Authenticator]{@link module:auth~Authenticator} instances to be used when
 * connecting to a host.
 * @extends AuthProvider
 * @alias module:auth~PlainTextAuthProvider
 * @example
 * const authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
 * // Set the auth provider in the clientOptions when creating the Client instance
 * const client = new Client({ contactPoints: contactPoints, authProvider: authProvider });
 */
class PlainTextAuthProvider extends AuthProvider {
  /**
   * @param {String} username User name in plain text
   * @param {String} password Password in plain text
   */
  constructor(username, password) {
    super();
    this.username = username;
    this.password = password;
  }

  /**
   * Returns a new [Authenticator]{@link module:auth~Authenticator} instance to be used for plain text authentication.
   * @override
   * @returns {Authenticator}
   */
  newAuthenticator() {
    return new PlainTextAuthenticator(this.username, this.password);
  }
}

/**
 * @class
 * @classdesc Authenticator for plain text authentication.
 * @extends Authenticator
 * @ignore
 */
class PlainTextAuthenticator extends Authenticator {
  /**
   * @param {String} username
   * @param {String} password
   */
  constructor(username, password) {
    super();
    this.username = username;
    this.password = password;
  }

  /**
   * Obtain an initial response token for initializing the SASL handshake.
   * @param {Function} callback
   */
  initialResponse(callback) {
    const initialToken = Buffer.concat([
      utils.allocBufferFromArray([0]),
      utils.allocBufferFromString(this.username, 'utf8'),
      utils.allocBufferFromArray([0]),
      utils.allocBufferFromString(this.password, 'utf8')
    ]);
    callback(null, initialToken);
  }

  /**
   * Evaluates a challenge received from the Server.
   * @param {Buffer} challenge
   * @param {Function} callback
   */
  evaluateChallenge(challenge, callback) {
    // noop
    callback();
  }
}

module.exports = {
  PlainTextAuthenticator,
  PlainTextAuthProvider,
};
