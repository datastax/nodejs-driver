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

import { Authenticator, AuthProvider } from './provider';
import BaseDseAuthenticator from './base-dse-authenticator';
import utils from "../utils";

const mechanism = utils.allocBufferFromString('PLAIN');
const separatorBuffer = utils.allocBufferFromArray([0]);
const initialServerChallenge = 'PLAIN-START';

/**
 * @classdesc
 * AuthProvider that provides plain text authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @extends AuthProvider
 * @alias module:auth~DsePlainTextAuthProvider
 * @example
 * const client = new cassandra.Client({
 *   contactPoints: ['h1', 'h2'],
 *   authProvider: new cassandra.auth.DsePlainTextAuthProvider('user', 'p@ssword1');
 * });
 */
class DsePlainTextAuthProvider extends AuthProvider {
  username: string;
  password: string;
  authorizationId: string;
  /**
   * Creates a new instance of <code>DsePlainTextAuthProvider</code>.
   * @classdesc
   * AuthProvider that provides plain text authenticator instances for clients to connect
   * to DSE clusters secured with the DseAuthenticator.
   * @param {String} username The username; cannot be <code>null</code>.
   * @param {String} password The password; cannot be <code>null</code>.
   * @param {String} [authorizationId] The optional authorization ID. Providing an authorization ID allows the currently
   * authenticated user to act as a different user (a.k.a. proxy authentication).
   * @extends AuthProvider
   * @alias module:auth~DsePlainTextAuthProvider
   * @example
   * const client = new cassandra.Client({
   *   contactPoints: ['h1', 'h2'],
   *   authProvider: new cassandra.auth.DsePlainTextAuthProvider('user', 'p@ssword1');
   * });
   * @constructor
   */
  constructor(username: string, password: string, authorizationId?: string) {
    super();
    if (typeof username !== 'string' || typeof password !== 'string') {
      // Validate for null and undefined
      throw new TypeError('Username and password must be a string');
    }
    this.username = username;
    this.password = password;
    this.authorizationId = authorizationId;
  }

  /**
   * Returns an Authenticator instance to be used by the driver when connecting to a host.
   * @param {String} endpoint The IP address and port number in the format ip:port.
   * @param {String} name Authenticator name.
   * @override
   * @returns {Authenticator}
   */
  newAuthenticator(endpoint: string, name: string): Authenticator {
    return new PlainTextAuthenticator(name, this.username, this.password, this.authorizationId);
  }
}

/**
 * @extends BaseDseAuthenticator
 * @private
 */
class PlainTextAuthenticator extends BaseDseAuthenticator {
  authenticatorId: Buffer;
  password: Buffer;
  authorizationId: Buffer;

  /**
   * @param {String} authenticatorName
   * @param {String} authenticatorId
   * @param {String} password
   * @param {String} authorizationId
   * @constructor
   * @private
   */
  constructor(authenticatorName: string, authenticatorId: string, password: string, authorizationId: string) {
    super(authenticatorName);
    this.authenticatorId = utils.allocBufferFromString(authenticatorId);
    this.password = utils.allocBufferFromString(password);
    this.authorizationId = utils.allocBufferFromString(authorizationId || '');
  }

  /** @override */
  getMechanism() {
    return mechanism;
  }

  /** @override */
  getInitialServerChallenge() {
    return utils.allocBufferFromString(initialServerChallenge);
  }

  /** @override */
  evaluateChallenge(challenge, callback) {
    if (!challenge || challenge.toString() !== initialServerChallenge) {
      return callback(new Error('Incorrect SASL challenge from server'));
    }
    // The SASL plain text format is authorizationId 0 username 0 password
    callback(null, Buffer.concat([
      this.authorizationId,
      separatorBuffer,
      this.authenticatorId,
      separatorBuffer,
      this.password
    ]));
  }
}

export default DsePlainTextAuthProvider;