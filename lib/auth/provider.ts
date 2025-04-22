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
/* eslint-disable @typescript-eslint/no-unused-vars */

/**
 * Provides [Authenticator]{@link module:auth~Authenticator} instances to be used when connecting to a host.
 * @abstract
 * @alias module:auth~AuthProvider
 */
class AuthProvider {
  /**
   * Returns an [Authenticator]{@link module:auth~Authenticator} instance to be used when connecting to a host.
   * @param {String} endpoint The ip address and port number in the format ip:port
   * @param {String} name Authenticator name
   * @abstract
   * @returns {Authenticator}
   */
  newAuthenticator(endpoint: string, name: string): Authenticator {
    throw new Error('This is an abstract class, you must implement newAuthenticator method or ' +
      'use another auth provider that inherits from this class');
  }
}

/**
 * Handles SASL authentication with Cassandra servers.
 * Each time a new connection is created and the server requires authentication,
 * a new instance of this class will be created by the corresponding.
 * @alias module:auth~Authenticator
 */
class Authenticator {
  /**
   * Obtain an initial response token for initializing the SASL handshake.
   * @param {Function} callback
   */
  initialResponse(callback: Function) {
    callback(new Error('Not implemented'));
  }

  /**
   * Evaluates a challenge received from the Server. Generally, this method should callback with
   * no error and no additional params when authentication is complete from the client perspective.
   * @param {Buffer} challenge
   * @param {Function} callback
   */
  evaluateChallenge(challenge: Buffer, callback: Function) {
    callback(new Error('Not implemented'));
  }

  /**
   * Called when authentication is successful with the last information
   * optionally sent by the server.
   * @param {Buffer} [token]
   */
  onAuthenticationSuccess(token?: Buffer) {
    // No-op
  }
}

export {
  Authenticator,
  AuthProvider
};