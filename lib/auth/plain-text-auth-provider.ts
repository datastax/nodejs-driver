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

import { AuthProvider, Authenticator } from './provider';
import utils from "../utils";

/**
 * @classdesc Provides plain text [Authenticator]{@link module:auth~Authenticator} instances to be used when
 * connecting to a host.
 * @extends module:auth~AuthProvider
 * @example
 * var authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
 * //Set the auth provider in the clientOptions when creating the Client instance
 * const client = new Client({ contactPoints: contactPoints, authProvider: authProvider });
 * @alias module:auth~PlainTextAuthProvider
 */
class PlainTextAuthProvider extends AuthProvider {
  private username: string;
  private password: string;

  /**
   * Creates a new instance of the Authenticator provider
   * @classdesc Provides plain text [Authenticator]{@link module:auth~Authenticator} instances to be used when
   * connecting to a host.
   * @example
   * var authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
   * //Set the auth provider in the clientOptions when creating the Client instance
   * const client = new Client({ contactPoints: contactPoints, authProvider: authProvider });
   * @param {String} username User name in plain text
   * @param {String} password Password in plain text
   * @alias module:auth~PlainTextAuthProvider
   * @constructor
   */
  constructor(username: string, password: string) {
    super();
    this.username = username;
    this.password = password;
  }

  /**
   * Returns a new [Authenticator]{@link module:auth~Authenticator} instance to be used for plain text authentication.
   * @override
   * @returns {Authenticator}
   */
  newAuthenticator(): Authenticator {
    return new PlainTextAuthenticator(this.username, this.password);
  }
}

/**
 * @ignore @internal
 */
class PlainTextAuthenticator extends Authenticator {
  username: any;
  password: any;
  constructor(username, password) {
    super();
    this.username = username;
    this.password = password;
  }

  initialResponse(callback) {
    const initialToken = Buffer.concat([
      utils.allocBufferFromArray([0]),
      utils.allocBufferFromString(this.username, 'utf8'),
      utils.allocBufferFromArray([0]),
      utils.allocBufferFromString(this.password, 'utf8')
    ]);
    callback(null, initialToken);
  }

  evaluateChallenge(challenge, callback) {
    // noop
    callback();
  }
}

export {
  PlainTextAuthenticator,
  PlainTextAuthProvider,
};