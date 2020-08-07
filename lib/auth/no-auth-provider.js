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
const { PlainTextAuthenticator } = require('./plain-text-auth-provider');
const errors = require('../errors');

const dseAuthenticator = 'com.datastax.bdp.cassandra.auth.DseAuthenticator';

/**
 * Internal authentication provider that is used when no provider has been set by the user.
 * @ignore
 */
class NoAuthProvider extends AuthProvider {
  newAuthenticator(endpoint, name) {
    if (name === dseAuthenticator) {
      // Try to use transitional mode
      return new TransitionalModePlainTextAuthenticator();
    }

    // Use an authenticator that doesn't allow auth flow
    return new NoAuthAuthenticator(endpoint);
  }
}

/**
 * An authenticator throws an error when authentication flow is started.
 * @ignore
 */
class NoAuthAuthenticator extends Authenticator {
  constructor(endpoint) {
    super();
    this.endpoint = endpoint;
  }

  initialResponse(callback) {
    callback(new errors.AuthenticationError(
      `Host ${this.endpoint} requires authentication, but no authenticator found in the options`));
  }
}

/**
 * Authenticator that accounts for DSE authentication configured with transitional mode: normal.
 *
 * In this situation, the client is allowed to connect without authentication, but DSE
 * would still send an AUTHENTICATE response. This Authenticator handles this situation
 * by sending back a dummy credential.
 */
class TransitionalModePlainTextAuthenticator extends PlainTextAuthenticator {
  constructor() {
    super('', '');
  }
}

module.exports = NoAuthProvider;
