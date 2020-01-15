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

export namespace auth {
  interface Authenticator {
    initialResponse(callback: Function): void;

    evaluateChallenge(challenge: Buffer, callback: Function): void;

    onAuthenticationSuccess(token?: Buffer): void;
  }

  interface AuthProvider {
    newAuthenticator(endpoint: string, name: string): Authenticator;
  }

  class PlainTextAuthProvider implements AuthProvider {
    constructor(username: string, password: string);

    newAuthenticator(endpoint: string, name: string): Authenticator;
  }

  class DsePlainTextAuthProvider implements AuthProvider {
    constructor(username: string, password: string, authorizationId?: string);

    newAuthenticator(endpoint: string, name: string): Authenticator;
  }

  class DseGssapiAuthProvider implements AuthProvider {
    constructor(gssOptions?: { authorizationId?: string, service?: string, hostNameResolver?: Function });

    newAuthenticator(endpoint: string, name: string): Authenticator;
  }
}