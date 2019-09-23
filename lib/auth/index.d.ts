/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
}