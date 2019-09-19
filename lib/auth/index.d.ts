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

  class DsePlainTextAuthProvider implements AuthProvider {
    constructor(username: string, password: string, authorizationId?: string);

    newAuthenticator(endpoint: string, name: string): Authenticator;
  }

  class DseGssapiAuthProvider implements AuthProvider {
    constructor(gssOptions?: { authorizationId?: string, service?: string, hostNameResolver?: Function });

    newAuthenticator(endpoint: string, name: string): Authenticator;
  }
}