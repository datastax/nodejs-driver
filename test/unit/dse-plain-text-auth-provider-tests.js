/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const Authenticator = require('../../lib/auth/provider').Authenticator;
const DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');

describe('DsePlainTextAuthProvider', function () {
  describe('#newAuthenticator()', function () {
    it('should return an Authenticator instance', function () {
      const authProvider = new DsePlainTextAuthProvider('u', 'p');
      const authenticator = authProvider.newAuthenticator('a:1', 'PassAuth');
      assert.ok(authenticator);
      assert.ok(authenticator instanceof Authenticator);
    });
  });
});