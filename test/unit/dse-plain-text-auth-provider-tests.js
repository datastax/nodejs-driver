/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var Authenticator = require('../../lib/auth/provider').Authenticator;
var DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');

describe('DsePlainTextAuthProvider', function () {
  describe('#newAuthenticator()', function () {
    it('should return an Authenticator instance', function () {
      var authProvider = new DsePlainTextAuthProvider('u', 'p');
      var authenticator = authProvider.newAuthenticator('a:1', 'PassAuth');
      assert.ok(authenticator);
      assert.ok(authenticator instanceof Authenticator);
    });
  });
});