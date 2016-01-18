'use strict';
var assert = require('assert');
var cassandra = require('cassandra-driver');
var DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');

describe('DsePlainTextAuthProvider', function () {
  describe('#newAuthenticator()', function () {
    it('should return an Authenticator instance', function () {
      var authProvider = new DsePlainTextAuthProvider('u', 'p');
      var authenticator = authProvider.newAuthenticator('a:1', 'PassAuth');
      assert.ok(authenticator);
      assert.ok(authenticator instanceof cassandra.auth.Authenticator);
    });
  });
});