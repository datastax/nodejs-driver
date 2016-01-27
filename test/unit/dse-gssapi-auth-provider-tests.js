'use strict';
var assert = require('assert');
var DseGssapiAuthProvider = require('../../lib/auth/dse-gssapi-auth-provider');

var dseAuthenticatorName = 'com.datastax.bdp.cassandra.auth.DseAuthenticator';

describe('DseGssapiAuthProvider', function () {
  describe('constructor', function () {
    it('should load optional kerberos module', function () {
      var authProvider = new DseGssapiAuthProvider();
      assert.ok(authProvider._kerberos);
    });
  });
  describe('#newAuthenticator()', function () {
  });
});
describe('GssapiAuthenticator', function () {
  describe('#initialResponse()', function () {
    it('should send mechanism and call client.init()', function (done) {
      var authProvider = new DseGssapiAuthProvider();
      var authenticator = authProvider.newAuthenticator('127.0.0.1:1001', dseAuthenticatorName);
      var initCalled = 0;
      authenticator.client = {
        init: function (h, cb) {
          initCalled++;
          cb();
        }
      };
      authenticator.initialResponse(function (err, response) {
        assert.ifError(err);
        assert.ok(response);
        assert.strictEqual(response.toString(), 'GSSAPI');
        assert.strictEqual(initCalled, 1);
        done();
      });
    });
    it('should call evaluateChallenge() when DSE lower than v5', function (done) {
      var authProvider = new DseGssapiAuthProvider();
      var authenticator = authProvider.newAuthenticator('127.0.0.1:1001', 'DSE4');
      var evaluateChallengeCalled = 0;
      authenticator.client = {
        init: function (h, cb) {
          cb();
        }
      };
      authenticator.evaluateChallenge = function (challenge, cb) {
        evaluateChallengeCalled++;
        cb(null, 'EVALUATED');
      };
      authenticator.initialResponse(function (err, response) {
        assert.ifError(err);
        assert.strictEqual(response, 'EVALUATED');
        assert.strictEqual(evaluateChallengeCalled, 1);
        done();
      });
    });
  });
  describe('#evaluateChallenge()', function () {
    it('should call client.evaluateChallenge()', function (done) {
      var authProvider = new DseGssapiAuthProvider();
      var authenticator = authProvider.newAuthenticator('127.0.0.1:1001', dseAuthenticatorName);
      var evaluateChallengeCalled = 0;
      authenticator.client = {
        evaluateChallenge: function (c, cb) {
          evaluateChallengeCalled++;
          cb();
        }
      };
      authenticator.evaluateChallenge(new Buffer(1), function (err) {
        assert.ifError(err);
        assert.strictEqual(evaluateChallengeCalled, 1);
        done();
      });
    });
  });
});