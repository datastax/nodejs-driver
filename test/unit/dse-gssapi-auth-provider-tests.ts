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
import DseGssapiAuthProvider from '../../lib/auth/dse-gssapi-auth-provider';
import assert from "assert";
import helper from "../test-helper";
import utils from "../../lib/utils";
const cDescribe = helper.conditionalDescribe(helper.requireOptional('kerberos'), 'kerberos required to run');

const dseAuthenticatorName = 'com.datastax.bdp.cassandra.auth.DseAuthenticator';

cDescribe('DseGssapiAuthProvider', function () {
  describe('constructor', function () {
    it('should load optional kerberos module', function () {
      const authProvider = new DseGssapiAuthProvider();
      assert.ok(authProvider._kerberos);
    });
  });
});
cDescribe('GssapiAuthenticator', function () {
  describe('#initialResponse()', function () {
    it('should send mechanism and call client.init()', function (done) {
      const authProvider = new DseGssapiAuthProvider();
      const authenticator = authProvider.newAuthenticator('127.0.0.1:1001', dseAuthenticatorName);
      let initCalled = 0;
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
      const authProvider = new DseGssapiAuthProvider();
      const authenticator = authProvider.newAuthenticator('127.0.0.1:1001', 'DSE4');
      let evaluateChallengeCalled = 0;
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
      const authProvider = new DseGssapiAuthProvider();
      const authenticator = authProvider.newAuthenticator('127.0.0.1:1001', dseAuthenticatorName);
      let evaluateChallengeCalled = 0;
      authenticator.client = {
        evaluateChallenge: function (c, cb) {
          evaluateChallengeCalled++;
          cb();
        }
      };
      authenticator.evaluateChallenge(utils.allocBuffer(1), function (err) {
        assert.ifError(err);
        assert.strictEqual(evaluateChallengeCalled, 1);
        done();
      });
    });
  });
});