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
const assert = require('assert');
const helper = require('../../../test-helper');
import DsePlainTextAuthProvider from '../../../../lib/auth/dse-plain-text-auth-provider';
import DseGssapiAuthProvider from '../../../../lib/auth/dse-gssapi-auth-provider';
const Client = require('../../../../lib/client');
const utils = require('../../../../lib/utils');
const errors = require('../../../../lib/errors');
const types = require('../../../../lib/types');
const vdescribe = helper.vdescribe;
const ads = helper.ads;
const cDescribe = helper.conditionalDescribe(helper.requireOptional('kerberos'), 'kerberos required to run');

vdescribe('dse-5.1', 'Proxy Authentication @SERVER_API', function () {
  this.timeout(180000);
  before(function (done) {
    utils.series([
      ads.start.bind(ads),
      function startCcm (next) {
        const ccmOptions = {
          yaml: [
            'authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator',
            'authorizer:com.datastax.bdp.cassandra.auth.DseAuthorizer',
          ],
          jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()],
          dseYaml: [
            '-y',
            'authorization_options:\n' +
            '  enabled: true\n' +
            'authentication_options:\n' +
            '  enabled: true\n' +
            '  default_scheme: kerberos\n' +
            '  other_schemes:\n' +
            '    - internal\n' +
            'kerberos_options:\n' +
            '  keytab: ' + ads.getKeytabPath('dse') + '\n' +
            '  service_principal: dse/_HOST@DATASTAX.COM\n' +
            '  http_principal: dse/_HOST@DATASTAX.COM\n' +
            '  qop: auth\n' +
            'audit_logging_options: \n' +
            '  enabled: true'
          ]
        };
        helper.ccm.startAll(1, ccmOptions, next);
      }
    ], done);
  });
  before(function (done) {
    const client = new Client(helper.getOptions({
      authProvider: new DsePlainTextAuthProvider('cassandra', 'cassandra')
    }));
    const queries = [
      "CREATE ROLE IF NOT EXISTS alice WITH PASSWORD = 'alice' AND LOGIN = FALSE",
      "CREATE ROLE IF NOT EXISTS ben WITH PASSWORD = 'ben' AND LOGIN = TRUE",
      "CREATE ROLE IF NOT EXISTS 'bob@DATASTAX.COM' WITH LOGIN = TRUE",
      "CREATE ROLE IF NOT EXISTS 'charlie@DATASTAX.COM' WITH PASSWORD = 'charlie' AND LOGIN = TRUE",
      "CREATE ROLE IF NOT EXISTS steve WITH PASSWORD = 'steve' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS aliceks " +
        "WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS aliceks.alicetable (key text PRIMARY KEY, value text)",
      "INSERT INTO aliceks.alicetable (key, value) VALUES ('hello', 'world')",
      "GRANT ALL ON KEYSPACE aliceks TO alice",
      "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'ben'",
      "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'bob@DATASTAX.COM'",
      "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'steve'",
      "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'charlie@DATASTAX.COM'",
      "GRANT PROXY.LOGIN ON ROLE 'alice' TO 'ben'",
      "GRANT PROXY.LOGIN ON ROLE 'alice' TO 'bob@DATASTAX.COM'",
      "GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'steve'",
      "GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'charlie@DATASTAX.COM'"
      // ben and bob are allowed to login as alice, but not execute as alice.
      // charlie and steve are allowed to execute as alice, but not login as alice.
    ];
    utils.eachSeries(queries, client.execute.bind(client), helper.finish(client, done));
  });
  after(helper.ccm.remove);
  after(ads.stop.bind(ads));
  describe('with DsePlainTextAuthProvider', function() {
    it('should allow plain text authorized user to login as', function (done) {
      connectAndQuery(new DsePlainTextAuthProvider('ben', 'ben', 'alice'), null, done);
    });
    it('should allow plain text authorized user to execute as', function (done) {
      connectAndQuery(new DsePlainTextAuthProvider('steve', 'steve'), 'alice', done);
    });
    it('should not allow plain text unauthorized user to login as', function (done) {
      connectAndQuery(new DsePlainTextAuthProvider('steve', 'steve', 'alice'), null, function (err) {
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        const innerErrors = utils.objectValues(err.innerErrors);
        assert.strictEqual(innerErrors.length, 1);
        helper.assertInstanceOf(innerErrors[0], errors.AuthenticationError);
        done();
      });
    });
    it('should not allow plain text unauthorized user to execute as', function (done) {
      connectAndQuery(new DsePlainTextAuthProvider('ben', 'ben'), 'alice', function (err) {
        helper.assertInstanceOf(err, errors.ResponseError);
        assert.strictEqual(err.code, types.responseErrorCodes.unauthorized);
        done();
      });
    });
  });
  cDescribe('with DseGssapiAuthProvider', function () {
    afterEach(function(done) {
      // clear out ticket cache between tests.
      ads.destroyTicket(null, done);
    });
    it('should allow kerberos authorized user to execute as', function (done) {
      utils.series([
        function (next) {
          ads.acquireTicket('charlie', 'charlie@DATASTAX.COM', next);
        },
        function (next) {
          connectAndQuery(new DseGssapiAuthProvider(), 'alice', next);
        }
      ], done);
    });
    it('should allow kerberos authorized user to login as', function (done) {
      utils.series([
        function (next) {
          ads.acquireTicket('bob', 'bob@DATASTAX.COM', next);
        },
        function (next) {
          connectAndQuery(new DseGssapiAuthProvider({ authorizationId: 'alice' }), null, next);
        }
      ], done);
    });
    it('should not allow kerberos unauthorized user to login as', function (done) {
      utils.series([
        function (next) {
          ads.acquireTicket('charlie', 'charlie@DATASTAX.COM', next);
        },
        function (next) {
          connectAndQuery(new DseGssapiAuthProvider({ authorizationId: 'alice' }), null, function (err) {
            helper.assertInstanceOf(err, errors.NoHostAvailableError);
            const innerErrors = utils.objectValues(err.innerErrors);
            assert.strictEqual(innerErrors.length, 1);
            helper.assertInstanceOf(innerErrors[0], errors.AuthenticationError);
            next();
          });
        }
      ], done);
    });
    it('should not allow kerberos unauthorized user to execute as', function (done) {
      utils.series([
        function (next) {
          ads.acquireTicket('bob', 'bob@DATASTAX.COM', next);
        },
        function (next) {
          connectAndQuery(new DseGssapiAuthProvider(), 'alice', function (err) {
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(err.code, types.responseErrorCodes.unauthorized);
            next();
          });
        }
      ], done);
    });
  });
});

function connectAndQuery(authProvider, executeAs, callback) {
  const client = new Client(helper.getOptions({
    authProvider: authProvider
  }));
  const options = { executeAs: executeAs };
  client.execute('SELECT * FROM aliceks.alicetable', null, options, function (err, result) {
    client.shutdown();
    callback(err, result && result.first());
  });
}