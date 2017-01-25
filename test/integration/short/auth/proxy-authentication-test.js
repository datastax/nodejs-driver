/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var helper = require('../../../test-helper');
var DsePlainTextAuthProvider = require('../../../../lib/auth/dse-plain-text-auth-provider');
var DseGssapiAuthProvider = require('../../../../lib/auth/dse-gssapi-auth-provider');
var Client = require('../../../../lib/dse-client');
var utils = require('../../../../lib/utils');
var errors = require('../../../../lib/errors');
var types = require('../../../../lib/types');
var vdescribe = helper.vdescribe;
var ads = helper.ads;

vdescribe('dse-5.1', 'Proxy Authentication', function () {
  this.timeout(60000);
  before(function (done) {
    utils.series([
      ads.start.bind(ads),
      function startCcm (next) {
        var ccmOptions = {
          yaml: [
            'authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator',
            'authorizer:com.datastax.bdp.cassandra.auth.DseAuthorizer',
          ],
          jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()],
          dseYaml: [
            'authorization_options.enabled:true',
            'authentication_options.enabled:true',
            'kerberos_options.keytab:' + ads.getKeytabPath('dse'),
            'kerberos_options.service_principal:dse/_HOST@DATASTAX.COM',
            'kerberos_options.http_principal:dse/_HOST@DATASTAX.COM',
            'kerberos_options.qop:auth',
            'audit_logging_options.enabled:true'
          ]
        };
        helper.ccm.startAll(1, ccmOptions, next);
      }
    ], done);
  });
  before(function (done) {
    var client = new Client(helper.getOptions({
      authProvider: new DsePlainTextAuthProvider('cassandra', 'cassandra')
    }));
    var queries = [
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
        var innerErrors = utils.objectValues(err.innerErrors);
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
  describe('with DseGssapiAuthProvider', function () {
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
            var innerErrors = utils.objectValues(err.innerErrors);
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
  var client = new Client(helper.getOptions({
    authProvider: authProvider
  }));
  var options = { executeAs: executeAs };
  client.execute('SELECT * FROM aliceks.alicetable', null, options, function (err, result) {
    client.shutdown();
    callback(err, result && result.first());
  });
}