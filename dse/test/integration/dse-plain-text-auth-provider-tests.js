/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DsePlainTextAuthProvider = require('../../lib/auth/dse-plain-text-auth-provider');
var vit = helper.vit;

describe('DsePlainTextAuthProvider', function () {
  this.timeout(60000);
  it('should authenticate against DSE daemon instance', function (done) {
    var testClusterOptions = {
      yaml: ['authenticator: PasswordAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0']
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      var clientOptions = helper.getOptions({ authProvider: authProvider });
      var client = new cassandra.Client(clientOptions);
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });
  vit('5.0', 'should authenticate against DSE 5+ DseAuthenticator', function (done) {
    var testClusterOptions = {
      yaml: ['authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0'],
      dseYaml: ['authentication_options.default_scheme: internal']
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      var clientOptions = helper.getOptions({ authProvider: authProvider });
      var client = new cassandra.Client(clientOptions);
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });
});