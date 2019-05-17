/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const helper = require('../../../test-helper');
const DsePlainTextAuthProvider = require('../../../../lib/auth/dse-plain-text-auth-provider');
const Client = require('../../../../lib/dse-client');
const vit = helper.vit;

describe('DsePlainTextAuthProvider', function () {
  this.timeout(180000);
  it('should authenticate against DSE daemon instance', function (done) {
    const testClusterOptions = {
      yaml: ['authenticator:PasswordAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0']
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      const authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      const clientOptions = helper.getOptions({ authProvider: authProvider });
      const client = new Client(clientOptions);
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });
  vit('dse-5.0', 'should authenticate against DSE 5+ DseAuthenticator', function (done) {
    const testClusterOptions = {
      yaml: ['authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0'],
      dseYaml: ['authentication_options.enabled:true', 'authentication_options.default_scheme:internal']
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      const authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      const clientOptions = helper.getOptions({ authProvider: authProvider });
      const client = new Client(clientOptions);
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });
});