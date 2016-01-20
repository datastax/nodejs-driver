var assert = require('assert');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DseGssAuthProvider = require('../../lib/auth/dse-gss-auth-provider');
var vit = helper.vit;

describe('DseGssAuthProvider', function () {
  this.timeout(60000);
  vit('<5.0', 'should authenticate against DSE v4.x instance', function (done) {
    var testClusterOptions = {
      yaml: ['authenticator: com.datastax.bdp.cassandra.auth.KerberosAuthenticator'],
      dseYaml: helper.getDseKerberosOptions(),
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=/Users/jorge/workspace/tests/ads-jar/d/krb5.conf']
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DseGssAuthProvider('127.0.0.1');
      var clientOptions = helper.getOptions({authProvider: authProvider});
      var client = new cassandra.Client(clientOptions);
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });
});