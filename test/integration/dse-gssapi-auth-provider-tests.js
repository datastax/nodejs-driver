var assert = require('assert');
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DseGssAuthProvider = require('../../lib/auth/dse-gssapi-auth-provider');
var vit = helper.vit;
var ads = helper.ads;

describe('DseGssapiAuthProvider', function () {
  this.timeout(60000);
  before(function (done) {
    ads.start(function(err) {
      assert.ifError(err);
      ads.acquireTicket('cassandra', 'cassandra@DATASTAX.COM', done);
    });
  });
  after(ads.stop.bind(ads));
  vit('<5.0', 'should authenticate against DSE v4.x instance', function (done) {
    var testClusterOptions = {
      yaml: ['authenticator: com.datastax.bdp.cassandra.auth.KerberosAuthenticator'],
      dseYaml: ads.getDseKerberosOptions(),
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()]
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DseGssAuthProvider();
      var clientOptions = helper.getOptions({ authProvider: authProvider });
      helper.connectAndQuery(new cassandra.Client(clientOptions), done);
    });
  });
  vit('5.0', 'should authenticate against DSE v5.x instance', function (done) {
    var testClusterOptions = {
      yaml: ['authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator'],
      dseYaml: ads.getDseKerberosOptions(),
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()]
    };
    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DseGssAuthProvider();
      var clientOptions = helper.getOptions({ authProvider: authProvider });
      helper.connectAndQuery(new cassandra.Client(clientOptions), done);
    });
  });
});