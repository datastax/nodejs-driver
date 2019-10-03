/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require('assert');
const helper = require('../../../test-helper');
const DseGssapiAuthProvider = require('../../../../lib/auth/dse-gssapi-auth-provider');
const Client = require('../../../../lib/dse-client');
const errors = require('../../../../lib/errors');
const ads = helper.ads;
const cDescribe = helper.conditionalDescribe(helper.requireOptional('kerberos'), 'kerberos required to run');
const vit = helper.vit;

cDescribe('DseGssapiAuthProvider', function () {
  this.timeout(180000);
  before(function (done) {
    ads.start(function(err) {
      assert.ifError(err);
      ads.acquireTicket('cassandra', 'cassandra@DATASTAX.COM', done);
    });
  });

  before(done => {

    const v5 = helper.versionCompare(helper.getDseVersion(), '5.0');
    // Set authenticator based on DSE version.
    const authenticator = v5 ?
      'authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator' :
      'authenticator:com.datastax.bdp.cassandra.auth.KerberosAuthenticator';

    const yamlOptions = [
      'kerberos_options.keytab:' + ads.getKeytabPath('dse'),
      'kerberos_options.service_principal:dse/_HOST@DATASTAX.COM',
      'kerberos_options.http_principal:dse/_HOST@DATASTAX.COM',
      'kerberos_options.qop:auth'
    ];

    // add DSE 5.0 specific options required to enable kerberos.
    if(v5) {
      yamlOptions.push(
        'authentication_options.enabled:true',
        'authentication_options.default_scheme:kerberos'
      );
    }

    const testClusterOptions = {
      yaml: authenticator,
      dseYaml: yamlOptions,
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()]
    };

    helper.ccm.startAll(1, testClusterOptions, done);
  });

  after(ads.stop.bind(ads));

  vit('dse-5.1', 'should authenticate against DSE instance using Kerberos', done => {
    const authProvider = new DseGssapiAuthProvider();
    const clientOptions = helper.getOptions({ authProvider: authProvider });
    helper.connectAndQuery(new Client(clientOptions), done);
  });

  vit('dse-5.1', 'should fail with auth errors when authProvider is undefined', () => {
    const clientOptions = helper.getOptions({ authProvider: undefined });
    const client = new Client(clientOptions);
    let catchCalled = false;

    return client.connect()
      .catch(err => {
        catchCalled = true;
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        const addresses = Object.keys(err.innerErrors);
        assert.strictEqual(addresses.length, 1);
        helper.assertInstanceOf(err.innerErrors[addresses[0]], errors.AuthenticationError);
      })
      .then(() => assert.strictEqual(catchCalled, true));
  });
});