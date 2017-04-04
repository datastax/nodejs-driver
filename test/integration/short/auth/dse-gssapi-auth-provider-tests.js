/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var helper = require('../../../test-helper');
var DseGssapiAuthProvider = require('../../../../lib/auth/dse-gssapi-auth-provider');
var Client = require('../../../../lib/dse-client');
var ads = helper.ads;
var cDescribe = helper.conditionalDescribe(helper.requireOptional('kerberos'), 'kerberos required to run');

cDescribe('DseGssapiAuthProvider', function () {
  this.timeout(60000);
  before(function (done) {
    ads.start(function(err) {
      assert.ifError(err);
      ads.acquireTicket('cassandra', 'cassandra@DATASTAX.COM', done);
    });
  });
  after(ads.stop.bind(ads));
  it('should authenticate against DSE instance using Kerberos', function (done) {
    var v5 = helper.versionCompare(helper.getDseVersion(), '5.0');
    // Set authenticator based on DSE version.
    var authenticator = v5 ?
      'authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator' :
      'authenticator:com.datastax.bdp.cassandra.auth.KerberosAuthenticator';

    var yamlOptions = [
      'kerberos_options.keytab:' + ads.getKeytabPath('dse'),
      'kerberos_options.service_principal:dse/_HOST@DATASTAX.COM',
      'kerberos_options.http_principal:dse/_HOST@DATASTAX.COM',
      'kerberos_options.qop:auth'
    ];

    // add DSE 5.0 specific options required to enable kerberos.
    if(v5) {
      yamlOptions.concat(
        'authentication_options.enabled:true',
        'authentication_options.default_scheme:kerberos'
      );
    }

    var testClusterOptions = {
      yaml: authenticator,
      dseYaml: yamlOptions,
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0', '-Djava.security.krb5.conf=' + ads.getKrb5ConfigPath()]
    };

    helper.ccm.startAll(1, testClusterOptions, function (err) {
      assert.ifError(err);
      var authProvider = new DseGssapiAuthProvider();
      var clientOptions = helper.getOptions({ authProvider: authProvider });
      helper.connectAndQuery(new Client(clientOptions), done);
    });
  });
});