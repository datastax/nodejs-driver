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
import DsePlainTextAuthProvider from '../../../../lib/auth/dse-plain-text-auth-provider';
import { assert } from "chai";
import helper from "../../../test-helper";
import Client from "../../../../lib/client";
const vdescribe = helper.vdescribe;

vdescribe('dse-5.0', 'DsePlainTextAuthProvider @SERVER_API', function () {
  this.timeout(180000);

  context('with Cassandra PasswordAuthenticator', () => {
    helper.setup(1, { initClient: false, ccmOptions: {
      yaml: ['authenticator:PasswordAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0']
    }});

    it('should authenticate against DSE daemon instance', function (done) {
      const authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      const clientOptions = helper.getOptions({ authProvider: authProvider });
      const client = helper.shutdownAfterThisTest(new Client(clientOptions));
      client.connect(function (err) {
        assert.ifError(err);
        assert.notEqual(client.hosts.length, 0);
        client.shutdown(done);
      });
    });
  });

  context('with DSE 5+ DseAuthenticator', () => {
    helper.setup(1, { initClient: false, ccmOptions: {
      yaml: ['authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0'],
      dseYaml: ['authentication_options.enabled:true', 'authentication_options.default_scheme:internal']
    }});

    it('should authenticate against DSE 5+ DseAuthenticator', async () => {
      const authProvider = new DsePlainTextAuthProvider('cassandra', 'cassandra');
      const clientOptions = helper.getOptions({ authProvider });
      const client = helper.shutdownAfterThisTest(new Client(clientOptions));
      await client.connect();
      // There is an open connection
      assert.strictEqual(client.getState().getOpenConnections(client.hosts.values()[0]), 1);
      await client.shutdown();
    });
  });

  context('with transitional mode normal', () => {
    helper.setup(1, { initClient: false, ccmOptions: {
      yaml: ['authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator'],
      jvmArgs: ['-Dcassandra.superuser_setup_delay_ms=0'],
      dseYaml: ['authentication_options.enabled:true', 'authentication_options.default_scheme:internal', 'authentication_options.transitional_mode:normal']
    }});

    it('should support transitional mode', async () => {
      // Without setting an authenticator
      const clientOptions = helper.getOptions({});
      const client = helper.shutdownAfterThisTest(new Client(clientOptions));
      await client.connect();
      await client.execute('SELECT * FROM system.local');
      await client.shutdown();
    });
  });
});
