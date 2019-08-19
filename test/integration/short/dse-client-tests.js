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
const version = require('../../../index').version;
const helper = require('../../test-helper');
const Client = require('../../../lib/dse-client');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');

describe('Client', function() {
  this.timeout(60000);
  before(function(done) {
    helper.ccm.startAll(1, {}, done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  it('should log the module versions on first connect only', function(done) {
    const client = new Client(helper.getOptions());
    const versionLogRE = /^Using DSE driver v(.+)$/;
    let versionMessage = undefined;

    client.on('log', function(level, className, message) {
      const match = message.match(versionLogRE);
      if(match) {
        versionMessage = { level: level, match: match };
      }
    });

    utils.series([
      client.connect.bind(client),
      function ensureLogged(next) {
        assert.ok(versionMessage);
        assert.strictEqual(versionMessage.level, 'info');
        // versions should match those from the modules.
        assert.strictEqual(versionMessage.match[1], version);
        versionMessage = undefined;
        next();
      },
      client.connect.bind(client),
      function ensureNotLogged(next) {
        assert.strictEqual(versionMessage, undefined);
        next();
      },
      client.shutdown.bind(client)
    ],done);
  });

  it('should support providing client id, application name and version', () => {
    const client = new Client(helper.getOptions({
      applicationName: 'My App',
      applicationVersion: 'v3.2.1',
      clientId: types.Uuid.random()
    }));

    return client.connect()
      .then(() => client.shutdown());
  });
});
