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

import path from "path";
import Client from "../../../../lib/client";
import helper from "../../../test-helper";
import { format } from "util";
import { exec } from "child_process";

const ccmCmdString = 'docker exec $(docker ps -a -q --filter ancestor=single_endpoint) ccm %s';

const cloudHelper = module.exports = {

  getOptions: function (options1, options2) {
    const baseOptions = {
      protocolOptions: { maxVersion: 4 }
    };

    return Object.assign(baseOptions, options1, options2);
  },

  /**
   * Creates a new client using the default options.
   * @param {ClientOptions} [options]
   * @return {Client}
   */
  getClient: function (options) {
    const client = new Client(
      cloudHelper.getOptions({ cloud: { secureConnectBundle: 'certs/bundles/creds-v1.zip' } }, options));

    helper.shutdownAfterThisTest(client);

    return client;
  },

  setup: function(options) {
    options = options || {};

    const setupInfo = {
      setupSucceeded: false,
    };

    before(() => this.runContainer());

    before(() => {
      if (!options.queries) {
        return;
      }

      const queries = options.queries.join('; ');
      return this.execCcm(`node1 cqlsh -u cassandra -p cassandra -x "${queries}"`);
    });

    before(() => setupInfo.setupSucceeded = true);

    after(() => {
      if (!setupInfo.setupSucceeded) {
        // eslint-disable-next-line no-undef,no-console
        console.log(this.containerLogs);
      }
    });

    after(() => this.stopContainer());

    return setupInfo;
  },

  execCcm: function (cmd) {
    return this.execCommand(format(ccmCmdString, cmd));
  },

  runContainer: function () {
    helper.trace('Starting SNI container');
    let singleEndpointPath = process.env['SINGLE_ENDPOINT_PATH'];
    if (!singleEndpointPath) {
      singleEndpointPath = path.join(process.env['HOME'], 'proxy', 'run.sh');
      helper.trace("SINGLE_ENDPOINT_PATH not set, using " + singleEndpointPath);
    }

    return this.execCommand(singleEndpointPath, { 'REQUIRE_CLIENT_CERTIFICATE': 'true' });
  },

  stopContainer: function () {
    return this.execCommand('docker kill $(docker ps -a -q --filter ancestor=single_endpoint)');
  },

  execCommand: function (cmd, env) {
    return new Promise((resolve, reject) => exec(cmd, { env }, (err, stdout, stderr) => {
      if (stderr) {
        helper.trace(stderr);
      }

      if (err) {
        reject(err);
      } else {
        resolve(stdout);
      }
    }));
  },

  startAllNodes: function () {
    return this.execCcm('start --root');
  },

  stopNode: function (nodeId) {
    return this.execCcm(`node${nodeId} stop`);
  },

  startNode: function (nodeId) {
    return this.execCcm(`node${nodeId} start --root --wait-for-binary-proto`);
  }
};

