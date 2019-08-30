/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const format = require('util').format;
const path = require('path');
const exec = require('child_process').exec;

const Client = require('../../../../lib/client');
const helper = require('../../../test-helper');

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

    after(() => client.shutdown());

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

