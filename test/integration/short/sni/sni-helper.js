/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const https = require('https');
const format = require('util').format;
const path = require('path');
const exec = require('child_process').exec;

const Client = require('../../../../lib/dse-client');
const helper = require('../../../test-helper');
const utils = require('../../../../lib/utils');

const ccmCmdString = 'docker exec $(docker ps -a -q --filter ancestor=single_endpoint) ccm %s';
const metadataSvcUrl = [ '127.0.0.1', ':', '30443', '/metadata' ];

module.exports = {
  setup: function() {
    const setupInfo = {
      setupSucceeded: false,
      getClient: function (options) {
        return Client
          .forClusterConfig(metadataSvcUrl.join(''), Object.assign({ protocolOptions: { maxVersion: 4 } }, options))
          .then(client => {
            after(() => client.shutdown());

            return client.connect().then(() => client);
          });
      }
    };

    before(() => this.runContainer());

    before(done => {

      const requestOptions = {
        hostname: metadataSvcUrl[0],
        port: metadataSvcUrl[2],
        path: metadataSvcUrl[3],
        rejectUnauthorized: false
      };

      let connected = false;
      const start = process.hrtime();
      const maxWaitSeconds = 60;

      utils.whilst(
        () => !connected && process.hrtime(start)[0] < maxWaitSeconds,
        next => {
          let errCalled = false;
          https.get(requestOptions, (res) => {
            res
              .on('data', chunk => {})
              .on('end', () => {
                if (errCalled) {
                  return;
                }

                connected = true;
                next();
              });
          }).on("error", (err) => {
            errCalled = true;
            helper.trace(err.toString());
            setTimeout(next, 200);
          });
        },
        () => {
          if (connected) {
            done();
          } else {
            done(new Error(`Could not connect to metadata svc after ${maxWaitSeconds} seconds`));
          }
        }
      );

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
    let singleEndpointPath = process.env['SINGLE_ENDPOINT_PATH'];
    if (!singleEndpointPath) {
      singleEndpointPath = path.join(process.env['HOME'], 'proxy', 'run.sh');
      helper.trace("SINGLE_ENDPOINT_PATH not set, using " + singleEndpointPath);
    }

    return this.execCommand(singleEndpointPath);
  },

  stopContainer: function () {
    return this.execCommand('docker kill $(docker ps -a -q --filter ancestor=single_endpoint)');
  },

  execCommand: function (cmd) {
    return new Promise((resolve, reject) => exec(cmd, (err, stdout, stderr) => {
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

