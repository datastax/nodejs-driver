/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const https = require('https');
const format = require('util').format;
const exec = require('child_process').exec;

const Client = require('../../../../lib/dse-client');
const helper = require('../../../test-helper');
const ccmCmdString = 'docker exec $(docker ps -a -q  --filter ancestor=single_endpoint) ccm %s';

module.exports = {
  setup: function() {
    const setupInfo = {
      contactPoints: [],
      proxyAddress: null,
      localDataCenter: null,
      getClient: function (options) {
        const client = new Client(Object.assign({
          contactPoints: this.contactPoints,
          localDataCenter: this.localDataCenter,
          protocolOptions: { maxVersion: 4 },
          sni: { address: this.proxyAddress},
          sslOptions: {
            checkServerIdentity: () => undefined,
            rejectUnauthorized: false
          }
        }, options));

        after(() => client.shutdown());

        return client;
      }
    };

    before(done => {
      let errCalled = false;

      const requestOptions = {
        hostname: '127.0.0.1',
        port: 30443,
        path: '/metadata',
        rejectUnauthorized: false
      };

      https.get(requestOptions, (res) => {
        let data = '';

        res
          .on('data', (chunk) => {
            data += chunk;
          })
          .on('end', () => {
            if (errCalled) {
              return;
            }

            const message = JSON.parse(data);
            const contactInfo = message['contact_info'];
            setupInfo.proxyAddress = contactInfo['sni_proxy_address'];
            setupInfo.localDataCenter = contactInfo['local_dc'];
            contactInfo['contact_points'].forEach(n => setupInfo.contactPoints.push(n));
            done();
          });

      }).on("error", (err) => {
        errCalled = true;
        done(err);
      });

    });

    return setupInfo;
  },
  execCcm: function (cmd) {
    return new Promise((resolve, reject) => exec(format(ccmCmdString, cmd), (err, stdout, stderr) => {
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

