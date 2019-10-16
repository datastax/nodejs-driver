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
const net = require('net');
const simulacron = require('../simulacron');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');

const Client = require('../../../lib/client.js');

describe('ControlConnection', function() {
  this.timeout(5000);

  before(simulacron.start.bind(simulacron));
  after(simulacron.stop.bind(simulacron));

  describe("#init", function() {
    it('should downgrade to protocol v3 with versions 3.0 & 2.1', testWithNodes(['3.0.13', '2.1.17'], 3));
    it('should downgrade to protocol v3 with versions 2.2 & 2.1', testWithNodes(['2.2.11', '2.1.17'], 3));
    it('should downgrade to protocol v2 with versions 2.2 & 2.0', testWithNodes(['2.2.11', '2.0.17'], 2));
    it('should downgrade to protocol v1 with versions 2.2 & 1.2', testWithNodes(['2.2.11', '1.2.19'], 1));
    it('should downgrade to protocol v2 with versions 2.1 & 2.0', testWithNodes(['2.1.17', '2.0.17'], 2, 3));
    it('should downgrade to protocol v1 with versions 2.1 & 1.2', testWithNodes(['2.1.17', '1.2.19'], 1, 3));
    it('should downgrade to protocol v1 with versions 2.0 & 1.2', testWithNodes(['2.0.17', '1.2.19'], 1, 3));
    // no need to downgrade since both support protocol V4.
    it('should not downgrade with versions 3.0 & 2.2', testWithNodes(['3.0.13', '3.0.11', '2.2.9'], 4));
    // can't downgrade because C* 3.0 does not support protocol V2.
    it('should not downgrade with versions 3.0 & 2.0', testWithNodes(['3.0.13', '2.0.17'], 4));
    // can't downgrade because C* 3.0 does not support protocol V1.
    it('should not downgrade with versions 3.0 & 1.2', testWithNodes(['3.0.13', '1.2.19'], 4));
  });

  describe('#getLocalAddress()', () => {
    const simulacronCluster = new simulacron.SimulacronCluster();

    before(done => simulacronCluster.register([5], null, done));

    after(done => simulacronCluster.unregister(done));

    it('should retrieve the local ip address of the host', () => {
      const client = new Client({ contactPoints: [simulacron.startingIp], localDataCenter: 'dc1'});

      return client.connect()
        .then(() => {
          const cc = client.controlConnection;
          assert.strictEqual(typeof cc.getLocalAddress(), 'string');
          assert.ok(net.isIP(cc.getLocalAddress()));
        })
        .then(() => client.shutdown());
    });
  });
});

function testWithNodes(nodeVersions, expectedProtocolVersion, maxVersion) {
  const nodes = [];
  for (let i = 0; i < nodeVersions.length; i++) {
    nodes.push({
      id: i,
      cassandra_version: nodeVersions[i]
    });
  }

  const clientOptions = {
    contactPoints: [simulacron.startingIp],
    localDataCenter: 'dc1',
  };

  if (maxVersion) {
    clientOptions.protocolOptions = { maxVersion: maxVersion };
  }

  const usedMaxVersion = maxVersion ? maxVersion : types.protocolVersion.maxSupported;

  return function (done) {
    const client = new Client(clientOptions);

    function cleanUp(err) {
      client.shutdown(() => cluster.unregister(function() {
        assert.ifError(err);
        done();
      }));
    }

    const cluster = new simulacron.SimulacronCluster();
    utils.series([
      function register(next) {
        cluster.registerWithBody({
          data_centers: [
            {
              id: 0,
              name: "dc1",
              nodes: nodes
            }
          ]
        }, next);
      },
      function connect(next) {
        client.connect(function (err, result) {
          if (expectedProtocolVersion < 3) {
            // An error is expected here since simulacron can't connect with < protocol v3.
            assert.ok(err);
            next();
          } else {
            assert.ifError(err);
            next();
          }
        });
      },
      function validateInitQueries(next) {
        // validate initialization queries.
        cluster.node(0).getLogs(function (err, logs) {
          const firstVersionLogs = logs.slice(0, 4);
          // Expect 3 initial messages using the max protocol version:
          // 1 - STARTUP
          // 2 - OPTIONS
          // 3 - local query
          // 4 - peers query
          firstVersionLogs.forEach((log) => {
            assert.strictEqual(log.frame.protocol_version, usedMaxVersion);
          });

          const remainingLogs = logs.slice(4);
          if (expectedProtocolVersion >= 3) {
            remainingLogs.forEach((log) => {
              assert.strictEqual(log.frame.protocol_version, expectedProtocolVersion);
            });
            // If downgraded, expect an additional startup message.
            // 2 other messages, schema query and register.
            if (expectedProtocolVersion !== usedMaxVersion) {
              assert.strictEqual(remainingLogs[0].frame.message.type, 'STARTUP');
              assert.strictEqual(remainingLogs.length, 3);
            } else {
              assert.strictEqual(remainingLogs.length, 2);
            }
            next();
          } else {
            // Since simulacron does not support < V3, check that older version was tried
            // and that's it.  We don't validate additional remaining logs as the driver
            // will try connecting at even lower protocol versions.
            assert.strictEqual(remainingLogs[0].frame.message.type, 'STARTUP');
            assert.strictEqual(remainingLogs[0].frame.protocol_version, expectedProtocolVersion);
            cleanUp();
          }
        });
      },
      cluster.clearLogs.bind(cluster),
      function query(next) {
        utils.times(nodeVersions.length * 3, (n, nNext) => {
          client.execute('select * from tbl', nNext);
        }, next);
      },
      function verifyQueriesAtProtocolVersion(next) {
        cluster.getLogs(function (err, logs) {
          const nodes = logs.data_centers[0].nodes;
          assert.strictEqual(nodes.length, nodeVersions.length);
          const queries = nodes.reduce((queries, node) => queries.concat(node.queries), [])
            .filter((q) => q.frame.message.type === 'QUERY');
          assert.strictEqual(queries.length, nodeVersions.length * 3);
          queries.forEach((log) => {
            assert.strictEqual(log.frame.protocol_version, expectedProtocolVersion);
          });
          next();
        });
      }
    ], function (err) {
      cleanUp(err);
    });
  };
}