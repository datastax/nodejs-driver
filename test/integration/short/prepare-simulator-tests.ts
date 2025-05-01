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
import { assert } from "chai";
import Client from "../../../lib/client";
import types from "../../../lib/types/index";
import utils from "../../../lib/utils";
import helper from "../../test-helper";
import reconnection from "../../../lib/policies/reconnection";
import simulacron from "../simulacron";


describe('Client', function () {
  this.timeout(20000);

  describe('Preparing statements on nodes behavior', function () {
    let sCluster = null;
    let client = null;

    const query = 'SELECT * FROM ks.table1 WHERE id1 = ?';
    const pausedQuery = 'SELECT * FROM paused';

    before(done => simulacron.start(done));

    beforeEach(done => {
      sCluster = new simulacron.SimulacronCluster();
      sCluster.register([5], {}, done);
    });

    beforeEach(done => {
      const poolingOptions = {};
      poolingOptions[types.distance.local] = 1;
      client = new Client({
        contactPoints: [sCluster.getContactPoints()[0]],
        localDataCenter: 'dc1',
        policies: {
          reconnection: new reconnection.ConstantReconnectionPolicy(100),
          retry: new helper.RetryMultipleTimes(3)
        },
        pooling: poolingOptions
      });

      client.connect(done);
    });

    beforeEach(done => sCluster.clear(done));
    beforeEach(done => sCluster.primeQuery(query, done));

    beforeEach(done => sCluster.node(0).prime({
      when: { query: pausedQuery },
      then: { result: 'success', delay_in_ms: 2000, ignore_on_prepare: false }
    }, done));

    afterEach(() => client.shutdown());
    afterEach(done => sCluster.unregister(done));
    after(done => simulacron.stop(done));

    it('should prepare query on all hosts', function (done) {
      const idRandom = types.Uuid.random();
      client.execute(query, [idRandom], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 5);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        utils.eachSeries(client.hosts.values(), function(host, next) {
          sCluster.node(host.address).getLogs(function(err, logs) {
            assert.ifError(err);
            let prepareQuery;
            for(let i = 0; i < logs.length; i++) {
              const queryLog = logs[i];
              if (queryLog.type === "PREPARE" && queryLog.query === query) {
                prepareQuery = queryLog;
              }
            }
            if (!prepareQuery) {
              assert.fail('Query no prepared on all hosts');
            }
            next();
          });
        }, done);
      });
    });

    it('should re-prepare query when host go UP again', function (done) {
      const idRandom = types.Uuid.random();
      const nodeDownAddress = sCluster.getContactPoints()[4];

      const logMessages = [];
      const logRegex = /Re-preparing all queries on host [\d.:]+ before setting it as UP/i;

      // Verify log messages
      client.on('log', (level, className, message) => {
        if (level === 'info' && className === 'Host' && logRegex.test(message)) {
          logMessages.push(message);
        }
      });

      utils.series(
        [
          function stopLastNode(next) {
            sCluster.node(nodeDownAddress).stop(next);
          },
          function runQuery(next) {
            utils.timesSeries(5, function (n, nextIteration) {
              client.execute(query, [idRandom], {prepare: 0}, function (err, result) {
                assert.ifError(err);
                assert.strictEqual(client.hosts.length, 5);
                assert.notEqual(result, null);
                assert.notEqual(result.rows, null);
                nextIteration();
              });
            }, next);
          },
          function verifyIfNodeIsMarkedDown(next) {
            const nodeDown = client.hosts.get(nodeDownAddress);
            assert(!nodeDown.isUp());
            next();
          },
          function prepareQuery(next) {
            client.execute(query, [idRandom], {prepare: 1}, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(client.hosts.length, 5);
              assert.notEqual(result, null);
              assert.notEqual(result.rows, null);
              next();
            });
          },
          function verifyLogs(next) {
            utils.eachSeries(client.hosts.values(), function(host, nextHost) {
              sCluster.node(host.address).getLogs(function(err, logs) {
                assert.ifError(err);
                let prepareQuery;
                for(let i = 0; i < logs.length; i++) {
                  const queryLog = logs[i];
                  if (queryLog.type === "PREPARE" && queryLog.query === query) {
                    prepareQuery = queryLog;
                  }
                }
                if (!prepareQuery) {
                  assert.strictEqual(nodeDownAddress, host.address);
                } else {
                  assert.notEqual(prepareQuery, undefined);
                }
                nextHost();
              });
            }, next);
          },
          function resumeLastNode(next) {
            const nodeDown = client.hosts.get(nodeDownAddress);
            nodeDown.on('up', function() {
              helper.trace("Node marked as UP");
              setTimeout(next, 1000); //give time for driver to re prepare statement
            });
            sCluster.node(nodeDownAddress).start(function() {
            });
          },
          function verifyPrepareQueryOnLastNode(next) {
            sCluster.node(nodeDownAddress).getLogs(function(err, logs) {
              assert.ifError(err);
              let prepareQuery;
              for(let i = 0; i < logs.length; i++) {
                const queryLog = logs[i];
                if (queryLog.type === "PREPARE" && queryLog.query === query) {
                  prepareQuery = queryLog;
                }
              }
              if (!prepareQuery) {
                assert.fail('Query no prepared on restarted host');
              }

              assert.lengthOf(logMessages, 1);

              next();
            });
          }
        ], done);
    });
  });
});
