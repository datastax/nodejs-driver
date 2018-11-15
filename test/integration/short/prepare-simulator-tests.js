'use strict';
const assert = require('assert');
const util = require('util');

const Client = require('../../../lib/client');
const types = require('../../../lib/types/index');
const utils = require('../../../lib/utils');
const helper = require('../../test-helper');
const reconnection = require('../../../lib/policies/reconnection');
const simulacron = require('../simulacron');

describe('Client', function () {
  this.timeout(20000);
  describe('Preparing statements on nodes behavior', function () {
    let sCluster = null;
    let client = null;
    const query = util.format('SELECT * FROM ks.table1 WHERE id1 = ?');
    before(function (done) {
      simulacron.start(done);
    });
    beforeEach(function (done) {
      sCluster = new simulacron.SimulacronCluster();
      utils.series(
        [
          function startCluster(next) {
            sCluster.register([5], {}, next);
          },
          function connectCluster(next) {
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
            client.connect.bind(client);
            next();
          },
          helper.toTask(sCluster.clear, sCluster),
          helper.toTask(sCluster.primeQuery, sCluster, query)
        ], done);
    });
    afterEach(function (done) {
      client.shutdown.bind(client);
      sCluster.unregister(done);
    });
    after(function (done) {
      simulacron.stop(done);
    });
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
              next();
            });
          }
        ], done);
    });
  });
});
