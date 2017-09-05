'use strict';
var assert = require('assert');
var util = require('util');

var Client = require('../../../lib/client');
var types = require('../../../lib/types/index');
var utils = require('../../../lib/utils');
var helper = require('../../test-helper');
var reconnection = require('../../../lib/policies/reconnection');
var simulacron = require('../simulacron');

describe('Client #prepare', function () {
  this.timeout(20000);
  describe('Preparing statements on nodes behavior', function () {
    var nodes = "5";
    var clusterName = "simulacronTest";
    var cassandraVersion = "3.10";
    var sCluster = null;
    var client = null;
    var query = util.format('SELECT * FROM ks.table1 WHERE id1 = ?');
    before(function (done) {
      simulacron.start(done);
    });
    beforeEach(function (done) {
      utils.series(
        [
          function startCluster(next) {
            sCluster = new simulacron.SimulacronCluster();
            sCluster.start(nodes, cassandraVersion, "", clusterName, true, 1, next);
          },
          function connectCluster(next) {
            var poolingOptions = {};
            poolingOptions[types.distance.local] = 1;
            client = new Client({
              contactPoints: [sCluster.getContactPoints()[0]],
              policies: {
                reconnection: new reconnection.ConstantReconnectionPolicy(100)
              },
              pooling: poolingOptions
            });
            client.connect.bind(client);
            next();
          },
          function primeQuery(next) {
            sCluster.primeQueryWithEmptyResult(query, next);
          },
          function clearLog(next) {
            sCluster.clearLog(next);
          }
        ], done);
    });
    afterEach(function (done) {
      client.shutdown.bind(client);
      sCluster.destroy(done);
    });
    after(function (done) {
      simulacron.stop(done);
    });
    it('should prepare query on all hosts', function (done) {
      var idRandom = types.Uuid.random();
      client.execute(query, [idRandom], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.strictEqual(client.hosts.length, 5);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        utils.eachSeries(client.hosts.values(), function(host, next) {
          sCluster.queryNodeLog(host.address, function(logs) {
            assert.ifError(err);
            var prepareQuery = logs.find(function(queryLog) {
              return queryLog.type === "PREPARE"
                && queryLog.query == query;
            });
            assert.notEqual(prepareQuery, undefined);
            next();
          });
        }, done);
      });
    });
    it('should re-prepare query when host go UP again', function (done) {
      var idRandom = types.Uuid.random();
      var nodeDownAddress = sCluster.getContactPoints()[4];
      utils.series(
        [
          function stopLastNode(next) {
            sCluster.stopNode(nodeDownAddress, next);
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
              sCluster.queryNodeLog(host.address, function(logs) {
                var prepareQuery = logs.find(function(queryLog) {
                  return queryLog.type === "PREPARE"
                    && queryLog.query == query;
                });
                if (prepareQuery == undefined) {
                  assert.strictEqual(nodeDownAddress, host.address);
                } else {
                  assert.notEqual(prepareQuery, undefined);
                }
                nextHost();
              });
            }, next);
          },
          function resumeLastNode(next) {
            var timeout = null;
            var nodeDown = client.hosts.get(nodeDownAddress);
            nodeDown.on('up', function() {
              clearTimeout(timeout);
              helper.trace("Node marked as UP");
              setTimeout(next, 1000); //give time for driver to re prepare statement
            });
            sCluster.resumeNode(nodeDownAddress, function() {
              timeout = setTimeout(next, 10000);
            });
          },
          function verifyPrepareQueryOnLastNode(next) {
            sCluster.queryNodeLog(nodeDownAddress, function(logs) {
              var prepareQuery = logs.find(function(queryLog) {
                return queryLog.type === "PREPARE"
                  && queryLog.query == query;
              });
              assert.notEqual(prepareQuery, undefined);
              next();
            });
          }
        ], done);
    });
  });
});
