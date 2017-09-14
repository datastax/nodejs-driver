'use strict';
var assert = require('assert');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types/index');
var helper = require('../../test-helper');
var simulacron = require('../simulacron');
var defaultOptions = require('../../../lib/client-options.js').defaultOptions();
var requests = require('../../../lib/requests.js');

describe('Connection', function () {
  this.timeout(20000);
  describe("#sendStream", function () {
    var sCluster = null;
    var connection = null;
    before(function (done) {
      simulacron.start(done);
    });
    beforeEach(function (done) {
      utils.series(
        [
          function startCluster(next) {
            sCluster = new simulacron.SimulacronCluster();
            sCluster.register('5', {}, next);
          },
          function connect(next) {
            var addressAndPort = sCluster.getContactPoints()[0].split(':');
            var address = addressAndPort[0];
            var port = addressAndPort[1];

            var options = utils.extend({}, defaultOptions);
            options.policies.retry = new helper.RetryMultipleTimes(3);
            options.contactPoints = [address];
            options.protocolOptions.port = port;

            connection = helper.newConnection(address, null, options);
            connection.open(next);
          },
          function clearLog(next) {
            sCluster.clearLogs(next);
          }
        ], done);
    });
    afterEach(function (done) {
      sCluster.unregister(done);
    });
    after(function (done) {
      simulacron.stop(done);
    });
    it('should use consistency level specified at QueryRequest', function (done) {
      var idRandom = types.Uuid.random();
      var testRequests = [
        {
          request: new requests.QueryRequest('SELECT * FROM system_traces.events WHERE session_id=?',
            [idRandom], {consistency: types.consistencies.all}),
          expectedConsistency: 'ALL'
        },
        {
          request: new requests.QueryRequest('SELECT * FROM system_traces.sessions WHERE session_id=?',
            [idRandom], {consistency: types.consistencies.one}),
          expectedConsistency: 'ONE'
        },
        {
          request: new requests.QueryRequest('SELECT schema_version FROM system.peers',
            [], {consistency: types.consistencies.quorum}),
          expectedConsistency: 'QUORUM'
        }
      ];
      utils.eachSeries(testRequests,
        function (req, nextRequest) {
          connection.sendStream(req.request, null, function() {
            sCluster.node(0).getLogs(function (err, logs) {
              assert.ifError(err);
              assert.notEqual(logs, null);
              for (var i = 0; i < logs.length; i++) {
                var queryLog = logs[i];
                if (queryLog.type === "QUERY" && queryLog.query === req.request.query) {
                  assert.strictEqual(queryLog.consistency_level, req.expectedConsistency);
                  return nextRequest();
                }
              }
              assert.fail('Consistency not achieved');
            });
          });
        }, done);
    });
  });
});
