'use strict';
var assert = require('assert');
var utils = require('../../../lib/utils');
var Client = require('../../../lib/client');
var types = require('../../../lib/types/index');
var helper = require('../../test-helper');
var simulacron = require('../simulacron');

describe('Metadata', function () {
  this.timeout(20000);
  describe("#getTrace()", function () {
    var sCluster = null;
    before(function (done) {
      simulacron.start(done);
    });
    beforeEach(function (done) {
      utils.series(
        [
          function startCluster(next) {
            sCluster = new simulacron.SimulacronCluster();
            sCluster.start('5', {}, next);
          },
          function clearLog(next) {
            sCluster.clearLog(next);
          }
        ], done);
    });
    afterEach(function (done) {
      sCluster.destroy(done);
    });
    after(function (done) {
      simulacron.stop(done);
    });
    it('should set consistency level', function (done) {
      // use a single node
      var client = new Client({
        contactPoints: [sCluster.getContactPoints()[0]],
        policies: {
          retry: new helper.RetryMultipleTimes(3)
        }
      });
      var traceId = types.Uuid.random();
      var sessionQuery = 'SELECT * FROM system_traces.sessions WHERE session_id=1';
      var eventsQuery = 'SELECT * FROM system_traces.events WHERE session_id=1';
      var primeParams = {traceId: traceId};
      var primeParamTypes = {traceId: 'uuid'};

      var resultSessions = {
        result: 'success',
        delay_in_ms: 0,
        rows: [
          {
            session_id: traceId,
            client: '127.0.0.1',
            command: 'QUERY',
            coordinator: sCluster.getContactPoints()[0],
            duration: 10000,
            request: 'Execute CQL3 query',
            started_at: new types.LocalTime.now
          }
        ]
      };
      var resultEvents = {
        result: 'success',
        delay_in_ms: 0,
        rows: [
          {
            session_id: traceId,
            event_id: types.Uuid.random(),
            activity: 'Parsing...',
            source: sCluster.getContactPoints()[0],
            source_elapsed: 1000,
            thread: 'Native-Transport-Requests-2'
          }
        ],

      };
      utils.series([
        client.connect.bind(client),
        // function primeTraceSessionsQueries(next) {
        //   sCluster.primeQueryWithEmptyResult(sessionQuery, next);
        // },
        // function primeTraceEventsQueries(next) {
        //   sCluster.primeQueryWithEmptyResult(eventsQuery, next);
        // },
        function getTrace(next) {
          client.metadata.getTrace(traceId, types.consistencies.all, function (err, trace) {
            assert.ifError(err);
            assert.ok(trace);
            assert.ok(trace.events.length);
            next();
          });
        },
        function getTraceWithConsistency(next) {
          sCluster.queryNodeLog(sCluster.getContactPoints()[0], function (logs) {
            assert.notEqual(logs, null);
            for (var i = 0; i < logs.length; i++) {
              var queryLog = logs[i];
              if (queryLog.type === "QUERY" && (queryLog.query === sessionQuery
                || queryLog.query === eventsQuery)) {
                assert.strictEqual(queryLog.consistency_level, 'ALL');
                return next();
              }
            }
            assert.fail('Consistency not achieved');
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});