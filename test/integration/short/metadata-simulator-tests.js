'use strict';
var assert = require('assert');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types/index');
var simulacron = require('../simulacron');
var util = require('util');

describe('Metadata', function () {
  this.timeout(20000);
  describe("#getTrace()", function () {
    var setupInfo = simulacron.setup('3', null);
    var sCluster = setupInfo.cluster;
    var client = setupInfo.client;

    it('should set consistency level', function (done) {
      var traceId = types.Uuid.random();
      var sessionQuery = util.format('SELECT * FROM system_traces.sessions WHERE session_id=%s', traceId);
      var eventsQuery = util.format('SELECT * FROM system_traces.events WHERE session_id=%s', traceId);

      var resultSessions = {
        result: 'success',
        delay_in_ms: 0,
        rows: [
          {
            session_id: traceId,
            client: '127.0.0.1',
            command: 'QUERY',
            coordinator: "127.0.0.101",
            duration: 10000,
            request: 'Execute CQL3 query',
            started_at: new types.LocalTime.now().getTotalNanoseconds()
          }
        ],
        column_types: {
          session_id: "uuid",
          command: "varchar",
          coordinator: "inet",
          duration: "int",
          request: "varchar",
          started_at: "timestamp"
        }
      };
      var resultEvents = {
        result: 'success',
        delay_in_ms: 0,
        rows: [
          {
            session_id: traceId,
            event_id: types.Uuid.random(),
            activity: 'Parsing...',
            source: "127.0.0.1",
            source_elapsed: 1000,
            thread: 'Native-Transport-Requests-2'
          }
        ],
        column_types: {
          session_id: "uuid",
          event_id: "uuid",
          activity: "varchar",
          source: "inet",
          source_elapsed: "int",
          thread: "varchar"
        }
      };
      utils.series([
        function primeTraceSessionsQueries(next) {
          sCluster.prime({
            when: {
              query: sessionQuery,
            },
            then : resultSessions
          }, next);
        },
        function primeTraceEventsQueries(next) {
          sCluster.prime({
            when: {
              query: eventsQuery,
            },
            then : resultEvents
          }, next);
        },
        function getTrace(next) {
          client.metadata.getTrace(traceId, types.consistencies.all, function (err, trace) {
            assert.ifError(err);
            assert.ok(trace);
            assert.ok(trace.events.length);
            next();
          });
        },
        function getTraceWithConsistency(next) {
          var node0 = sCluster.node(0);
          node0.getLogs(function (err, logs) {
            assert.ifError(err);
            assert.notEqual(logs, null);
            var verifyQuery = function(logsToSearch, query, consistency) {
              for (var i = 0; i < logsToSearch.length; i++) {
                var log = logsToSearch[i];
                if (log.type === "QUERY" && log.query === query && log.consistency_level === consistency) {
                  return true;
                }
              }
              return false;
            };
            assert(verifyQuery(logs, sessionQuery, 'ALL'));
            assert(verifyQuery(logs, eventsQuery, 'ALL'));
            next();
          });
        }
      ], done);
    });
  });
});