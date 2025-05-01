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

import utils from "../../../lib/utils";
import types from "../../../lib/types/index";
import simulacron from "../simulacron";
import util from "util";
import {assert} from "chai";

describe('Metadata', function () {
  this.timeout(20000);
  describe("#getTrace()", function () {
    const setupInfo = simulacron.setup('3', null);
    const sCluster = setupInfo.cluster;
    const client = setupInfo.client;

    it('should set consistency level', function (done) {
      const traceId = types.Uuid.random();
      const sessionQuery = util.format('SELECT * FROM system_traces.sessions WHERE session_id=%s', traceId);
      const eventsQuery = util.format('SELECT * FROM system_traces.events WHERE session_id=%s', traceId);

      const resultSessions = {
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
            started_at: types.LocalTime.now().getTotalNanoseconds()
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
      const resultEvents = {
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
          const node0 = sCluster.node(0);
          node0.getLogs(function (err, logs) {
            assert.ifError(err);
            assert.notEqual(logs, null);
            const verifyQuery = function(logsToSearch, query, consistency) {
              for (let i = 0; i < logsToSearch.length; i++) {
                const log = logsToSearch[i];
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

  describe('#compareSchemaVersions()', function () {
    const setupInfo = simulacron.setup('6');
    const client = setupInfo.client;

    // Validate that simulacron is returning values for this queries
    before(() =>
      client.execute('SELECT schema_version FROM system.peers')
        .then(rs => assert.lengthOf(rs.rows, 5)));

    before(() =>
      client.execute('SELECT schema_version FROM system.local')
        .then(rs => assert.lengthOf(rs.rows, 1)));

    context('with callback specified', () => {

      it('should return true when the schema version is the same', done =>
        client.metadata.checkSchemaAgreement((err, agreement) => {
          assert.ifError(err);
          assert.strictEqual(agreement, true);
          done();
        }));
    });

    context('with no callback specified', () => {
      it('should return true when the schema version is the same', () =>
        client.metadata.checkSchemaAgreement().then(agreement => assert.strictEqual(agreement, true)));
    });
  });
});