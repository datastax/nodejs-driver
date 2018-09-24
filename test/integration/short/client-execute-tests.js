"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const ExecutionProfile = require('../../../lib/execution-profile.js').ExecutionProfile;
const types = require('../../../lib/types');
const utils = require('../../../lib/utils.js');
const errors = require('../../../lib/errors.js');
const vit = helper.vit;
const vdescribe = helper.vdescribe;

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 0}, callback)', function () {
    const keyspace = helper.getRandomName('ks');
    const table = keyspace + '.' + helper.getRandomName('table');
    const selectAllQuery = 'SELECT * FROM ' + table;
    const setupInfo = helper.setup(1, { keyspace: keyspace, queries: [ helper.createTableCql(table) ] });
    it('should execute a basic query', function (done) {
      const client = setupInfo.client;
      client.execute(helper.queries.basic, function (err, result) {
        assert.equal(err, null);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        done();
      });
    });
    it('should callback with syntax error', function (done) {
      const client = setupInfo.client;
      client.connect(function (err) {
        assert.ifError(err);
        const query = 'SELECT WILL FAIL';
        client.execute(query, function (err, result) {
          assert.ok(err);
          assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
          assert.strictEqual(err.query, query);
          assert.equal(result, null);
          done();
        });
      });
    });
    it('should callback with an empty Array instance as rows when not found', function (done) {
      const client = setupInfo.client;
      client.execute(helper.queries.basicNoResults, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(util.isArray(result.rows));
        helper.assertInstanceOf(result, types.ResultSet);
        assert.strictEqual(result.rows.length, 0);
        done();
      });
    });
    it('should handle 250 parallel queries', function (done) {
      const client = setupInfo.client;
      utils.times(250, function (n, next) {
        client.execute(helper.queries.basic, [], next);
      }, done);
    });
    it('should fail if non-existent profile provided', function (done) {
      const client = newInstance();
      utils.series([
        function queryWithBadProfile(next) {
          client.execute(helper.queries.basicNoResults, [], {executionProfile: 'none'}, function(err) {
            assert.ok(err);
            helper.assertInstanceOf(err, errors.ArgumentError);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.0', 'should guess known types', function (done) {
      const client = setupInfo.client;
      const columns = 'id, timeuuid_sample, text_sample, double_sample, timestamp_sample, blob_sample, list_sample';
      //a precision a float32 can represent
      const values = [types.Uuid.random(), types.TimeUuid.now(), 'text sample 1', 133, new Date(121212211), utils.allocBufferUnsafe(100), ['one', 'two']];
      //no hint
      insertSelectTest(client, table, columns, values, null, done);
    });
    vit('2.0', 'should use parameter hints as number for simple types', function (done) {
      const client = setupInfo.client;
      const columns = 'id, text_sample, float_sample, int_sample';
      //a precision a float32 can represent
      const values = [types.Uuid.random(), 'text sample', 1000.0999755859375, -12];
      const hints = [types.dataTypes.uuid, types.dataTypes.text, types.dataTypes.float, types.dataTypes.int];
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for simple types', function (done) {
      const columns = 'id, text_sample, float_sample, int_sample';
      const values = [types.Uuid.random(), 'text sample', -9, 1];
      const hints = [null, 'text', 'float', 'int'];
      const client = setupInfo.client;
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for complex types partial', function (done) {
      const columns = 'id, map_sample, list_sample, set_sample';
      const values = [types.Uuid.random(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      const hints = [null, 'map', 'list', 'set'];
      const client = setupInfo.client;
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for complex types complete', function (done) {
      const columns = 'id, map_sample, list_sample, set_sample';
      const values = [types.Uuid.random(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      //complete info
      const hints = [null, 'map<text, text>', 'list<text>', 'set<text>'];
      const client = setupInfo.client;
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints for custom map polyfills', function (done) {
      const columns = 'id, map_sample';
      const map = new helper.Map();
      map.set('k1', 'value 1');
      map.set('k2', 'value 2');
      const values = [types.Uuid.random(), map];
      //complete info
      const hints = [null, 'map<text, text>'];
      const client = newInstance({encoding: { map: helper.Map }});
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use pageState and fetchSize', function (done) {
      const client = setupInfo.client;
      let pageState = null;
      utils.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.execute(query, [types.Uuid.random(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Only fetch 70
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 70}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 70);
            pageState = result.pageState;
            //ResultSet#pageState is the hex string representation of the meta.pageState
            assert.strictEqual(pageState, result.meta.pageState.toString('hex'));
            seriesNext();
          });
        },
        function selectDataRemaining(seriesNext) {
          //The remaining
          client.execute(util.format('SELECT * FROM %s', table), [], {pageState: pageState}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 30);
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.0', 'should not autoPage', function (done) {
      const client = setupInfo.client;
      utils.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.execute(query, [types.Uuid.random(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //It should only return the first page
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 65, autoPage: true}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 65);
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.0', 'should return ResultSet compatible with @@iterator', function (done) {
      const client = setupInfo.client;
      utils.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.execute(query, [types.Uuid.random(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //It should only return the first page and iteration should not invoke next page.
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 25, autoPage: true}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 25);
            // should not page
            const iterator = result[Symbol.iterator]();
            let count = 0;
            const uuids = [];
            let item;
            for (item = iterator.next(); !item.done; item = iterator.next()) {
              assert.ok(item.value);
              const id = item.value.id;
              // should not encounter same id twice.
              assert.strictEqual(uuids.indexOf(id), -1);
              uuids.push(item.value.id);
              count++;
            }

            // last item should be done with no value.
            assert.strictEqual(item.done, true);
            assert.strictEqual(item.value, undefined);
            // should have only retrieved rows from first page.
            assert.strictEqual(count, 25);
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.0', 'should callback in err when wrong hints are provided', function (done) {
      const client = setupInfo.client;
      const query = util.format('SELECT * FROM %s WHERE id IN (?, ?, ?)', table);
      //valid params
      const params = [types.Uuid.random(), types.Uuid.random(), types.Uuid.random()];
      utils.series([
        client.connect.bind(client),
        function hintsArrayAsObject(next) {
          client.execute(query, params, {hints: {}}, function (err) {
            //it should not fail
            next(err);
          });
        },
        function hintsDifferentAmount(next) {
          client.execute(query, params, {hints: ['uuid']}, function (err) {
            //it should not fail
            next(err);
          });
        },
        function hintsArrayWrongSubtype(next) {
          client.execute(query, params, {hints: [[]]}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertNotInstanceOf(err, errors.NoHostAvailableError);
            next();
          });
        },
        function hintsInvalidStrings(next) {
          client.execute(query, params, {hints: ['zzz', 'mmmm']}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertNotInstanceOf(err, errors.NoHostAvailableError);
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should encode CONTAINS parameter', function (done) {
      const client = setupInfo.client;
      client.execute(util.format('CREATE INDEX list_sample_index ON %s(list_sample)', table), function (err) {
        assert.ifError(err);
        // Allow 1 second for index to build (otherwise an IndexNotAvailableException may be raised while index is building).
        setTimeout(function() {
          const query = util.format('SELECT * FROM %s WHERE list_sample CONTAINS ? AND list_sample CONTAINS ? ALLOW FILTERING', table);
          //valid params
          const params = ['val1', 'val2'];
          client.execute(query, params, function (err) {
            //it should not fail
            assert.ifError(err);
            done();
          });
        }, 1000);
      });
    });
    it('should accept localOne and localQuorum consistencies', function (done) {
      const client = setupInfo.client;
      utils.series([
        function (next) {
          client.execute(selectAllQuery, [], {consistency: types.consistencies.localOne}, next);
        },
        function (next) {
          client.execute(selectAllQuery, [], {consistency: types.consistencies.localQuorum}, next);
        }
      ], done);
    });
    it('should use consistency level from profile and override profile when provided in query options', function (done) {
      const client = newInstance({profiles: [new ExecutionProfile('cl', {consistency: types.consistencies.quorum})]});
      utils.series([
        function ensureProfileCLUsed (next) {
          client.execute(selectAllQuery, [], {executionProfile: 'cl'}, function(err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.achievedConsistency, types.consistencies.quorum);
            next();
          });
        },
        function ensureQueryCLUsed (next) {
          client.execute(selectAllQuery, [], {executionProfile: 'cl', consistency: types.consistencies.one}, function(err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.achievedConsistency, types.consistencies.one);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    vit('2.2', 'should accept unset as a valid value', function (done) {
      const client = setupInfo.client;
      const id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          const query = util.format('INSERT INTO %s (id, text_sample, double_sample) VALUES (?, ?, ?)', table);
          client.execute(query, [id, 'sample unset', types.unset], next);
        },
        function select(next) {
          const query = util.format('SELECT id, text_sample, double_sample FROM %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 1);
            const row = result.first();
            assert.strictEqual(row['text_sample'], 'sample unset');
            assert.strictEqual(row['double_sample'], null);
            next();
          });
        }
      ], done);
    });
    it('should handle several concurrent executes while the pool is not ready', function (done) {
      const client = newInstance({pooling: {
        coreConnectionsPerHost: {
          //lots of connections per host
          '0': 100,
          '1': 1,
          '2': 0
        }}});
      const execute = function (next) {
        client.execute(selectAllQuery, next);
      };
      utils.parallel([
        function (parallelNext) {
          utils.parallel(helper.fillArray(400, execute), parallelNext);
        },
        function (parallelNext) {
          utils.times(200, function (n, next) {
            setTimeout(() => execute(next), n * 5 + 50);
          }, parallelNext);
        }
      ], helper.finish(client, done));
    });
    it('should return the column definitions', function (done) {
      const client = setupInfo.client;
      //insert at least 1 row
      const insertQuery = util.format('INSERT INTO %s (id) VALUES (%s)', table, types.Uuid.random());
      utils.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, insertQuery),
        function verifyColumns(next) {
          const query = util.format('SELECT text_sample, timestamp_sample, int_sample, timeuuid_sample, list_sample2, map_sample from %s LIMIT 1', table);
          client.execute(query, function (err, result) {
            assert.ifError(err);
            assert.ok(result.rows.length);
            assert.ok(result.columns);
            assert.ok(util.isArray(result.columns));
            assert.strictEqual(result.columns.length, 6);
            assert.strictEqual(result.columns[1].type.code, types.dataTypes.timestamp);
            assert.equal(result.columns[1].type.info, null);
            assert.strictEqual(result.columns[2].type.code, types.dataTypes.int);
            assert.strictEqual(result.columns[4].name, 'list_sample2');
            assert.strictEqual(result.columns[4].type.code, types.dataTypes.list);
            assert.ok(result.columns[4].type.info);
            assert.strictEqual(result.columns[4].type.info.code, types.dataTypes.int);
            assert.strictEqual(result.columns[5].type.code, types.dataTypes.map);
            assert.ok(
              result.columns[5].type.info[0].code === types.dataTypes.text ||
              result.columns[5].type.info[0].code === types.dataTypes.varchar);
            next();
          });
        },
        function verifyColumnsInAnEmptyResultSet(next) {
          const query = util.format('SELECT * from %s WHERE id = 00000000-0000-0000-0000-000000000000', table);
          client.execute(query, function (err, result) {
            assert.ifError(err);
            assert.ok(result.columns);
            assert.ok(result.columns.length);
            next();
          });
        }
      ], done);
    });
    it('should return rows that are serializable to json', function (done) {
      const client = setupInfo.client;
      const id = types.Uuid.random();
      const timeId = types.TimeUuid.now();
      utils.series([
        function insert(next) {
          const query = util.format(
            'INSERT INTO %s (id, timeuuid_sample, inet_sample, bigint_sample, decimal_sample) VALUES (%s, %s, \'%s\', %s, %s)',
            table, id, timeId, '::2233:0:0:bb', -100, "0.1");
          client.execute(query, next);
        },
        function select(next) {
          const query = util.format(
            'SELECT id, timeuuid_sample, inet_sample, bigint_sample, decimal_sample from %s WHERE id = %s', table, id);
          client.execute(query, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 1);
            const row = result.rows[0];
            const expected = util.format('{"id":"%s",' +
              '"timeuuid_sample":"%s",' +
              '"inet_sample":"::2233:0:0:bb",' +
              '"bigint_sample":"-100",' +
              '"decimal_sample":"0.1"}', id, timeId);
            assert.strictEqual(JSON.stringify(row), expected);
            next();
          });
        }
      ], done);
    });
    vit('2.0', 'should use serial consistency level from profile and override profile when provided in query options', function (done) {
      // This is a bit crude, but sets an invalid serial CL (ONE) and ensures an error is thrown when using the
      // profile serial CL.  This establishes a way to differentiate between when the profile cl is used and not.
      const client = newInstance({profiles: [new ExecutionProfile('cl', {serialConsistency: types.consistencies.one})]});
      const id = types.Uuid.random();
      utils.series([
        function insertWithProfileSerialCL(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', table);
          client.execute(query, [id, 'hello serial'], { executionProfile: 'cl'}, function(err) {
            // expect an error as we used an invalid serial CL.
            assert.ok(err);
            assert.strictEqual(err.code, 0x2200); // should be an invalid query.
            next();
          });
        },
        function insertWithQueryCL(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', table);
          client.execute(query, [id, 'hello serial'], { executionProfile: 'cl', serialConsistency: types.consistencies.localSerial}, next);
        },
        function select(next) {
          const query = util.format('SELECT id, text_sample from %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            const row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'hello serial');
            next();
          });
        }
      ], helper.finish(client, done));
    });
    vit('2.1', 'should support protocol level timestamp', function (done) {
      const client = setupInfo.client;
      const id = types.Uuid.random();
      const timestamp = types.generateTimestamp(new Date(), 777);
      utils.series([
        function insert(next) {
          const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          client.execute(query, [id, 'hello timestamp'], { timestamp: timestamp}, next);
        },
        function select(next) {
          const query = util.format('SELECT id, text_sample, writetime(text_sample) from %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            const row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'hello timestamp');
            assert.strictEqual(row['writetime(text_sample)'].toString(), timestamp.toString());
            next();
          });
        }
      ], done);
    });
    it('should retrieve the trace id when queryTrace flag is set', function (done) {
      const client = setupInfo.client;
      const id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function selectNotExistent(next) {
          const query = util.format('SELECT * FROM %s WHERE id = %s', table, types.Uuid.random());
          client.execute(query, [], { traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function insertQuery(next) {
          const query = util.format('INSERT INTO %s (id) VALUES (%s)', table, id.toString());
          client.execute(query, [], { traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function selectSingleRow(next) {
          const query = util.format('SELECT * FROM %s WHERE id = %s', table, id.toString());
          client.execute(query, [], { traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        }
      ], done);
    });
    it('should not retrieve trace id by default', function (done) {
      const client = setupInfo.client;
      client.execute('SELECT * FROM system.local', function (err, result) {
        assert.ifError(err);
        assert.ok(result.info);
        assert.equal(result.info.traceId, null); //its undefined really but anything that evaluates as null is OK
        done();
      });
    });
    vit('2.2', 'should include the warning in the ResultSet', function (done) {
      const client = setupInfo.client;
      let loggedMessage = false;
      client.on('log', function (level, className, message) {
        if (loggedMessage || level !== 'warning') {
          return;
        }
        message = message.toLowerCase();
        if (message.indexOf('batch') >= 0 && message.indexOf('exceeding')) {
          loggedMessage = true;
        }
      });
      const query = util.format(
        "BEGIN UNLOGGED BATCH INSERT INTO %s (id, text_sample) VALUES (%s, '%s')\n" +
        "INSERT INTO %s (id, text_sample) VALUES (%s, '%s') APPLY BATCH",
        table,
        types.Uuid.random(),
        utils.stringRepeat('a', 2 * 1025),
        table,
        types.Uuid.random(),
        utils.stringRepeat('a', 3 * 1025)
      );
      client.execute(query, function (err, result) {
        assert.ifError(err);
        assert.ok(result.info.warnings);
        assert.strictEqual(result.info.warnings.length, 1);
        helper.assertContains(result.info.warnings[0], 'batch');
        helper.assertContains(result.info.warnings[0], 'exceeding');
        assert.ok(loggedMessage);
        done();
      });
    });
    it('should support buffer as input for any data type', () => {
      const buffer4 = utils.allocBufferFromArray([0, 0, 0, 1]);
      const buffer8 = utils.allocBuffer(8);
      const buffer16 = types.Uuid.random().getBuffer();

      const client = setupInfo.client;
      const insertQuery = `INSERT INTO ${table}` +
        ' (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, inet_sample, list_sample2)' +
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
      const selectQuery = `SELECT * FROM ${table} WHERE id = ?`;
      const params = [ buffer16, buffer4, buffer4, buffer8, buffer4, buffer8, buffer4, [ buffer4 ] ];

      return Promise.all([ false, true ].map(prepare =>
        client.execute(insertQuery, params, { prepare })
          .then(() => client.execute(selectQuery, [ buffer16 ]))
          .then(rs => {
            const row = rs.first();
            assert.ok(row);
            assert.strictEqual(row['id'].toString(), new types.Uuid(buffer16).toString());
            assert.strictEqual(row['text_sample'], buffer4.toString('utf8'));
            assert.strictEqual(row['int_sample'], 1);
            assert.strictEqual(row['bigint_sample'].toString(), '0');
            assert.strictEqual(row['float_sample'], buffer4.readFloatBE(0));
            assert.strictEqual(row['double_sample'], 0);
            assert.strictEqual(row['inet_sample'].toString(), '0.0.0.1');
            assert.deepStrictEqual(row['list_sample2'], [ 1 ]);
          })));
    });
    vdescribe('3.0.16', 'with noCompact', function () {
      before(function (done) {
        // While C* 4.0 supports the NO_COMPACT option, there is no way to create
        // COMPACT STORAGE tables other than creating with an older C* version and
        // then upgrading which is outside the scope of this test.
        if (helper.isCassandraGreaterThan('4.0')) {
          this.skip();
          return;
        }
        const client = newInstance({keyspace: keyspace, protocolOptions: { noCompact: true }});
        utils.series([
          client.connect.bind(client),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_cs (key blob PRIMARY KEY, bar int, baz uuid) WITH COMPACT STORAGE'),
          helper.toTask(client.execute, client, "INSERT INTO tbl_cs (key, bar, baz, column1, value) values (0xc0, 10, 33cb65d4-6721-4ca8-854f-1f020c5353cb, 'yak', 0xcafedead)"),
        ], done);
      });
      it('set to true should reveal non-schema columns', () => {
        const client = newInstance({keyspace: keyspace, protocolOptions: { noCompact: true }});
        return client.execute('select * from tbl_cs')
          .then(result => {
            assert.strictEqual(result.columns.length, 5);
            const row = result.first();
            assert.ok(row.column1, 'column1 should be present');
            assert.ok(row.value, 'value should be present');
            assert.strictEqual(row.column1, 'yak');
            assert.deepEqual(row.value, utils.allocBufferFromArray([0xca, 0xfe, 0xde, 0xad]));
            return client.shutdown();
          });
      });
      it('set to false should not reveal non-schema columns', () => {
        const client = newInstance({keyspace: keyspace, protocolOptions: { noCompact: false }});
        return client.execute('select * from tbl_cs')
          .then(result => {
            assert.strictEqual(result.columns.length, 3);
            const row = result.first();
            assert.ifError(row.column1, 'column1 should not be present');
            assert.ifError(row.value, 'value should not be present');
            return client.shutdown();
          });
      });
      it('unset should not reveal non-schema columns', () => {
        const client = newInstance({keyspace: keyspace});
        return client.execute('select * from tbl_cs')
          .then(result => {
            assert.strictEqual(result.columns.length, 3);
            const row = result.first();
            assert.ifError(row.column1, 'column1 should not be present');
            assert.ifError(row.value, 'value should not be present');
            return client.shutdown();
          });
      });
    });
    describe('with udt and tuple', function () {
      const sampleId = types.Uuid.random();
      const insertQuery = 'INSERT INTO tbl_udts (id, phone_col, address_col) VALUES (%s, %s, %s)';
      const selectQuery = 'SELECT id, phone_col, address_col FROM tbl_udts WHERE id = %s';
      before(function (done) {
        const client = setupInfo.client;
        utils.series([
          client.connect.bind(client),
          helper.toTask(client.execute, client, 'CREATE TYPE phone (alias text, number text, country_code int, other boolean)'),
          helper.toTask(client.execute, client, 'CREATE TYPE address (street text, "ZIP" int, phones set<frozen<phone>>)'),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_udts (id uuid PRIMARY KEY, phone_col frozen<phone>, address_col frozen<address>)'),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_tuples (id uuid PRIMARY KEY, tuple_col tuple<text,int,blob>)'),
          helper.toTask(client.execute, client, util.format(
            insertQuery,
            sampleId,
            "{alias: 'home', number: '555 1234', country_code: 54}",
            "{street: 'NightMan', \"ZIP\": 90988, phones: {{alias: 'personal', number: '555 5678'}, {alias: 'work'}}}"))
        ], done);
      });
      vit('2.1', 'should retrieve column information', function (done) {
        const client = setupInfo.client;
        client.execute(util.format(selectQuery, sampleId), function (err, result) {
          assert.ifError(err);
          assert.ok(result.columns);
          assert.strictEqual(result.columns.length, 3);
          assert.strictEqual(result.columns[1].type.code, types.dataTypes.udt);
          const phoneInfo = result.columns[1].type.info;
          assert.strictEqual(phoneInfo.name, 'phone');
          assert.strictEqual(phoneInfo.fields.length, 4);
          assert.strictEqual(phoneInfo.fields[0].name, 'alias');
          assert.strictEqual(phoneInfo.fields[0].type.code, types.dataTypes.varchar);
          assert.strictEqual(phoneInfo.fields[2].name, 'country_code');
          assert.strictEqual(phoneInfo.fields[2].type.code, types.dataTypes.int);
          assert.strictEqual(result.columns[2].type.code, types.dataTypes.udt);
          const addressInfo = result.columns[2].type.info;
          assert.strictEqual(addressInfo.name, 'address');
          assert.strictEqual(addressInfo.fields.length, 3);
          assert.strictEqual(addressInfo.fields[0].name, 'street');
          assert.strictEqual(addressInfo.fields[1].name, 'ZIP');
          assert.strictEqual(addressInfo.fields[1].type.code, types.dataTypes.int);
          assert.strictEqual(addressInfo.fields[2].name, 'phones');
          assert.strictEqual(addressInfo.fields[2].type.code, types.dataTypes.set);
          assert.strictEqual(addressInfo.fields[2].type.info.code, types.dataTypes.udt);
          const subPhone = addressInfo.fields[2].type.info.info;
          assert.strictEqual(subPhone.name, 'phone');
          assert.strictEqual(subPhone.fields.length, 4);
          assert.strictEqual(subPhone.fields[0].name, 'alias');
          assert.strictEqual(subPhone.fields[0].type.code, types.dataTypes.varchar);
          assert.strictEqual(subPhone.fields[1].name, 'number');
          assert.strictEqual(subPhone.fields[1].type.code, types.dataTypes.varchar);
          assert.strictEqual(subPhone.fields[2].name, 'country_code');
          assert.strictEqual(subPhone.fields[2].type.code, types.dataTypes.int);
          done();
        });
      });
      vit('2.1', 'should parse udt row', function (done) {
        const client = setupInfo.client;
        client.execute(util.format(selectQuery, sampleId), function (err, result) {
          assert.ifError(err);
          const row = result.first();
          assert.ok(row);
          const phone = row['phone_col'];
          assert.ok(phone);
          assert.strictEqual(phone['alias'], 'home');
          assert.strictEqual(phone['number'], '555 1234');
          assert.strictEqual(phone['country_code'], 54);
          assert.strictEqual(phone['other'], null);
          const address = row['address_col'];
          assert.ok(address);
          assert.strictEqual(address['street'], 'NightMan');
          assert.strictEqual(address['ZIP'], 90988);
          assert.ok(util.isArray(address['phones']));
          assert.strictEqual(address['phones'].length, 2);
          assert.strictEqual(address['phones'][0]['alias'], 'personal');
          assert.strictEqual(address['phones'][0]['number'], '555 5678');
          assert.strictEqual(address['phones'][1]['alias'], 'work');
          assert.strictEqual(address['phones'][1]['number'], null);
          done();
        });
      });
      vit('2.1', 'should allow udt parameter hints and retrieve metadata', function (done) {
        const phone = { alias: 'home2', number: '555 0000', country_code: 34, other: true};
        const address = {street: 'NightMan2', ZIP: 90987, phones: [{ 'alias': 'personal2', 'number': '555 0001'}, {alias: 'work2'}]};
        const id = types.Uuid.random();
        const client = setupInfo.client;
        utils.series([
          function insert(next) {
            const query = util.format(insertQuery, '?', '?', '?');
            client.execute(query, [id, phone, address], { hints: [null, 'udt<phone>', 'udt<address>']}, next);
          },
          function select(next) {
            client.execute(util.format(selectQuery, '?'), [id], function (err, result) {
              assert.ifError(err);
              const row = result.first();
              assert.ok(row);
              assert.ok(row['phone_col']);
              assert.strictEqual(row['phone_col']['alias'], phone.alias);
              assert.strictEqual(row['phone_col']['number'], phone.number);
              assert.strictEqual(row['phone_col']['country_code'], phone.country_code);
              assert.strictEqual(row['phone_col']['other'], phone.other);
              assert.ok(row['address_col']);
              assert.strictEqual(row['address_col']['street'], address.street);
              assert.strictEqual(row['address_col']['ZIP'], address.ZIP);
              assert.ok(row['address_col']['phones']);
              assert.strictEqual(row['address_col']['phones'].length, 2);
              assert.strictEqual(row['address_col']['phones'][0]['alias'], address.phones[0]['alias']);
              next();
            });
          }
        ], done);
      });
      vit('2.1', 'should allow tuple parameter hints', function (done) {
        const client = setupInfo.client;
        const id = types.Uuid.random();
        const tuple = new types.Tuple('Surf Rider', 110, utils.allocBufferFromString('0f0f', 'hex'));
        utils.series([
          function insert(next) {
            const query ='INSERT INTO tbl_tuples (id, tuple_col) VALUES (?, ?)';
            client.execute(query, [id, tuple], { hints: [null, 'tuple<text, int,blob>']}, next);
          },
          function select(next) {
            client.execute(util.format('SELECT * FROM tbl_tuples WHERE id = ?'), [id], function (err, result) {
              assert.ifError(err);
              const row = result.first();
              assert.ok(row);
              assert.ok(row['tuple_col']);
              assert.strictEqual(row['tuple_col'].length, 3);
              assert.strictEqual(row['tuple_col'].get(0), tuple.get(0));
              assert.strictEqual(row['tuple_col'].get(1), tuple.get(1));
              assert.strictEqual(row['tuple_col'].get(2).toString('hex'), '0f0f');
              next();
            });
          }
        ], done);
      });
      vit('2.2', 'should allow insertions as json', function (done) {
        const client = setupInfo.client;
        const o = {
          id: types.Uuid.random(),
          address_col: {
            street: 'whatever',
            phones: [
              { 'alias': 'main', 'number': '0000212123'}
            ]}
        };
        utils.series([
          function insert(next) {
            const query = 'INSERT INTO tbl_udts JSON ?';
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            const query = 'SELECT * FROM tbl_udts WHERE id = ?';
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              const row = result.first();
              assert.ok(row);
              assert.ok(row['address_col']);
              assert.strictEqual(row['address_col'].street, o.address_col.street);
              assert.strictEqual(row['address_col'].toString(), o.address_col.toString());
              next();
            });
          }
        ], done);
      });
    });
    describe('with named parameters', function () {
      vit('2.1', 'should allow named parameters', function (done) {
        const query = util.format('INSERT INTO %s (id, text_sample, bigint_sample) VALUES (:id, :myText, :myBigInt)', table);
        const values = { id: types.Uuid.random(), myText: 'hello', myBigInt: types.Long.fromNumber(2)};
        const client = setupInfo.client;
        client.execute(query, values, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'text_sample, bigint_sample', [values.myText, values.myBigInt], done);
        });
      });
      vit('2.1', 'should use parameter hints', function (done) {
        const query = util.format('INSERT INTO %s (id, int_sample, float_sample) VALUES (:id, :myInt, :myFloat)', table);
        const values = {id: types.Uuid.random(), myInt: 100, myFloat: 2.0999999046325684};
        const client = setupInfo.client;
        client.execute(query, values, { hints: {myFloat: 'float', myInt: {code: types.dataTypes.int}}}, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'int_sample, float_sample', [values.myInt, values.myFloat], done);
        });
      });
      vit('2.1', 'should allow parameters with different casings', function (done) {
        const query = util.format('INSERT INTO %s (id, text_sample, list_sample2) VALUES (:ID, :MyText, :mylist)', table);
        const values = { id: types.Uuid.random(), mytext: 'hello', myLIST: [ -1, 0, 500, 3]};
        const client = setupInfo.client;
        client.execute(query, values, { hints: { myLIST: 'list<int>'}}, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'text_sample, list_sample2', [values.mytext, values.myLIST], done);
        });
      });
    });
    describe('with smallint and tinyint', function () {
      const sampleId = types.Uuid.random();
      const insertQuery = 'INSERT INTO tbl_smallints (id, smallint_sample, tinyint_sample, text_sample) VALUES (%s, %s, %s, %s)';
      const selectQuery = 'SELECT id, smallint_sample, tinyint_sample, text_sample FROM tbl_smallints WHERE id = %s';
      before(function (done) {
        const client = setupInfo.client;
        utils.series([
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_smallints (id uuid PRIMARY KEY, smallint_sample smallint, tinyint_sample tinyint, text_sample text)'),
          helper.toTask(client.execute, client, util.format(
            insertQuery, sampleId, 0x0200, 2, "'two'"))
        ], done);
      });
      vit('2.2', 'should retrieve smallint and tinyint values as Number', function (done) {
        const query = util.format(selectQuery, sampleId);
        const client = setupInfo.client;
        client.execute(query, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.rowLength);
          const row = result.first();
          assert.ok(row);
          assert.strictEqual(row['text_sample'], 'two');
          assert.strictEqual(row['smallint_sample'], 0x0200);
          assert.strictEqual(row['tinyint_sample'], 2);
          done();
        });
      });
      vit('2.2', 'should encode and decode smallint and tinyint values as Number', function (done) {
        const client = setupInfo.client;
        const query = util.format(insertQuery, '?', '?', '?', '?');
        const id = types.Uuid.random();
        client.execute(query, [id, 10, 11, 'another text'], { hints: [null, 'smallint', 'tinyint']}, function (err) {
          assert.ifError(err);
          client.execute(util.format(selectQuery, id), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rowLength);
            const row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'another text');
            assert.strictEqual(row['smallint_sample'], 10);
            assert.strictEqual(row['tinyint_sample'], 11);
            done();
          });
        });
      });
    });
    describe('with date and time types', function () {
      const LocalDate = types.LocalDate;
      const LocalTime = types.LocalTime;
      const insertQuery = 'INSERT INTO tbl_datetimes (id, date_sample, time_sample) VALUES (?, ?, ?)';
      const selectQuery = 'SELECT id, date_sample, time_sample FROM tbl_datetimes WHERE id = ?';
      before(function (done) {
        const query = 'CREATE TABLE tbl_datetimes ' +
          '(id uuid PRIMARY KEY, date_sample date, time_sample time, text_sample text)';
        setupInfo.client.execute(query, done);
      });
      vit('2.2', 'should encode and decode date and time values as LocalDate and LocalTime', function (done) {
        const values = [
          [types.Uuid.random(), new LocalDate(1969, 10, 13), new LocalTime(types.Long.fromString('0'))],
          [types.Uuid.random(), new LocalDate(2010, 4, 29), LocalTime.fromString('15:01:02.1234')],
          [types.Uuid.random(), new LocalDate(2005, 8, 5), LocalTime.fromString('01:56:03.000501')],
          [types.Uuid.random(), new LocalDate(1983, 2, 24), new LocalTime(types.Long.fromString('86399999999999'))],
          [types.Uuid.random(), new LocalDate(-2147483648), new LocalTime(types.Long.fromString('6311999549933'))]
        ];
        const client = setupInfo.client;
        utils.eachSeries(values, function (params, next) {
          client.execute(insertQuery, params, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [params[0]], function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.ok(result.rowLength);
              const row = result.first();
              assert.ok(row);
              assert.strictEqual(row['id'].toString(), params[0].toString());
              helper.assertInstanceOf(row['date_sample'], LocalDate);
              assert.strictEqual(row['date_sample'].toString(), params[1].toString());
              helper.assertInstanceOf(row['time_sample'], LocalTime);
              assert.strictEqual(row['time_sample'].toString(), params[2].toString());
              next();
            });
          });
        }, done);
      });
    });
    describe('with json support', function () {
      before(function (done) {
        const client = setupInfo.client;
        const query =
          'CREATE TABLE tbl_json (' +
          '  id uuid PRIMARY KEY,' +
          '  tid timeuuid,' +
          '  dec decimal,' +
          '  vi varint,' +
          '  bi bigint,' +
          '  ip inet,' +
          '  tup frozen<tuple<int, int>>,' +
          '  d date,' +
          '  t time)';
        client.execute(query, done);
      });
      vit('2.2', 'should allow insert of all ECMAScript types as json', function (done) {
        const client = setupInfo.client;
        const o = {
          id: types.Uuid.random(),
          text_sample: 'hello json',
          int_sample: 100,
          float_sample: 1.2000000476837158,
          double_sample: 1/3,
          boolean_sample: true,
          timestamp_sample: new Date(1432889533534),
          map_sample: { a: 'one', z: 'two'},
          list_sample: ['b', 'a', 'b', 'a', 's', 'o', 'n', 'i', 'c', 'o', 's'],
          list_sample2: [100, 100, 1, 2],
          set_sample: ['a', 'b', 'x', 'zzzz']
        };
        utils.series([
          function insert(next) {
            const query = util.format('INSERT INTO %s JSON ?', table);
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            const query = util.format('SELECT * FROM %s WHERE id = ?', table);
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              const row = result.first();
              assert.ok(row);
              assert.strictEqual(row['text_sample'], o.text_sample);
              assert.strictEqual(row['int_sample'], o.int_sample);
              assert.strictEqual(row['float_sample'], o.float_sample);
              assert.strictEqual(row['double_sample'], o.double_sample);
              assert.strictEqual(row['boolean_sample'], o.boolean_sample);
              assert.strictEqual(row['timestamp_sample'].getTime(), o.timestamp_sample.getTime());
              assert.ok(row['map_sample']);
              assert.strictEqual(row['map_sample'].a, o.map_sample.a);
              assert.strictEqual(row['map_sample'].z, o.map_sample.z);
              assert.strictEqual(row['list_sample'].toString(), o.list_sample.toString());
              assert.strictEqual(row['list_sample2'].toString(), o.list_sample2.toString());
              assert.strictEqual(row['set_sample'].toString(), o.set_sample.toString());
              next();
            });
          }
        ], done);
      });
      vit('2.2', 'should allow insert of all non - ECMAScript types as json', function (done) {
        const client = setupInfo.client;
        const o = {
          id:   types.Uuid.random(),
          tid:  types.TimeUuid.now(),
          dec:  new types.BigDecimal(113, 2),
          vi:   types.Integer.fromString('903234243231132008846'),
          bi:   types.Long.fromString('2305843009213694123'),
          ip:   types.InetAddress.fromString('12.10.126.11'),
          tup:  new types.Tuple(1, 300),
          d:    new types.LocalDate(2015, 6, 1),
          t:    new types.LocalTime.fromMilliseconds(10160088, 123)
        };
        utils.series([
          function insert(next) {
            const query = 'INSERT INTO tbl_json JSON ?';
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            const query = 'SELECT * FROM tbl_json WHERE id = ?';
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              const row = result.first();
              assert.ok(row);
              assert.strictEqual(row['tid'].toString(), o.tid.toString());
              assert.strictEqual(row['dec'].toString(), o.dec.toString());
              assert.strictEqual(row['vi'].toString(), o.vi.toString());
              assert.strictEqual(row['bi'].toString(), o.bi.toString());
              assert.strictEqual(row['ip'].toString(), o.ip.toString());
              assert.strictEqual(row['tup'].toString(), o.tup.toString());
              assert.strictEqual(row['tup'].get(0), o.tup.get(0));
              assert.strictEqual(row['tup'].get(1), o.tup.get(1));
              assert.strictEqual(row['d'].toString(), o.d.toString());
              assert.strictEqual(row['t'].toString(), o.t.toString());
              next();
            });
          }
        ], done);
      });
    });
    describe('with no callback specified', function () {
      vit('2.0', 'should return a promise with the result as a value', function () {
        const client = newInstance();
        return client.connect()
          .then(function () {
            // Only the query
            return client.execute(helper.queries.basic);
          })
          .then(function (result) {
            // With parameters
            helper.assertInstanceOf(result, types.ResultSet);
            assert.strictEqual(result.rowLength, 1);
            return client.execute('select key from system.local WHERE key = ?', [ 'local' ]);
          })
          .then(function (result) {
            // With parameters and options
            helper.assertInstanceOf(result, types.ResultSet);
            const options = { consistency: types.consistencies.localOne };
            return client.execute('select key from system.local WHERE key = ?', [ 'local' ], options);
          })
          .then(function () {
            return client.shutdown();
          });
      });
      it('should reject the promise when there is a syntax error', function () {
        const client = setupInfo.client;
        return client.connect()
          .then(function () {
            return client.execute('SELECT INVALID QUERY');
          })
          .then(function () {
            throw new Error('should have been rejected');
          })
          .catch(function (err) {
            helper.assertInstanceOf(err, errors.ResponseError);
          });
      });
    });
    vdescribe('2.0', 'with lightweight transactions', function () {
      const client = setupInfo.client;
      const id = types.Uuid.random();
      before(function (done) {
        const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
        client.execute(query, [id, 'val' ], done);
      });
      [
        [ 'is not a conditional update', true, 'INSERT INTO %s (id, text_sample) VALUES (?, ?)', [ id, 'val'] ],
        [ 'is a conditional update and it was applied', true,
          'INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', [ types.Uuid.random(), 'val2'] ],
        [ 'is a conditional update and it was not applied', false,
          'INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', [ id, 'val'] ]
      ].forEach(function (item) {
        context('when it ' + item[0], function () {
          it('should return a ResultSet with wasApplied set to ' + item[1], function (done) {
            client.execute(util.format(item[2], table), item[3], function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.wasApplied(), item[1]);
              done();
            });
          });
        });
      });
    });
  });
});

function insertSelectTest(client, table, columns, values, hints, done) {
  const columnsSplit = columns.split(',');
  utils.series([
    function (next) {
      let markers = '?';
      for (let i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      const query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 0, hints: hints}, next);
    },
    function (next) {
      const query = util.format('SELECT %s FROM %s WHERE id = %s', columns, table, values[0]);
      client.execute(query, null, {prepare: 0}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        const row = result.rows[0];
        assert.strictEqual(row.values().length, values.length);
        assert.strictEqual(row.keys().join(', '), columnsSplit.join(','));
        for (let i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}

function verifyRow(table, id, fields, values, callback) {
  const client = newInstance();
  client.execute(util.format('SELECT %s FROM %s WHERE id = %s', fields, table, id), function (err, result) {
    assert.ifError(err);
    const row = result.first();
    assert.ok(row, 'It should contain a row');
    helper.assertValueEqual(row.values(), values);
    callback();
  });
}

/**
 * @param [options]
 * @returns {Client}
 */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}
