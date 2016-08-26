"use strict";
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var ExecutionProfile = require('../../../lib/execution-profile.js').ExecutionProfile;
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');
var vit = helper.vit;

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 0}, callback)', function () {
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    var selectAllQuery = 'SELECT * FROM ' + table;
    before(function (done) {
      var client = newInstance();
      utils.series([
        helper.ccmHelper.start(1),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table), next);
        }
      ], done);
    });
    after(helper.ccmHelper.remove);
    it('should execute a basic query', function (done) {
      var client = newInstance();
      client.execute(helper.queries.basic, function (err, result) {
        assert.equal(err, null);
        assert.notEqual(result, null);
        assert.notEqual(result.rows, null);
        done();
      });
    });
    it('should callback with syntax error', function (done) {
      var client = newInstance();
      client.connect(function (err) {
        assert.ifError(err);
        var query = 'SELECT WILL FAIL';
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
      var client = newInstance();
      client.execute(helper.queries.basicNoResults, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(util.isArray(result.rows));
        helper.assertInstanceOf(result, types.ResultSet);
        assert.strictEqual(result.rows.length, 0);
        done();
      });
    });
    it('should handle 500 parallel queries', function (done) {
      var client = newInstance();
      utils.times(500, function (n, next) {
        client.execute(helper.queries.basic, [], next);
      }, done)
    });
    it('should fail if non-existent profile provided', function (done) {
      var client = newInstance();
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
      var client = newInstance();
      var columns = 'id, timeuuid_sample, text_sample, double_sample, timestamp_sample, blob_sample, list_sample';
      //a precision a float32 can represent
      var values = [types.Uuid.random(), types.TimeUuid.now(), 'text sample 1', 133, new Date(121212211), new Buffer(100), ['one', 'two']];
      //no hint
      insertSelectTest(client, table, columns, values, null, done);
    });
    vit('2.0', 'should use parameter hints as number for simple types', function (done) {
      var client = newInstance();
      var columns = 'id, text_sample, float_sample, int_sample';
      //a precision a float32 can represent
      var values = [types.Uuid.random(), 'text sample', 1000.0999755859375, -12];
      var hints = [types.dataTypes.uuid, types.dataTypes.text, types.dataTypes.float, types.dataTypes.int];
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for simple types', function (done) {
      var columns = 'id, text_sample, float_sample, int_sample';
      var values = [types.Uuid.random(), 'text sample', -9, 1];
      var hints = [null, 'text', 'float', 'int'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for complex types partial', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.Uuid.random(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      var hints = [null, 'map', 'list', 'set'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints as string for complex types complete', function (done) {
      var columns = 'id, map_sample, list_sample, set_sample';
      var values = [types.Uuid.random(), {val1: 'text sample1'}, ['list_text1'], ['set_text1']];
      //complete info
      var hints = [null, 'map<text, text>', 'list<text>', 'set<text>'];
      var client = newInstance();
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use parameter hints for custom map polyfills', function (done) {
      var columns = 'id, map_sample';
      var map = new helper.Map();
      map.set('k1', 'value 1');
      map.set('k2', 'value 2');
      var values = [types.Uuid.random(), map];
      //complete info
      var hints = [null, 'map<text, text>'];
      var client = newInstance({encoding: { map: helper.Map }});
      insertSelectTest(client, table, columns, values, hints, done);
    });
    vit('2.0', 'should use pageState and fetchSize', function (done) {
      var client = newInstance();
      var pageState = null;
      utils.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
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
      var client = newInstance({keyspace: keyspace});
      var pageState = null;
      utils.series([
        function truncate(seriesNext) {
          client.execute('TRUNCATE ' + table, seriesNext);
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.execute(query, [types.Uuid.random(), n.toString()], next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //It should only return the first page
          client.execute(util.format('SELECT * FROM %s', table), [], {fetchSize: 65, autoPage: true}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 65);
            pageState = result.meta.pageState;
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.0', 'should callback in err when wrong hints are provided', function (done) {
      var client = newInstance();
      var query = util.format('SELECT * FROM %s WHERE id IN (?, ?, ?)', table);
      //valid params
      var params = [types.Uuid.random(), types.Uuid.random(), types.Uuid.random()];
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
      var client = newInstance();
      client.execute(util.format('CREATE INDEX list_sample_index ON %s(list_sample)', table), function (err) {
        assert.ifError(err);
        // Allow 1 second for index to build (otherwise an IndexNotAvailableException may be raised while index is building).
        setTimeout(function() {
          var query = util.format('SELECT * FROM %s WHERE list_sample CONTAINS ? AND list_sample CONTAINS ? ALLOW FILTERING', table);
          //valid params
          var params = ['val1', 'val2'];
          client.execute(query, params, function (err) {
            //it should not fail
            assert.ifError(err);
            done();
          });
        }, 1000);
      });
    });
    it('should accept localOne and localQuorum consistencies', function (done) {
      var client = newInstance();
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
      var client = newInstance({profiles: [new ExecutionProfile('cl', {consistency: types.consistencies.quorum})]});
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
        }
      ], done);
    });
    vit('2.2', 'should accept unset as a valid value', function (done) {
      var client = newInstance();
      var id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function insert(next) {
          var query = util.format('INSERT INTO %s (id, text_sample, double_sample) VALUES (?, ?, ?)', table);
          client.execute(query, [id, 'sample unset', types.unset], next);
        },
        function select(next) {
          var query = util.format('SELECT id, text_sample, double_sample FROM %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 1);
            var row = result.first();
            assert.strictEqual(row['text_sample'], 'sample unset');
            assert.strictEqual(row['double_sample'], null);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should handle several concurrent executes while the pool is not ready', function (done) {
      var client = newInstance({pooling: {
        coreConnectionsPerHost: {
          //lots of connections per host
          '0': 100,
          '1': 1,
          '2': 0
        }}});
      var execute = function (next) {
        client.execute(selectAllQuery, next);
      };
      utils.parallel([
        function (parallelNext) {
          utils.parallel(helper.fillArray(400, execute), parallelNext);
        },
        function (parallelNext) {
          utils.times(200, function (n, next) {
            setTimeout(function () {
              execute(next);
            }, n * 5 + 50);
          }, parallelNext);
        }
      ], done);
    });
    it('should return the column definitions', function (done) {
      var client = newInstance();
      //insert at least 1 row
      var insertQuery = util.format('INSERT INTO %s (id) VALUES (%s)', table, types.Uuid.random());
      utils.series([
        client.connect.bind(client),
        helper.toTask(client.execute, client, insertQuery),
        function verifyColumns(next) {
          var query = util.format('SELECT text_sample, timestamp_sample, int_sample, timeuuid_sample, list_sample2, map_sample from %s LIMIT 1', table);
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
          var query = util.format('SELECT * from %s WHERE id = 00000000-0000-0000-0000-000000000000', table);
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
      var client = newInstance();
      var id = types.Uuid.random();
      var timeId = types.TimeUuid.now();
      utils.series([
        function insert(next) {
          var query = util.format(
            'INSERT INTO %s (id, timeuuid_sample, inet_sample, bigint_sample, decimal_sample) VALUES (%s, %s, \'%s\', %s, %s)',
            table, id, timeId, '::2233:0:0:bb', -100, "0.1");
          client.execute(query, next);
        },
        function select(next) {
          var query = util.format(
            'SELECT id, timeuuid_sample, inet_sample, bigint_sample, decimal_sample from %s WHERE id = %s', table, id);
          client.execute(query, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 1);
            var row = result.rows[0];
            var expected = util.format('{"id":"%s",' +
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
      var client = newInstance({profiles: [new ExecutionProfile('cl', {serialConsistency: types.consistencies.one})]});
      var id = types.Uuid.random();
      utils.series([
        function insertWithProfileSerialCL(next) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', table);
          client.execute(query, [id, 'hello serial'], { executionProfile: 'cl'}, function(err) {
            // expect an error as we used an invalid serial CL.
            assert.ok(err);
            assert.strictEqual(err.code, 0x2200); // should be an invalid query.
            next();
          });
        },
        function insertWithQueryCL(next) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?) IF NOT EXISTS', table);
          client.execute(query, [id, 'hello serial'], { executionProfile: 'cl', serialConsistency: types.consistencies.localSerial}, next);
        },
        function select(next) {
          var query = util.format('SELECT id, text_sample from %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            var row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'hello serial');
            next();
          });
        }
      ], done);
    });
    vit('2.1', 'should support protocol level timestamp', function (done) {
      var client = newInstance();
      var id = types.Uuid.random();
      var timestamp = types.generateTimestamp(new Date(), 777);
      utils.series([
        function insert(next) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          client.execute(query, [id, 'hello timestamp'], { timestamp: timestamp}, next);
        },
        function select(next) {
          var query = util.format('SELECT id, text_sample, writetime(text_sample) from %s WHERE id = ?', table);
          client.execute(query, [id], function (err, result) {
            var row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'hello timestamp');
            assert.strictEqual(row['writetime(text_sample)'].toString(), timestamp.toString());
            next();
          });
        }
      ], done);
    });
    it('should retrieve the trace id when queryTrace flag is set', function (done) {
      var client = newInstance();
      var id = types.Uuid.random();
      utils.series([
        client.connect.bind(client),
        function selectNotExistent(next) {
          var query = util.format('SELECT * FROM %s WHERE id = %s', table, types.Uuid.random());
          client.execute(query, [], { traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function insertQuery(next) {
          var query = util.format('INSERT INTO %s (id) VALUES (%s)', table, id.toString());
          client.execute(query, [], { traceQuery: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            helper.assertInstanceOf(result.info.traceId, types.Uuid);
            next();
          });
        },
        function selectSingleRow(next) {
          var query = util.format('SELECT * FROM %s WHERE id = %s', table, id.toString());
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
      var client = newInstance();
      client.execute('SELECT * FROM system.local', function (err, result) {
        assert.ifError(err);
        assert.ok(result.info);
        assert.equal(result.info.traceId, null); //its undefined really but anything that evaluates as null is OK
        done();
      });
    });
    vit('2.2', 'should include the warning in the ResultSet', function (done) {
      var client = newInstance();
      var loggedMessage = false;
      client.on('log', function (level, className, message) {
        if (loggedMessage) return;
        if (level !== 'warning') return;
        message = message.toLowerCase();
        if (message.indexOf('batch') >= 0 && message.indexOf('exceeding')) {
          loggedMessage = true;
        }
      });
      var query = util.format(
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
        client.shutdown(done);
      });
    });
    describe('with udt and tuple', function () {
      var sampleId = types.Uuid.random();
      var insertQuery = 'INSERT INTO tbl_udts (id, phone_col, address_col) VALUES (%s, %s, %s)';
      var selectQuery = 'SELECT id, phone_col, address_col FROM tbl_udts WHERE id = %s';
      before(function (done) {
        var client = newInstance({ keyspace: keyspace });
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
        var client = newInstance({ keyspace: keyspace });
        client.execute(util.format(selectQuery, sampleId), function (err, result) {
          assert.ifError(err);
          assert.ok(result.columns);
          assert.strictEqual(result.columns.length, 3);
          assert.strictEqual(result.columns[1].type.code, types.dataTypes.udt);
          var phoneInfo = result.columns[1].type.info;
          assert.strictEqual(phoneInfo.name, 'phone');
          assert.strictEqual(phoneInfo.fields.length, 4);
          assert.strictEqual(phoneInfo.fields[0].name, 'alias');
          assert.strictEqual(phoneInfo.fields[0].type.code, types.dataTypes.varchar);
          assert.strictEqual(phoneInfo.fields[2].name, 'country_code');
          assert.strictEqual(phoneInfo.fields[2].type.code, types.dataTypes.int);
          assert.strictEqual(result.columns[2].type.code, types.dataTypes.udt);
          var addressInfo = result.columns[2].type.info;
          assert.strictEqual(addressInfo.name, 'address');
          assert.strictEqual(addressInfo.fields.length, 3);
          assert.strictEqual(addressInfo.fields[0].name, 'street');
          assert.strictEqual(addressInfo.fields[1].name, 'ZIP');
          assert.strictEqual(addressInfo.fields[1].type.code, types.dataTypes.int);
          assert.strictEqual(addressInfo.fields[2].name, 'phones');
          assert.strictEqual(addressInfo.fields[2].type.code, types.dataTypes.set);
          assert.strictEqual(addressInfo.fields[2].type.info.code, types.dataTypes.udt);
          var subPhone = addressInfo.fields[2].type.info.info;
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
        var client = newInstance({ keyspace: keyspace });
        client.execute(util.format(selectQuery, sampleId), function (err, result) {
          assert.ifError(err);
          var row = result.first();
          assert.ok(row);
          var phone = row['phone_col'];
          assert.ok(phone);
          assert.strictEqual(phone['alias'], 'home');
          assert.strictEqual(phone['number'], '555 1234');
          assert.strictEqual(phone['country_code'], 54);
          assert.strictEqual(phone['other'], null);
          var address = row['address_col'];
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
        var phone = { alias: 'home2', number: '555 0000', country_code: 34, other: true};
        var address = {street: 'NightMan2', ZIP: 90987, phones: [{ 'alias': 'personal2', 'number': '555 0001'}, {alias: 'work2'}]};
        var id = types.Uuid.random();
        var client = newInstance({ keyspace: keyspace});
        utils.series([
          function insert(next) {
            var query = util.format(insertQuery, '?', '?', '?');
            client.execute(query, [id, phone, address], { hints: [null, 'udt<phone>', 'udt<address>']}, next);
          },
          function select(next) {
            client.execute(util.format(selectQuery, '?'), [id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
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
        var client = newInstance({ keyspace: keyspace});
        var id = types.Uuid.random();
        var tuple = new types.Tuple('Surf Rider', 110, new Buffer('0f0f', 'hex'));
        utils.series([
          function insert(next) {
            var query ='INSERT INTO tbl_tuples (id, tuple_col) VALUES (?, ?)';
            client.execute(query, [id, tuple], { hints: [null, 'tuple<text, int,blob>']}, next);
          },
          function select(next) {
            client.execute(util.format('SELECT * FROM tbl_tuples WHERE id = ?'), [id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
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
        var client = newInstance({ keyspace: keyspace });
        var o = {
          id: types.Uuid.random(),
          address_col: {
            street: 'whatever',
            phones: [
              { 'alias': 'main', 'number': '0000212123'}
            ]}
        };
        utils.series([
          client.connect.bind(client),
          function insert(next) {
            var query = 'INSERT INTO tbl_udts JSON ?';
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            var query = 'SELECT * FROM tbl_udts WHERE id = ?';
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
              assert.ok(row);
              assert.ok(row['address_col']);
              assert.strictEqual(row['address_col'].street, o.address_col.street);
              assert.strictEqual(row['address_col'].toString(), o.address_col.toString());
              next();
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
    });
    describe('with named parameters', function () {
      vit('2.1', 'should allow named parameters', function (done) {
        var query = util.format('INSERT INTO %s (id, text_sample, bigint_sample) VALUES (:id, :myText, :myBigInt)', table);
        var values = { id: types.Uuid.random(), myText: 'hello', myBigInt: types.Long.fromNumber(2)};
        var client = newInstance();
        client.execute(query, values, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'text_sample, bigint_sample', [values.myText, values.myBigInt], done);
        });
      });
      vit('2.1', 'should use parameter hints', function (done) {
        var query = util.format('INSERT INTO %s (id, int_sample, float_sample) VALUES (:id, :myInt, :myFloat)', table);
        var values = {id: types.Uuid.random(), myInt: 100, myFloat: 2.0999999046325684};
        var client = newInstance();
        client.execute(query, values, { hints: {myFloat: 'float', myInt: {code: types.dataTypes.int}}}, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'int_sample, float_sample', [values.myInt, values.myFloat], done);
        });
      });
      vit('2.1', 'should allow parameters with different casings', function (done) {
        var query = util.format('INSERT INTO %s (id, text_sample, list_sample2) VALUES (:ID, :MyText, :mylist)', table);
        var values = { id: types.Uuid.random(), mytext: 'hello', myLIST: [ -1, 0, 500, 3]};
        var client = newInstance();
        client.execute(query, values, { hints: { myLIST: 'list<int>'}}, function (err) {
          assert.ifError(err);
          verifyRow(table, values.id, 'text_sample, list_sample2', [values.mytext, values.myLIST], done);
        });
      });
    });
    describe('with smallint and tinyint', function () {
      var sampleId = types.Uuid.random();
      var insertQuery = 'INSERT INTO tbl_smallints (id, smallint_sample, tinyint_sample, text_sample) VALUES (%s, %s, %s, %s)';
      var selectQuery = 'SELECT id, smallint_sample, tinyint_sample, text_sample FROM tbl_smallints WHERE id = %s';
      before(function (done) {
        var client = newInstance({ keyspace: keyspace });
        utils.series([
          client.connect.bind(client),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_smallints (id uuid PRIMARY KEY, smallint_sample smallint, tinyint_sample tinyint, text_sample text)'),
          helper.toTask(client.execute, client, util.format(
            insertQuery, sampleId, 0x0200, 2, "'two'"))
        ], done);
      });
      vit('2.2', 'should retrieve smallint and tinyint values as Number', function (done) {
        var query = util.format(selectQuery, sampleId);
        var client = newInstance({ keyspace: keyspace });
        client.execute(query, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.rowLength);
          var row = result.first();
          assert.ok(row);
          assert.strictEqual(row['text_sample'], 'two');
          assert.strictEqual(row['smallint_sample'], 0x0200);
          assert.strictEqual(row['tinyint_sample'], 2);
          done();
        });
      });
      vit('2.2', 'should encode and decode smallint and tinyint values as Number', function (done) {
        var client = newInstance({ keyspace: keyspace });
        var query = util.format(insertQuery, '?', '?', '?', '?');
        var id = types.Uuid.random();
        client.execute(query, [id, 10, 11, 'another text'], { hints: [null, 'smallint', 'tinyint']}, function (err) {
          assert.ifError(err);
          client.execute(util.format(selectQuery, id), function (err, result) {
            assert.ifError(err);
            assert.ok(result);
            assert.ok(result.rowLength);
            var row = result.first();
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
      var LocalDate = types.LocalDate;
      var LocalTime = types.LocalTime;
      var insertQuery = 'INSERT INTO tbl_datetimes (id, date_sample, time_sample) VALUES (?, ?, ?)';
      var selectQuery = 'SELECT id, date_sample, time_sample FROM tbl_datetimes WHERE id = ?';
      before(function (done) {
        var client = newInstance({ keyspace: keyspace });
        utils.series([
          client.connect.bind(client),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_datetimes (id uuid PRIMARY KEY, date_sample date, time_sample time, text_sample text)'),
          client.shutdown.bind(client)
        ], done);
      });
      vit('2.2', 'should encode and decode date and time values as LocalDate and LocalTime', function (done) {
        var values = [
          [types.Uuid.random(), new LocalDate(1969, 10, 13), new LocalTime(types.Long.fromString('0'))],
          [types.Uuid.random(), new LocalDate(2010, 4, 29), LocalTime.fromString('15:01:02.1234')],
          [types.Uuid.random(), new LocalDate(2005, 8, 5), LocalTime.fromString('01:56:03.000501')],
          [types.Uuid.random(), new LocalDate(1983, 2, 24), new LocalTime(types.Long.fromString('86399999999999'))],
          [types.Uuid.random(), new LocalDate(-2147483648), new LocalTime(types.Long.fromString('6311999549933'))]
        ];
        var client = newInstance({ keyspace: keyspace });
        utils.eachSeries(values, function (params, next) {
          client.execute(insertQuery, params, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [params[0]], function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.ok(result.rowLength);
              var row = result.first();
              assert.ok(row);
              assert.strictEqual(row['id'].toString(), params[0].toString());
              helper.assertInstanceOf(row['date_sample'], LocalDate);
              assert.strictEqual(row['date_sample'].toString(), params[1].toString());
              helper.assertInstanceOf(row['time_sample'], LocalTime);
              assert.strictEqual(row['time_sample'].toString(), params[2].toString());
              next();
            });
          });
        }, helper.finish(client, done));
      });
    });
    describe('with json support', function () {
      before(function (done) {
        var client = newInstance({ keyspace: keyspace });
        var query =
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
        client.execute(query, function (err) {
          assert.ifError(err);
          client.shutdown(done);
        });
      });
      vit('2.2', 'should allow insert of all ECMAScript types as json', function (done) {
        var client = newInstance();
        var o = {
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
          client.connect.bind(client),
          function insert(next) {
            var query = util.format('INSERT INTO %s JSON ?', table);
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            var query = util.format('SELECT * FROM %s WHERE id = ?', table);
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
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
          },
          client.shutdown.bind(client)
        ], done);
      });
      vit('2.2', 'should allow insert of all non - ECMAScript types as json', function (done) {
        var client = newInstance({ keyspace: keyspace });
        var o = {
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
          client.connect.bind(client),
          function insert(next) {
            var query = 'INSERT INTO tbl_json JSON ?';
            client.execute(query, [JSON.stringify(o)], next);
          },
          function select(next) {
            var query = 'SELECT * FROM tbl_json WHERE id = ?';
            client.execute(query, [o.id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
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
          },
          client.shutdown.bind(client)
        ], done);
      });
    });
  });
});

function insertSelectTest(client, table, columns, values, hints, done) {
  var columnsSplit = columns.split(',');
  utils.series([
    function (next) {
      var markers = '?';
      for (var i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      var query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 0, hints: hints}, next);
    },
    function (next) {
      var query = util.format('SELECT %s FROM %s WHERE id = %s', columns, table, values[0]);
      client.execute(query, null, {prepare: 0}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        var row = result.rows[0];
        assert.strictEqual(row.values().length, values.length);
        assert.strictEqual(row.keys().join(', '), columnsSplit.join(','));
        for (var i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}

function verifyRow(table, id, fields, values, callback) {
  var client = newInstance();
  client.execute(util.format('SELECT %s FROM %s WHERE id = %s', fields, table, id), function (err, result) {
    assert.ifError(err);
    var row = result.first();
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
