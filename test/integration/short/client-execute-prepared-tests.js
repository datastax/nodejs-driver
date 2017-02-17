'use strict';
var assert = require('assert');
var util = require('util');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils');
var errors = require('../../../lib/errors');
var loadBalancing = require('../../../lib/policies/load-balancing');
var vit = helper.vit;
var vdescribe = helper.vdescribe;
var Uuid = types.Uuid;

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 1}, callback)', function () {
    var commonKs = helper.getRandomName('ks');
    var commonTable = commonKs + '.' + helper.getRandomName('table');
    var setupInfo = helper.setup(3, {
      keyspace: commonKs,
      queries: [ helper.createTableWithClusteringKeyCql(commonTable) ]
    });
    it('should execute a prepared query with parameters on all hosts', function (done) {
      var client = setupInfo.client;
      var query = util.format('SELECT * FROM %s WHERE id1 = ?', commonTable);
      utils.timesSeries(3, function (n, next) {
        client.execute(query, [types.Uuid.random()], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 3);
          assert.notEqual(result, null);
          assert.notEqual(result.rows, null);
          next();
        });
      }, done);
    });
    it('should callback with error when query is invalid', function (done) {
      var client = setupInfo.client;
      var query = 'SELECT WILL FAIL';
      client.execute(query, ['system'], {prepare: 1}, function (err) {
        assert.ok(err);
        assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
        assert.strictEqual(err.query, query);
        done();
      });
    });
    it('should prepare and execute a query without parameters', function (done) {
      var client = setupInfo.client;
      client.execute(helper.queries.basic, null, {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.strictEqual(typeof result.rows.length, 'number');
        done();
      });
    });
    it('should prepare and execute a queries in parallel', function (done) {
      var client = setupInfo.client;
      var queries = [
        helper.queries.basic,
        helper.queries.basicNoResults,
        util.format('SELECT * FROM %s WHERE id1 = ?', commonTable),
        util.format('SELECT * FROM %s WHERE id1 IN (?, ?)', commonTable)
      ];
      var params = [
        null,
        null,
        [types.Uuid.random()],
        [types.Uuid.random(), types.Uuid.random()]
      ];
      utils.times(100, function (n, next) {
        var index = n % 4;
        client.execute(queries[index], params[index], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.strictEqual(typeof result.rows.length, 'number');
          next();
        });
      }, done);
    });
    it('should fail following times if it fails to prepare', function (done) {
      var client = setupInfo.client;
      utils.series([function (seriesNext) {
        //parallel
        utils.times(10, function (n, next) {
          client.execute('SELECT * FROM system.table1', ['val'], {prepare: 1}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(err.code, types.responseErrorCodes.invalid);
            next();
          });
        }, seriesNext);
      }, function (seriesNext) {
        utils.timesSeries(10, function (n, next) {
          client.execute('SELECT * FROM system.table2', ['val'], {prepare: 1}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(err.code, types.responseErrorCodes.invalid);
            next();
          });
        }, seriesNext);
      }], done);
    });
    it('should fail if the type does not match', function (done) {
      var client = setupInfo.client;
      client.execute(util.format('SELECT * FROM %s WHERE id1 = ?', commonTable), [1000], {prepare: 1}, function (err) {
        helper.assertInstanceOf(err, Error);
        helper.assertInstanceOf(err, TypeError);
        done();
      });
    });
    it('should serialize all guessed types', function (done) {
      var values = [types.Uuid.random(), 'as', '111', null, new types.Long(0x1001, 0x0109AA), 1, new Buffer([1, 240]),
        true, new Date(1221111111), types.InetAddress.fromString('10.12.0.1'), null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, ' +
        'boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';
      serializationTest(setupInfo.client, values, columnNames, done);
    });
    it('should serialize all null values', function (done) {
      var values = [types.Uuid.random(), null, null, null, null, null, null, null, null, null, null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';
      serializationTest(setupInfo.client, values, columnNames, done);
    });
    it('should use prepared metadata to determine the type of params in query', function (done) {
      var values = [types.Uuid.random(), types.TimeUuid.now(), [1, 1000, 0], {k: '1'}, 1, -100019, ['arr'], types.InetAddress.fromString('192.168.1.1')];
      var columnNames = 'id, timeuuid_sample, list_sample2, map_sample, int_sample, float_sample, set_sample, inet_sample';
      serializationTest(setupInfo.client, values, columnNames, done);
    });
    vit('2.0', 'should support IN clause with 1 marker', function (done) {
      var query = util.format('SELECT * FROM %s WHERE id1 IN ?', commonTable);
      setupInfo.client.execute(query, [ [ Uuid.random(), Uuid.random() ] ], { prepare: true }, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.strictEqual(typeof result.rows.length, 'number');
        done();
      });
    });
    vit('2.0', 'should use pageState and fetchSize', function (done) {
      var client = newInstance({
        keyspace: setupInfo.keyspace,
        queryOptions: { consistency: types.consistencies.quorum }
      });
      var pageState;
      var metaPageState;
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      utils.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, helper.createTableCql(table)),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          utils.times(100, function (n, next) {
            client.execute(query, [types.uuid(), n.toString()], {prepare: 1}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Only fetch 70
          client.execute(util.format('SELECT * FROM %s', table), [], {prepare: 1, fetchSize: 70}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 70);
            pageState = result.pageState;
            metaPageState = result.meta.pageState;
            seriesNext();
          });
        },
        function selectDataRemaining(seriesNext) {
          //The remaining
          client.execute(util.format('SELECT * FROM %s', table), [], {prepare: 1, pageState: pageState}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 30);
            seriesNext();
          });
        },
        function selectDataRemainingWithMetaPageState(seriesNext) {
          //The remaining
          client.execute(util.format('SELECT * FROM %s', table), [], {prepare: 1, pageState: metaPageState}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rows.length, 30);
            seriesNext();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should encode and decode varint values', function (done) {
      var client = setupInfo.client;
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var expectedRows = {};
      utils.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, util.format('CREATE TABLE %s (id uuid primary key, val varint)', table)),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, val) VALUES (?, ?)', table);
          utils.timesLimit(150, 100, function (n, next) {
            var id = types.uuid();
            var value = types.Integer.fromNumber(n * 999);
            value = value.multiply(types.Integer.fromString('9999901443'));
            if (n % 2 === 0) {
              //as a string also
              value = value.toString();
            }
            expectedRows[id] = value.toString();
            client.execute(query, [id, value], {prepare: 1}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          client.execute(util.format('SELECT id, val, varintAsBlob(val) FROM %s', table), [], {prepare: 1}, function (err, result) {
            assert.ifError(err);
            result.rows.forEach(function (row) {
              helper.assertInstanceOf(row['val'], types.Integer);
              var expectedValue = expectedRows[row['id']];
              assert.ok(expectedValue);
              assert.strictEqual(row['val'].toString(), expectedValue.toString());
            });
            seriesNext();
          });
        }
      ], done);
    });
    it('should encode and decode decimal values', function (done) {
      var client = setupInfo.client;
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var expectedRows = {};
      utils.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, util.format('CREATE TABLE %s (id uuid primary key, val decimal)', table)),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, val) VALUES (?, ?)', table);
          utils.timesLimit(150, 100, function (n, next) {
            var id = types.Uuid.random();
            var value = (n * 999).toString() + '.' + (100 + n * 7).toString();
            if (n % 10 === 0) {
              value = '-' + value;
            }
            if (n % 2 === 0) {
              //as a BigDecimal too
              value = types.BigDecimal.fromString(value);
            }
            expectedRows[id] = value.toString();
            client.execute(query, [id, value], {prepare: 1}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          client.execute(util.format('SELECT id, val FROM %s', table), [], {prepare: 1}, function (err, result) {
            assert.ifError(err);
            result.rows.forEach(function (row) {
              helper.assertInstanceOf(row['val'], types.BigDecimal);
              var expectedValue = expectedRows[row['id']];
              assert.ok(expectedValue);
              assert.strictEqual(row['val'].toString(), expectedValue.toString());
            });
            seriesNext();
          });
        }
      ], done);
    });
    describe('with named parameters', function () {
      vit('2.0', 'should allow an array of parameters', function (done) {
        var query = util.format('SELECT * FROM %s WHERE id1 = :id1', commonTable);
        setupInfo.client.execute(query, [ Uuid.random() ], { prepare: 1 }, function (err, result) {
          assert.ifError(err);
          assert.ok(result && result.rows);
          assert.strictEqual(typeof result.rows.length, 'number');
          done();
        });
      });
      vit('2.0', 'should allow associative array of parameters', function (done) {
        var query = util.format('SELECT * FROM %s WHERE id1 = :id1', commonTable);
        setupInfo.client.execute(query, {'id1': Uuid.random()}, {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result && result.rows);
          assert.strictEqual(typeof result.rows.length, 'number');
          done();
        });
      });
      vit('2.0', 'should be case insensitive', function (done) {
        var query = util.format('SELECT * FROM %s WHERE id1 = :ID1', commonTable);
        setupInfo.client.execute(query, {'iD1': Uuid.random()}, {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result && result.rows);
          assert.strictEqual(typeof result.rows.length, 'number');
          done();
        });
      });
      vit('2.0', 'should allow objects with other props as parameters', function (done) {
        var query = util.format('SELECT * FROM %s WHERE id1 = :ID1', commonTable);
        setupInfo.client.execute(query, {'ID1': Uuid.random(), other: 'value'}, {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result && result.rows);
          assert.strictEqual(typeof result.rows.length, 'number');
          done();
        });
      });
    });
    it('should encode and decode maps using Map polyfills', function (done) {
      var client = newInstance({ encoding: { map: helper.Map}});
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var MapPF = helper.Map;
      var values = [
        [
          //map1 to n with array of length 2 as values
          Uuid.random(),
          new MapPF([['k1', 'v1'], ['k2', 'v2'], ['k3', 'v33333']]),
          new MapPF([[-100, new Date(1423499543481)], [1, new Date()]]),
          new MapPF([[new Date(1413496543466), -2], [new Date(), 1.1233799457550049]]),
          new MapPF([[types.Integer.fromString('100000001'), true]]),
          new MapPF([[types.timeuuid(), types.BigDecimal.fromString('1.20008')], [types.timeuuid(), types.BigDecimal.fromString('-9.26')]])
        ]
      ];
      var createTableCql = util.format('CREATE TABLE %s ' +
      '(id uuid primary key, ' +
      'map_text_text map<text,text>, ' +
      'map_int_date map<int,timestamp>, ' +
      'map_date_float map<timestamp,float>, ' +
      'map_varint_boolean map<varint,boolean>, ' +
      'map_timeuuid_text map<timeuuid,decimal>)', table);
      utils.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, createTableCql),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, map_text_text, map_int_date, map_date_float, map_varint_boolean, map_timeuuid_text) ' +
          'VALUES (?, ?, ?, ?, ?, ?)', table);
          utils.each(values, function (params, next) {
            client.execute(query, params, {prepare: true}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Make ? markers C*1.2-compatible
          var markers = values.map(function () { return '?'; }).join(',');
          var query = util.format('SELECT * FROM %s WHERE id IN (' + markers + ')', table);
          client.execute(query, values.map(function (x) { return x[0]; }), {prepare: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result.rows.length);
            result.rows.forEach(function (row) {
              var expectedValues = helper.first(values, function (item) { return item[0].equals(row.id); });
              helper.assertInstanceOf(row['map_text_text'], MapPF);
              assert.strictEqual(row['map_text_text'].toString(), expectedValues[1].toString());
              assert.strictEqual(row['map_int_date'].toString(), expectedValues[2].toString());
              assert.strictEqual(row['map_date_float'].toString(), expectedValues[3].toString());
              assert.strictEqual(row['map_varint_boolean'].toString(), expectedValues[4].toString());
              assert.strictEqual(row['map_timeuuid_text'].toString(), expectedValues[5].toString());
            });
            seriesNext();
          });
        }
      ], done);
    });
    it('should encode and decode sets using Set polyfills', function (done) {
      var client = newInstance({ encoding: { set: helper.Set}});
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var SetPF = helper.Set;
      var values = [
        [
          Uuid.random(),
          new SetPF(['k3', 'v33333122', 'z1', 'z2']),
          new SetPF([new Date(1423499543481), new Date()]),
          new SetPF([-2, 0, 1, 1.1233799457550049]),
          new SetPF([types.Long.fromString('100000001')]),
          new SetPF([types.timeuuid(), types.timeuuid(), types.timeuuid()])
        ],
        [
          Uuid.random(),
          new SetPF(['v1']),
          new SetPF([new Date(1423199543111), new Date()]),
          new SetPF([1, 2]),
          new SetPF([types.Long.fromNumber(-3)]),
          null
        ]
      ];
      var createTableCql = util.format('CREATE TABLE %s ' +
      '(id uuid primary key, ' +
      'set_text set<text>, ' +
      'set_timestamp set<timestamp>, ' +
      'set_float set<float>, ' +
      'set_bigint set<bigint>, ' +
      'set_timeuuid set<timeuuid>)', table);
      utils.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, createTableCql),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, set_text, set_timestamp, set_float, set_bigint, set_timeuuid) ' +
          'VALUES (?, ?, ?, ?, ?, ?)', table);
          utils.each(values, function (params, next) {
            client.execute(query, params, {prepare: true}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          //Make ? markers C*1.2-compatible
          var markers = values.map(function () { return '?'; }).join(',');
          var query = util.format('SELECT * FROM %s WHERE id IN (' + markers + ')', table);
          client.execute(query, values.map(function (x) { return x[0]; }), {prepare: true}, function (err, result) {
            assert.ifError(err);
            assert.ok(result.rows.length);
            result.rows.forEach(function (row) {
              var expectedValues = helper.first(values, function (item) { return item[0].equals(row.id); });
              helper.assertInstanceOf(row['set_text'], SetPF);
              assert.strictEqual(row['set_text'].toString(), expectedValues[1].toString());
              assert.strictEqual(row['set_timestamp'].toString(), expectedValues[2].toString());
              assert.strictEqual(row['set_float'].toString(), expectedValues[3].toString());
              assert.strictEqual(row['set_bigint'].toString(), expectedValues[4].toString());
              if (row['set_timeuuid'] === null) {
                assert.strictEqual(expectedValues[5], null);
              }
              else {
                assert.strictEqual(row['set_timeuuid'].toString(), expectedValues[5].toString());
              }
            });
            seriesNext();
          });
        }
      ], done);
    });
    vit('2.1', 'should support protocol level timestamp', function (done) {
      var client = setupInfo.client;
      var id = Uuid.random();
      var timestamp = types.generateTimestamp(new Date(), 456);
      utils.series([
        function insert(next) {
          var query = util.format('INSERT INTO %s (id1, id2, text_sample) VALUES (?, ?, ?)', commonTable);
          var params = [id, types.TimeUuid.now(), 'hello sample timestamp'];
          client.execute(query, params, { timestamp: timestamp, prepare: 1}, next);
        },
        function select(next) {
          var query = util.format('SELECT text_sample, writetime(text_sample) from %s WHERE id1 = ?', commonTable);
          client.execute(query, [id], { prepare: 1}, function (err, result) {
            var row = result.first();
            assert.ok(row);
            assert.strictEqual(row['text_sample'], 'hello sample timestamp');
            assert.strictEqual(row['writetime(text_sample)'].toString(), timestamp.toString());
            next();
          });
        }
      ], done);
    });
    vit('2.1.3', 'should support nested collections', function (done) {
      var client = newInstance({ keyspace: commonKs,
        queryOptions: { consistency: types.consistencies.quorum,
          prepare: true}});
      var createTableCql = 'CREATE TABLE tbl_nested (' +
        'id uuid PRIMARY KEY, ' +
        'map1 map<text, frozen<set<timeuuid>>>, ' +
        'list1 list<frozen<set<timeuuid>>>)';
      var id = Uuid.random();
      var map = {
        'key1': [types.TimeUuid.now(), types.TimeUuid.now()],
        'key2': [types.TimeUuid.now()]
      };
      var list = [
        [types.TimeUuid.now()],
        [types.TimeUuid.now(), types.TimeUuid.now()]
      ];
      utils.series([
        helper.toTask(client.execute, client, createTableCql),
        function insert(next) {
          var query = 'INSERT INTO tbl_nested (id, map1, list1) VALUES (?, ?, ?)';
          client.execute(query, [id, map, list], next);
        },
        function select(next) {
          var query = 'SELECT * FROM tbl_nested WHERE id = ?';
          client.execute(query, [id], function (err, result) {
            assert.ifError(err);
            var row = result.first();
            assert.ok(row['map1']);
            assert.strictEqual(Object.keys(row['map1']).length, 2);
            assert.ok(util.isArray(row['map1']['key1']));
            assert.strictEqual(row['map1']['key1'].length, 2);
            assert.strictEqual(row['map1']['key1'][0].toString(), map.key1[0].toString());
            assert.strictEqual(row['map1']['key1'][1].toString(), map.key1[1].toString());
            assert.strictEqual(row['map1']['key2'].length, 1);
            assert.strictEqual(row['map1']['key2'][0].toString(), map.key2[0].toString());
            assert.ok(row['list1']);
            assert.strictEqual(row['list1'].length, 2);
            assert.ok(util.isArray(row['list1'][0]));
            assert.strictEqual(row['list1'][0][0].toString(), list[0][0].toString());
            assert.ok(util.isArray(row['list1'][1]));
            assert.strictEqual(row['list1'][1][0].toString(), list[1][0].toString());
            assert.strictEqual(row['list1'][1][1].toString(), list[1][1].toString());
            next();
          });
        }
      ], done);
    });
    vit('2.2', 'should include the warning in the ResultSet', function (done) {
      var client = newInstance();
      var loggedMessage = false;
      client.on('log', function (level, className, message) {
        if (loggedMessage || level !== 'warning') {
          return;
        }
        message = message.toLowerCase();
        if (message.indexOf('batch') >= 0 && message.indexOf('exceeding')) {
          loggedMessage = true;
        }
      });
      var query = util.format(
        "BEGIN UNLOGGED BATCH INSERT INTO %s (id1, id2, text_sample) VALUES (:id0, :id2, :sample)\n" +
        "INSERT INTO %s (id1, id2, text_sample) VALUES (:id1, :id2, :sample) APPLY BATCH",
        commonTable,
        commonTable
      );
      var params = { id0: types.Uuid.random(), id1: types.Uuid.random(), id2: types.TimeUuid.now(), sample: utils.stringRepeat('c', 2562) };
      client.execute(query, params, {prepare: true}, function (err, result) {
        assert.ifError(err);
        assert.ok(result.info.warnings);
        assert.ok(result.info.warnings.length >= 1);
        helper.assertContains(result.info.warnings[0], 'batch');
        assert.ok(loggedMessage);
        client.shutdown(done);
      });
    });
    it('should support hardcoded parameters that are part of the routing key', function (done) {
      var client = setupInfo.client;
      var table = helper.getRandomName('tbl');
      var createQuery = util.format('CREATE TABLE %s (a int, b int, c int, d int, ' +
        'PRIMARY KEY ((a, b, c)))', table);
      utils.series([
        helper.toTask(client.execute, client, createQuery),
        function (next) {
          var query = util.format('SELECT * FROM %s WHERE c = ? AND a = ? AND b = 0', table);
          client.execute(query, [1, 1], { prepare: true}, function (err) {
            assert.ifError(err);
            next();
          });
        }
      ], done);
    });
    it('should allow undefined value as a null or unset depending on the protocol version', function (done) {
      var client1 = newInstance();
      var client2 = newInstance({ encoding: { useUndefinedAsUnset: true}});
      var id = Uuid.random();
      utils.series([
        client1.connect.bind(client1),
        client2.connect.bind(client2),
        function insert2(next) {
          //use undefined
          var query = util.format('INSERT INTO %s (id1, id2, text_sample, map_sample) VALUES (?, ?, ?, ?)', commonTable);
          client2.execute(query, [id, types.TimeUuid.now(), 'test null or unset', undefined], { prepare: true}, next);
        },
        function select2(next) {
          var query = util.format('SELECT id1, id2, text_sample, map_sample FROM %s WHERE id1 = ?', commonTable);
          client2.execute(query, [id], { prepare: true}, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.rowLength, 1);
            var row = result.first();
            assert.strictEqual(row['id1'].toString(), id.toString());
            assert.strictEqual(row['text_sample'], 'test null or unset');
            assert.strictEqual(row['map_sample'], null);
            next();
          });
        },
        client1.shutdown.bind(client1),
        client2.shutdown.bind(client2)
      ], done);
    });
    it('should not allow collections with null or unset values', function (done) {
      var client = setupInfo.client;
      var tid = types.TimeUuid.now();
      utils.series([
        function testListWithNull(next) {
          var query = util.format('INSERT INTO %s (id1, id2, list_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, [tid, null]], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function testListWithUnset(next) {
          var query = util.format('INSERT INTO %s (id1, id2, list_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, [tid, types.unset]], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function testSetWithNull(next) {
          var query = util.format('INSERT INTO %s (id1, id2, set_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, [1, null]], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function testSetWithUnset(next) {
          var query = util.format('INSERT INTO %s (id1, id2, set_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, [1, types.unset]], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function testMapWithNull(next) {
          var map = {};
          map[tid] = 1;
          map[types.TimeUuid.now()] = null;
          var query = util.format('INSERT INTO %s (id1, id2, map_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, map], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function testMapWithUnset(next) {
          var map = {};
          map[tid] = 1;
          map[types.TimeUuid.now()] = types.unset;
          var query = util.format('INSERT INTO %s (id1, id2, map_sample) VALUES (?, ?, ?)', commonTable);
          client.execute(query, [Uuid.random(), tid, map], { prepare: true}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        }
      ], done);
    });
    describe('with udt and tuple', function () {
      before(function (done) {
        var client = setupInfo.client;
        utils.series([
          helper.toTask(client.execute, client, 'CREATE TYPE phone (alias text, number text, country_code int, other boolean)'),
          helper.toTask(client.execute, client, 'CREATE TYPE address (street text, "ZIP" int, phones set<frozen<phone>>)'),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_udts (id uuid PRIMARY KEY, phone_col frozen<phone>, address_col frozen<address>)'),
          helper.toTask(client.execute, client, 'CREATE TABLE tbl_tuples (id uuid PRIMARY KEY, tuple_col1 tuple<text,int>, tuple_col2 tuple<uuid,bigint,boolean>)')
        ], done);
      });
      vit('2.1', 'should encode objects into udt', function (done) {
        var insertQuery = 'INSERT INTO tbl_udts (id, phone_col, address_col) VALUES (?, ?, ?)';
        var selectQuery = 'SELECT id, phone_col, address_col FROM tbl_udts WHERE id = ?';
        var client = newInstance({ keyspace: commonKs, queryOptions: { prepare: true }});
        var id = Uuid.random();
        var phone = { alias: 'work2', number: '555 9012', country_code: 54};
        var address = { street: 'DayMan', ZIP: 28111, phones: [ { alias: 'personal'} ]};
        utils.series([
          function insert(next) {
            client.execute(insertQuery, [id, phone, address], next);
          },
          function select(next) {
            client.execute(selectQuery, [id], function (err, result) {
              assert.ifError(err);
              var row = result.first();
              var phoneResult = row['phone_col'];
              assert.strictEqual(phoneResult.alias, phone.alias);
              assert.strictEqual(phoneResult.number, phone.number);
              assert.strictEqual(phoneResult.country_code, phone.country_code);
              assert.equal(phoneResult.other, phone.other);
              var addressResult = row['address_col'];
              assert.strictEqual(addressResult.street, address.street);
              assert.strictEqual(addressResult.ZIP, address.ZIP);
              assert.strictEqual(addressResult.phones.length, 1);
              assert.strictEqual(addressResult.phones[0].alias, address.phones[0].alias);
              assert.strictEqual(addressResult.phones[0].number, null);
              next();
            });
          },
          client.shutdown.bind(client)
        ], done);
      });
      vit('2.1', 'should encode and decode tuples', function (done) {
        var insertQuery = 'INSERT INTO tbl_tuples (id, tuple_col1, tuple_col2) VALUES (?, ?, ?)';
        var selectQuery = 'SELECT * FROM tbl_tuples WHERE id = ?';
        var client = newInstance({ keyspace: commonKs, queryOptions: { prepare: true}});
        var id1 = Uuid.random();
        var tuple1 = new types.Tuple('val1', 1);
        var tuple2 = new types.Tuple(Uuid.random(), types.Long.fromInt(12), true);
        utils.series([
          function insert1(next) {
            client.execute(insertQuery, [id1, tuple1, tuple2], next);
          },
          function insert2(next) {
            client.execute(insertQuery, [Uuid.random(), new types.Tuple('unset pair', undefined), null], next);
          },
          function insert3(next) {
            client.execute(insertQuery, [Uuid.random(), new types.Tuple('null pair', null), null], next);
          },
          function select1(next) {
            client.execute(selectQuery, [id1], function (err, result) {
              assert.ifError(err);
              var row = result.first();
              var tuple1Result = row['tuple_col1'];
              var tuple2Result = row['tuple_col2'];
              assert.strictEqual(tuple1Result.length, 2);
              assert.strictEqual(tuple1Result.get(0), 'val1');
              assert.strictEqual(tuple1Result.get(0), tuple1.get(0));
              assert.strictEqual(tuple1Result.get(1), tuple1.get(1));
              assert.strictEqual(tuple2Result.length, 3);
              assert.strictEqual(tuple2Result.get(0).toString(), tuple2.get(0).toString());
              assert.strictEqual(tuple2Result.get(1).toString(), '12');
              assert.strictEqual(tuple2Result.get(2), tuple2.get(2));
              next();
            });
          }
        ], done);
      });
    });
    describe('with smallint and tinyint types', function () {
      var insertQuery = 'INSERT INTO tbl_smallints (id, smallint_sample, tinyint_sample) VALUES (?, ?, ?)';
      var selectQuery = 'SELECT id, smallint_sample, tinyint_sample FROM tbl_smallints WHERE id = ?';
      before(function (done) {
        var query = 'CREATE TABLE tbl_smallints ' +
          '(id uuid PRIMARY KEY, smallint_sample smallint, tinyint_sample tinyint, text_sample text)';
        setupInfo.client.execute(query, done);
      });
      vit('2.2', 'should encode and decode smallint and tinyint values as Number', function (done) {
        var values = [
          [Uuid.random(), 1, 1],
          [Uuid.random(), 0, 0],
          [Uuid.random(), -1, -2],
          [Uuid.random(), -130, -128]
        ];
        var client = setupInfo.client;
        utils.eachSeries(values, function (params, next) {
          client.execute(insertQuery, params, { prepare: true}, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [params[0]], { prepare: true}, function (err, result) {
              assert.ifError(err);
              assert.ok(result);
              assert.ok(result.rowLength);
              var row = result.first();
              assert.ok(row);
              assert.strictEqual(row['id'].toString(), params[0].toString());
              assert.strictEqual(row['smallint_sample'], params[1]);
              assert.strictEqual(row['tinyint_sample'], params[2]);
              next();
            });
          });
        }, done);
      });
    });
    describe('with date and time types', function () {
      var LocalDate = types.LocalDate;
      var LocalTime = types.LocalTime;
      var insertQuery = 'INSERT INTO tbl_datetimes (id, date_sample, time_sample) VALUES (?, ?, ?)';
      var selectQuery = 'SELECT id, date_sample, time_sample FROM tbl_datetimes WHERE id = ?';
      before(function (done) {
        var query = 'CREATE TABLE tbl_datetimes ' +
          '(id uuid PRIMARY KEY, date_sample date, time_sample time, text_sample text)';
        setupInfo.client.execute(query, done);
      });
      vit('2.2', 'should encode and decode date and time values as LocalDate and LocalTime', function (done) {
        var values = [
          [Uuid.random(), new LocalDate(1969, 10, 13), new LocalTime(types.Long.fromString('0'))],
          [Uuid.random(), new LocalDate(2010, 4, 29), LocalTime.fromString('15:01:02.1234')],
          [Uuid.random(), new LocalDate(2005, 8, 5), LocalTime.fromString('01:56:03.000501')],
          [Uuid.random(), new LocalDate(1983, 2, 24), new LocalTime(types.Long.fromString('86399999999999'))],
          [Uuid.random(), new LocalDate(-2147483648), new LocalTime(types.Long.fromString('6311999549933'))]
        ];
        var client = setupInfo.client;
        utils.eachSeries(values, function (params, next) {
          client.execute(insertQuery, params, { prepare: true}, function (err) {
            assert.ifError(err);
            client.execute(selectQuery, [params[0]], { prepare: true}, function (err, result) {
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
        }, done);
      });
    });
    describe('with unset', function () {
      vit('2.2', 'should allow unset as a valid value', function (done) {
        var client1 = newInstance();
        var client2 = newInstance({ encoding: { useUndefinedAsUnset: true}});
        var id1 = Uuid.random();
        var id2 = Uuid.random();
        utils.series([
          client1.connect.bind(client1),
          client2.connect.bind(client2),
          function insert1(next) {
            var query = util.format('INSERT INTO %s (id1, id2, text_sample, map_sample) VALUES (?, ?, ?, ?)', commonTable);
            client1.execute(query, [id1, types.TimeUuid.now(), 'test unset', types.unset], { prepare: true}, next);
          },
          function select1(next) {
            var query = util.format('SELECT id1, id2, text_sample, map_sample FROM %s WHERE id1 = ?', commonTable);
            client1.execute(query, [id1], { prepare: true}, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              var row = result.first();
              assert.strictEqual(row['id1'].toString(), id1.toString());
              assert.strictEqual(row['text_sample'], 'test unset');
              assert.strictEqual(row['map_sample'], null);
              next();
            });
          },
          function insert2(next) {
            //use undefined
            var query = util.format('INSERT INTO %s (id1, id2, text_sample, map_sample) VALUES (?, ?, ?, ?)', commonTable);
            client2.execute(query, [id2, types.TimeUuid.now(), 'test unset 2', undefined], { prepare: true}, next);
          },
          function select2(next) {
            var query = util.format('SELECT id1, id2, text_sample, map_sample FROM %s WHERE id1 = ?', commonTable);
            client2.execute(query, [id2], { prepare: true}, function (err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              var row = result.first();
              assert.strictEqual(row['id1'].toString(), id2.toString());
              assert.strictEqual(row['text_sample'], 'test unset 2');
              assert.strictEqual(row['map_sample'], null);
              next();
            });
          },
          client1.shutdown.bind(client1),
          client2.shutdown.bind(client2)
        ], done);
      });
    });
    describe('with secondary indexes', function() {
      it('should be able to retrieve using simple index', function(done) {
        var client = setupInfo.client;
        var table = helper.getRandomName('tbl');
        utils.series([
          helper.toTask(client.execute, client, util.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", table)),
          helper.toTask(client.execute, client, util.format("CREATE INDEX simple_index ON %s (v)", table)),
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (k, v) VALUES (?, ?)', table);
            utils.times(100, function (n, next) {
              client.execute(query, [n, n % 10], {prepare: 1}, next);
            }, seriesNext);
          },
          function selectData(seriesNext) {
            var query = util.format('SELECT * FROM %s WHERE v=?', table);
            client.execute(query, [0], {prepare: 1}, function(err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 10);
              // each key should be a multiple of 10.
              var keys = result.rows.map(function(row) {
                assert.strictEqual(row['v'], 0);
                return row['k'];
              }).sort();
              assert.deepEqual(keys, [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
              seriesNext();
            });
          }
        ],done);
      });
      vit('2.1', 'should be able to retrieve using index on frozen list', function(done) {
        var client = setupInfo.client;
        var table = helper.getRandomName('tbl');
        utils.series([
          helper.toTask(client.execute, client, util.format("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<int>>)", table)),
          helper.toTask(client.execute, client, util.format("CREATE INDEX frozen_index ON %s (full(v))", table)),
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (k, v) VALUES (?, ?)', table);
            utils.times(100, function (n, next) {
              client.execute(query, [n, [n-1, n-2, n-3]], {prepare: 1}, next);
            }, seriesNext);
          },
          function selectData(seriesNext) {
            var query = util.format('SELECT * FROM %s WHERE v=?', table);
            client.execute(query, [[20,19,18]], {prepare: 1}, function(err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              var row = result.rows[0];
              assert.strictEqual(row['k'], 21);
              assert.deepEqual(row['v'], [20,19,18]);
              seriesNext();
            });
          }
        ],done);
      });
      vit('2.1', 'should be able to retrieve using index on map keys', function(done) {
        var client = setupInfo.client;
        var table = helper.getRandomName('tbl');
        utils.series([
          helper.toTask(client.execute, client, util.format("CREATE TABLE %s (k int PRIMARY KEY, v map<text,int>)", table)),
          helper.toTask(client.execute, client, util.format("CREATE INDEX keys_index on %s (keys(v))", table)),
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (k, v) VALUES (?, ?)', table);
            utils.times(100, function (n, next) {
              var v = {
                'key1' : n + 1,
                'keyt10' : n * 10
              };
              if(n % 10 === 0) {
                v['by10'] = n / 10;
              }
              client.execute(query, [n, v], {prepare :1}, next);
            }, seriesNext);
          },
          function selectData(seriesNext) {
            var query = util.format('SELECT * FROM %s WHERE v CONTAINS KEY ?', table);
            client.execute(query, ['by10'], {prepare: 1}, function(err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 10);
              // each key should be a multiple of 10.
              var keys = result.rows.map(function(row) {
                var k = row['k'];
                assert.deepEqual(row['v'], {'key1': k + 1, 'keyt10' : k * 10, 'by10' : k / 10});
                return k;
              }).sort();
              assert.deepEqual(keys, [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
              seriesNext();
            });
          }
        ], done);
      });
      vit('2.1', 'should be able to retrieve using index on map values', function(done) {
        var client = setupInfo.client;
        var table = helper.getRandomName('tbl');
        utils.series([
          helper.toTask(client.execute, client, util.format("CREATE TABLE %s (k int PRIMARY KEY, v map<text,int>)", table)),
          helper.toTask(client.execute, client, util.format("CREATE INDEX values_index on %s (v)", table)),
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (k, v) VALUES (?, ?)', table);
            utils.times(100, function (n, next) {
              var v = {
                'key1' : n + 1,
                'keyt10' : n * 10
              };
              client.execute(query, [n, v], {prepare :1}, next);
            }, seriesNext);
          },
          function selectData(seriesNext) {
            var query = util.format('SELECT * FROM %s WHERE v CONTAINS ?', table);
            client.execute(query, [100], {prepare: 1}, function(err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 2);
              var rows = result.rows.sort(function(a, b) {
                return a['k'] - b['k'];
              });

              assert.strictEqual(rows[0]['k'], 10);
              assert.deepEqual(rows[0]['v'], {'key1' : 11, 'keyt10' : 100});
              assert.strictEqual(rows[1]['k'], 99);
              assert.deepEqual(rows[1]['v'], {'key1' : 100, 'keyt10' : 990});
              seriesNext();
            });
          }
        ], done);
      });
      vit('2.2', 'should be able to retrieve using index on map entries', function(done) {
        var client = setupInfo.client;
        var table = helper.getRandomName('tbl');
        utils.series([
          helper.toTask(client.execute, client, util.format("CREATE TABLE %s (k int PRIMARY KEY, v map<text,int>)", table)),
          helper.toTask(client.execute, client, util.format("CREATE INDEX entries_index on %s (entries(v))", table)),
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (k, v) VALUES (?, ?)', table);
            utils.times(100, function (n, next) {
              var v = {
                'key1' : n + 1,
                'keyt10' : n * 10
              };
              client.execute(query, [n, v], {prepare :1}, next);
            }, seriesNext);
          },
          function selectData(seriesNext) {
            var query = util.format('SELECT * FROM %s WHERE v[?]=?', table);
            client.execute(query, ['key1', 100], {prepare: 1}, function(err, result) {
              assert.ifError(err);
              assert.strictEqual(result.rowLength, 1);
              var rows = result.rows;
              assert.strictEqual(rows[0]['k'], 99);
              assert.deepEqual(rows[0]['v'], {'key1' : 100, 'keyt10' : 990});
              seriesNext();
            });
          }
        ], done);
      });
    });
    vdescribe('3.0', 'with materialized views', function () {
      var keyspace = 'ks_view_prepared';
      before(function createTables(done) {
        var queries = [
          "CREATE KEYSPACE ks_view_prepared WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
          "CREATE TABLE ks_view_prepared.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
          "CREATE MATERIALIZED VIEW ks_view_prepared.alltimehigh AS SELECT user FROM scores WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL PRIMARY KEY (game, score, user, year, month, day) WITH CLUSTERING ORDER BY (score desc)"
        ];
        utils.eachSeries(queries, setupInfo.client.execute.bind(setupInfo.client), helper.wait(2000, done));
      });
      it('should choose the correct coordinator based on the partition key', function (done) {
        var client = new Client({
          policies: { loadBalancing: new loadBalancing.TokenAwarePolicy(new loadBalancing.RoundRobinPolicy())},
          keyspace: keyspace,
          contactPoints: helper.baseOptions.contactPoints
        });
        utils.timesSeries(10, function (n, timesNext) {
          var game = n.toString();
          var query = 'SELECT * FROM alltimehigh WHERE game = ?';
          client.execute(query, [game], { traceQuery: true, prepare: true}, function (err, result) {
            assert.ifError(err);
            var coordinator = result.info.queriedHost;
            var traceId = result.info.traceId;
            client.metadata.getTrace(traceId, function (err, trace) {
              assert.ifError(err);
              trace.events.forEach(function (event) {
                assert.strictEqual(helper.lastOctetOf(event['source'].toString()), helper.lastOctetOf(coordinator.toString()));
              });
              timesNext();
            });
          });
        }, helper.finish(client, done));
      });
    });
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  options = options || {};
  options = utils.deepExtend({
    queryOptions: {consistency: types.consistencies.quorum}
  }, options, helper.baseOptions);
  return new Client(options);
}

function serializationTest(client, values, columns, done) {
  var keyspace = helper.getRandomName('ks');
  var table = keyspace + '.' + helper.getRandomName('table');
  utils.series([
    helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
    helper.toTask(client.execute, client, helper.createTableCql(table)),
    function (next) {
      var markers = '?';
      var columnsSplit = columns.split(',');
      for (var i = 1; i < columnsSplit.length; i++) {
        markers += ', ?';
      }
      var query = util.format('INSERT INTO %s ' +
        '(%s) VALUES ' +
        '(%s)', table, columns, markers);
      client.execute(query, values, {prepare: 1}, next);
    },
    function (next) {
      var query = util.format('SELECT %s FROM %s WHERE id = ?', columns, table);
      client.execute(query, [values[0]], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows && result.rows.length > 0, 'There should be a row');
        var row = result.rows[0];
        assert.strictEqual(row.values().length, values.length);
        for (var i = 0; i < values.length; i++) {
          helper.assertValueEqual(values[i], row.get(i));
        }
        next();
      });
    }
  ], done);
}