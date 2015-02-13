var assert = require('assert');
var async = require('async');
var util = require('util');

var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var types = require('../../../lib/types');
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');

describe('Client', function () {
  this.timeout(120000);
  describe('#execute(query, params, {prepare: 1}, callback)', function () {
    before(helper.ccmHelper.start(3));
    after(helper.ccmHelper.remove);
    it('should execute a prepared query with parameters on all hosts', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces where keyspace_name = ?';
      async.timesSeries(3, function (n, next) {
        client.execute(query, ['system'], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 3);
          assert.notEqual(result, null);
          assert.notEqual(result.rows, null);
          next();
        });
      }, done);
    });
    it('should callback with error when query is invalid', function (done) {
      var client = newInstance();
      var query = 'SELECT WILL FAIL';
      client.execute(query, ['system'], {prepare: 1}, function (err) {
        assert.ok(err);
        assert.strictEqual(err.code, types.responseErrorCodes.syntaxError);
        assert.strictEqual(err.query, query);
        done();
      });
    });
    it('should prepare and execute a query without parameters', function (done) {
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_columnfamilies', null, {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows.length);
        done();
      });
    });
    it('should prepare and execute a queries in parallel', function (done) {
      var client = newInstance();
      var queries = [
        'SELECT * FROM system.schema_columnfamilies',
        'SELECT * FROM system.schema_keyspaces',
        'SELECT * FROM system.schema_keyspaces where keyspace_name = ?',
        'SELECT * FROM system.schema_columnfamilies where keyspace_name IN (?, ?)'
      ];
      var params = [
        null,
        null,
        ['system'],
        ['system', 'other']
      ];
      async.times(100, function (n, next) {
        var index = n % 4;
        client.execute(queries[index], params[index], {prepare: 1}, function (err, result) {
          assert.ifError(err);
          assert.ok(result);
          assert.ok(result.rows.length);
          next();
        });
      }, done);
    });
    it('should fail following times if it fails to prepare', function (done) {
      var client = newInstance();
      async.series([function (seriesNext) {
        //parallel
        async.times(10, function (n, next) {
          client.execute('SELECT * FROM system.table1', ['val'], {prepare: 1}, function (err) {
            helper.assertInstanceOf(err, Error);
            helper.assertInstanceOf(err, errors.ResponseError);
            assert.strictEqual(err.code, types.responseErrorCodes.invalid);
            next();
          });
        }, seriesNext);
      }, function (seriesNext) {
        async.timesSeries(10, function (n, next) {
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
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_keyspaces where keyspace_name = ?', [1000], {prepare: 1}, function (err) {
        helper.assertInstanceOf(err, Error);
        helper.assertInstanceOf(err, TypeError);
        done();
      });
    });
    it('should serialize all guessed types', function (done) {
      var values = [types.Uuid.random(), 'as', '111', null, new types.Long(0x1001, 0x0109AA), 1, new Buffer([1, 240]), true, new Date(1221111111), null, null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';

      serializationTest(values, columnNames, done);
    });
    it('should serialize all null values', function (done) {
      var values = [types.Uuid.random(), null, null, null, null, null, null, null, null, null, null, null, null];
      var columnNames = 'id, ascii_sample, text_sample, int_sample, bigint_sample, double_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample, timeuuid_sample, list_sample, set_sample';
      serializationTest(values, columnNames, done);
    });
    it('should use prepared metadata to determine the type of params in query', function (done) {
      var values = [types.Uuid.random(), types.TimeUuid.now(), [1, 1000, 0], {k: '1'}, 1, -100019, ['arr'], new Buffer([192, 168, 1, 200])];
      var columnNames = 'id, timeuuid_sample, list_sample2, map_sample, int_sample, float_sample, set_sample, inet_sample';
      serializationTest(values, columnNames, done);
    });
    it('should support IN clause with 1 marker @c2_0', function (done) {
      var client = newInstance();
      client.execute('SELECT * FROM system.schema_keyspaces WHERE keyspace_name IN ?', [['system', 'another']], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result);
        assert.ok(result.rows.length);
        done();
      });
    });
    it('should use pageState and fetchSize @c2_0', function (done) {
      var client = newInstance();
      var pageState;
      var metaPageState;
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      async.series([
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
          async.times(100, function (n, next) {
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
        }
      ], done);
    });
    it('should encode and decode varint values', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var expectedRows = {};
      async.series([
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          var query = util.format('CREATE TABLE %s (id uuid primary key, val varint)', table);
          client.execute(query, helper.waitSchema(client, next));
        },
          function insertData(seriesNext) {
            var query = util.format('INSERT INTO %s (id, val) VALUES (?, ?)', table);
            async.times(150, function (n, next) {
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
    it('should allow named parameters with array of parameters', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = :ksname';
      client.execute(query, ['system'], {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result && result.rows);
        assert.strictEqual(result.rows.length, 1);
        done();
      });
    });
    it('should allow named parameters with parameters as an associative array', function (done) {
      var client = newInstance();
      var query = 'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = :ksname';
      client.execute(query, {'ksname': 'system'}, {prepare: 1}, function (err, result) {
        assert.ifError(err);
        assert.ok(result && result.rows);
        assert.strictEqual(result.rows.length, 1);
        done();
      });
    });
    it('should encode and decode decimal values', function (done) {
      var client = newInstance();
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var expectedRows = {};
      async.series([
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
        },
        function (next) {
          var query = util.format('CREATE TABLE %s (id uuid primary key, val decimal)', table);
          client.execute(query, helper.waitSchema(client, next));
        },
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, val) VALUES (?, ?)', table);
          async.times(150, function (n, next) {
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
    it('should encode and decode maps using Map polyfills', function (done) {
      var client = newInstance({ encoding: { map: helper.Map}});
      var keyspace = helper.getRandomName('ks');
      var table = keyspace + '.' + helper.getRandomName('table');
      var MapPF = helper.Map;
      var values = [
        [
          //map1 to n with array of length 2 as values
          types.Uuid.random(),
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
      async.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, createTableCql),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, map_text_text, map_int_date, map_date_float, map_varint_boolean, map_timeuuid_text) ' +
          'VALUES (?, ?, ?, ?, ?, ?)', table);
          async.each(values, function (params, next) {
            client.execute(query, params, {prepare: true}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          var query = util.format('SELECT * FROM %s WHERE id IN ?', table);
          client.execute(query, [values.map(function (x) { return x[0]; })], {prepare: true}, function (err, result) {
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
          types.Uuid.random(),
          new SetPF(['k3', 'v33333122', 'z1', 'z2']),
          new SetPF([new Date(1423499543481), new Date()]),
          new SetPF([-2, 0, 1, 1.1233799457550049]),
          new SetPF([types.Long.fromString('100000001')]),
          new SetPF([types.timeuuid(), types.timeuuid(), types.timeuuid()])
        ],
        [
          types.Uuid.random(),
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
      async.series([
        helper.toTask(client.execute, client, helper.createKeyspaceCql(keyspace, 3)),
        helper.toTask(client.execute, client, createTableCql),
        function insertData(seriesNext) {
          var query = util.format('INSERT INTO %s (id, set_text, set_timestamp, set_float, set_bigint, set_timeuuid) ' +
          'VALUES (?, ?, ?, ?, ?, ?)', table);
          async.each(values, function (params, next) {
            client.execute(query, params, {prepare: true}, next);
          }, seriesNext);
        },
        function selectData(seriesNext) {
          var query = util.format('SELECT * FROM %s WHERE id IN ?', table);
          client.execute(query, [values.map(function (x) { return x[0]; })], {prepare: true}, function (err, result) {
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
  });
});

/**
 * @returns {Client}
 */
function newInstance(options) {
  var logEmitter = function () {};
  options = options || {};
  options = utils.extend(options, {logEmitter: logEmitter}, helper.baseOptions);
  return new Client(options);
}

function serializationTest(values, columns, done) {
  var client = newInstance();
  var keyspace = helper.getRandomName('ks');
  var table = keyspace + '.' + helper.getRandomName('table');
  async.series([
    function (next) {
      client.execute(helper.createKeyspaceCql(keyspace, 3), helper.waitSchema(client, next));
    },
    function (next) {
      client.execute(helper.createTableCql(table), helper.waitSchema(client, next));
    },
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