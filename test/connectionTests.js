var assert = require('assert');
var util = require('util');
var async = require('async');

var Connection = require('../index.js').Connection;
var types = require('../lib/types.js');
var utils = require('../lib/utils.js');
var dataTypes = types.dataTypes;
var helper = require('./testHelper.js');
var keyspace = 'unittestkp1_1';
var config = require('./config.js');

types.consistencies.getDefault = function () {return this.one};

var con = new Connection(utils.extend({}, config, {maxRequests: 32}));

describe('Connection', function () {
  before(function (done) {
    this.timeout(5000);
    async.series([
      function open (callback) {
        con.open(callback);
      },
      function dropKeyspace(callback) {
        var query = util.format("DROP KEYSPACE %s;", keyspace);
        con.execute(query, [], function (err) {
          if (err && err.name === 'ResponseError') {
            //don't mind if there is an response error
            err = null;
          }
          callback(err);
        });
      },
      function createKeyspace(callback) {
        var query = util.format("CREATE KEYSPACE %s WITH replication = " +
          "{'class': 'SimpleStrategy','replication_factor': '3'};", keyspace);
        con.execute(query, callback);
      },
      function useKeyspace(callback) {
        var query = util.format("USE %s;", keyspace);
        con.execute(query, callback);
      },
      function createTestTable(callback) {
        con.execute(
          "CREATE TABLE sampletable1 (" +
            "id int PRIMARY KEY," +
            "big_sample bigint," +
            "timestamp_sample timestamp," +
            "blob_sample blob," +
            "decimal_sample decimal," +
            "float_sample float," +
            "uuid_sample uuid," +
            "timeuuid_sample timeuuid," +
            "boolean_sample boolean," +
            "double_sample double," +
            "list_sample list<int>," +
            "list_float_sample list<float>," +
            "set_sample set<int>," +
            "map_sample map<text, text>," +
            "int_sample int," +
            "inet_sample inet," +
            "text_sample text);"
          , callback);
      }
    ], done);
  });
  
  describe('#open()', function () {
    it('should fail when the host does not exits', function (done) {
      this.timeout(5000);
      var localCon = new Connection(utils.extend({}, config, {host: 'not-existent-host'}));
      localCon.open(function (err) {
        assert.ok(err, 'Must return a connection error');
        assert.ok(!localCon.connected && !localCon.connecting);
        localCon.close(done);
      });
    });
    it('should fail when the keyspace does not exist', function (done) {
      var localCon = new Connection(utils.extend({}, con.options, {keyspace: 'this__keyspace__does__not__exist'}));
      localCon.open(function (err) {
        assert.ok(err, 'An error must be returned as the keyspace does not exist');
        localCon.close(done);
      });
    });
    it('should fail when the username and password are incorrect', function (done) {
      this.timeout(5000);
      var localCon = new Connection(utils.extend({}, con.options, {password: 'invalidpassword789'}));
      localCon.open(function (err) {
        //it could be that authentication is off on the node
        //in that case, do not assert anything
        if (err) {
          assert.strictEqual(err.name, 'ResponseError');
          assert.strictEqual(err.code, types.responseErrorCodes.badCredentials);
        }
        localCon.close(done);
      });
    });
  });
  
  describe('#execute()', function () {
    it('should allow from 2 to 4 arguments and use defaults', function (done) {
      async.series([
        function (callback) {
          //all params
          con.execute('SELECT * FROM system.schema_keyspaces', [], types.consistencies.one, function(err){
            callback(err);
          });
        },
        function (callback) {
          //no consistency specified
          con.execute('SELECT * FROM system.schema_keyspaces', [], function(err){
            callback(err);
          });
        },
        function (callback) {
          //change the meaning of the second parameter to consistency
          con.execute('SELECT * FROM system.schema_keyspaces', types.consistencies.one, function(err){
            callback(err);
          });
        },
        function (callback) {
          //query params but no params args, consistency specified, must fail
          con.execute('SELECT * FROM system.schema_keyspaces keyspace_name = ?', types.consistencies.one, function(err){
            if (!err) {
              callback(new Error('Consistency should not be treated as query parameters'));
            }
            else {
              callback(null);
            }
          });
        },
        function (callback) {
          //no query params
          con.execute('SELECT * FROM system.schema_keyspaces', function(err){
            callback(err);
          });
        }
      ], done);
    });
    
    it ('should return a zero-length Array of rows when no matches', function (done) {
      con.execute('SELECT * FROM sampletable1 WHERE ID = -1', function (err, result) {
        assert.ok(!err, err);
        assert.ok(result && result.rows && result.rows.length === 0, 'Rows must be defined and the length of the rows array must be zero');
        done();
      });
    });
    
    it('should retrieve all the columns in the select statement in order', function (done) {
      con.execute('select keyspace_name, strategy_class, strategy_options from system.schema_keyspaces;', [], function(err, result) {
        assert.ok(!err, err);
        assert.ok(result.rows.length > 0, 'No keyspaces');
        assert.strictEqual(result.meta.columns.length, 3, 'There must be 2 columns');
        var row = result.rows[0];
        assert.ok(row.get('keyspace_name'), 'Get cell by column name failed');
        assert.strictEqual(row.get(0), row['keyspace_name']);
        assert.strictEqual(row.get(1), row['strategy_class']);
        done();
      });
    });
    
    it('should return javascript null for null stored values', function (done) {
      con.execute('INSERT INTO sampletable1 (id) VALUES(1)', null, function(err) {
        assert.ok(!err, err);
        con.execute('select * from sampletable1 where id=?', [1], function (err, result) {
          assert.ok(!err, err);
          assert.ok(result.rows && result.rows.length === 1);
          var row = result.rows[0];
          assert.strictEqual(row.get('big_sample'), null);
          assert.strictEqual(row.get('blob_sample'), null);
          assert.strictEqual(row.get('decimal_sample'), null);
          assert.strictEqual(row.get('list_sample'), null);
          assert.strictEqual(row.get('map_sample'), null);
          assert.strictEqual(row.get('set_sample'), null);
          assert.strictEqual(row.get('text_sample'), null);
          done();
        });
      });
    });
    
    it('should callback with a ResponseError when the query is malformed', function (done) {
      con.execute("Malformed SLEECT SELECT * FROM sampletable1;", null, function(err) {
        assert.ok(err, 'This query must yield an error.');
        assert.ok(!err.isServerUnhealthy, 'The connection should be reusable and the server should be healthy even if a query fails.');
        assert.strictEqual(err.name, 'ResponseError', 'The error should be of type ResponseError');
        done();
      });
    });
    
    it('should store and retrieve the same bigint value', function (done) {
      var big = new types.Long('0x0123456789abcdef0');
      con.execute('INSERT INTO sampletable1 (id, big_sample) VALUES(100, ?)', [big], function(err) {
        assert.ok(!err, err);
        con.execute('select id, big_sample from sampletable1 where id=100;', null, function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(
            types.Long.toBuffer(big).toString('hex'),
            types.Long.toBuffer(result.rows[0].big_sample).toString('hex'),
            'Retrieved bigint does not match.');
          done();
        });
      });
    });
    
    it('should store and retrieve the same uuid value', function (done) {
      var uuidValue = types.uuid();
      con.execute('INSERT INTO sampletable1 (id, uuid_sample) VALUES(150, ?)', [uuidValue], function (err, result) {
        assert.ok(!err, err);
        con.execute('SELECT id, uuid_sample FROM sampletable1 WHERE ID=150', function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows[0].uuid_sample, uuidValue);
          done();
        });
      });
    });

    it('should store and retrieve the same timeuuid value', function (done) {
      var timeUuidValue = types.timeuuid();
      con.execute('INSERT INTO sampletable1 (id, timeuuid_sample) VALUES(151, ?)', [timeUuidValue], function (err, result) {
        assert.ok(!err, err);
        con.execute('SELECT id, timeuuid_sample, dateOf(timeuuid_sample) as date1 FROM sampletable1 WHERE ID=151', function (err, result) {
          assert.ok(!err, err);
          assert.ok(result.rows[0].date1 instanceof Date, 'The dateOf function should yield an instance of date');
          assert.strictEqual(result.rows[0].timeuuid_sample, timeUuidValue);
          done();
        });
      });
    });
    
    it('should insert the same value as param or as query literal', function (done) {
      var queryFields = "INSERT INTO sampletable1 (id, big_sample, blob_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample)";
      var insertQueries = [
        [queryFields + " values (200, 1, 0x48656c6c6f, [1, 2, 3], [1.1, 1, 1.02], {1, 2, 3}, {'a': 'value a', 'b': 'value b'}, 'abc');"],
        [queryFields + " values (201, ?, ?, ?, ?, ?, ?, ?);",
          [types.Long.fromNumber(1), new Buffer('Hello', 'utf-8'), [1, 2, 3],
            {hint: 'list<float>', value: [1.1, 1, 1.02]}, {hint: 'set<int>', value: [1, 2, 3]},
            {hint: 'map', value: {'a': 'value a', 'b': 'value b'}}, 'abc']
        ],
        [queryFields + " values (202, NULL, NULL, NULL, NULL, NULL, NULL, null);"],
        [queryFields + " values (203, ?, ?, ?, ?, ?, ?, ?);", [null, null, null, null, null, null, null]]
      ];
      async.eachSeries(insertQueries, function(query, next) {
        con.execute(query[0], query[1], next);
      }, function (err) {
        assert.ok(!err, err);
        con.execute("select id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample from sampletable1 where id IN (200, 201, 202, 203);", null, function(err, result) {
          assert.ok(!err, err);
          setRowsByKey(result.rows, 'id');
          //sort the rows retrieved
          var rows = [result.rows.get(200), result.rows.get(201), result.rows.get(202), result.rows.get(203)]
          
          //test that coming from parameters or hardcoded query, it stores and yields the same values
          compareRows(rows[0], rows[1], ['big_sample', 'blob_sample', 'decimal_sample', 'list_sample', 'list_float_sample', 'set_sample', 'map_sample', 'text_sample']);
          compareRows(rows[2], rows[3], ['big_sample', 'blob_sample', 'decimal_sample', 'list_sample', 'list_float_sample', 'set_sample', 'map_sample', 'text_sample']);
          done();
        });
      });
    });
    
    it('should handle multiple parallel queries and callback per each', function (done) {
      async.times(15, function(n, callback) {
        con.execute('SELECT * FROM sampletable1', [], function (err, result) {
          assert.ok(result && result.rows && result.rows.length, 'It should return all rows');
          callback(err, result);
        });
      }, function (err, results) {
        done(err);
      });
    });
    
    it('should queue the query when the amount of parallel requests is reached', function (done) {
      //tests that max streamId is reached and the connection waits for a free id
      var options = utils.extend({}, con.options, {maxRequests: 10, maxRequestsRetry: 0});
      //total amount of queries to issue
      var totalQueries = 50;
      var timeoutId;
      var localCon = new Connection(options);
      localCon.open(function (err) {
        assert.ok(!err, err);
        async.times(totalQueries, function (n, callback) {
          var query = util.format('SELECT * FROM %s.sampletable1 WHERE ID IN (?, ?, ?);', keyspace);
          localCon.execute(query, [1, 100, 200], callback);
        }, done);
      });
    });

    it('should return page state when page size is smaller that total rows ', function (done) {
      var query = 'SELECT * FROM sampletable1 LIMIT 200';
      con.execute(query, [], types.consistencies.one, {pageSize: 2}, function (err, result) {
        assert.ok(!err, err);
        assert.ok(result && result.rows, 'It should return rows');
        assert.strictEqual(result.rows.length, 2);
        assert.ok(result.meta.pageState, 'Page state should be returned');
        done();
      });
    });
  });

  describe('#prepare()', function () {
    it('should prepare a query and yield the id', function (done) {
      con.prepare('select id, big_sample from sampletable1 where id = ?', function (err, result) {
        assert.ok(result.id, 'The query id must be defined');
        done(err);
      });
    });

    it('should prepare the queries and yield the ids', function (done) {
      async.series([
        function (callback) {
          con.prepare("select id, big_sample from sampletable1 where id = ?", callback);
        },
        function (callback) {
          con.prepare("select id, big_sample from sampletable1 where id = ?", callback);
        }, 
        function (callback) {
          con.prepare("select id, big_sample, map_sample from sampletable1 where id = ?", callback);
        }
      ], function (err, results) {
        assert.ok(!err, err);
        assert.ok(results[0].id.toString('hex').length > 0);
        assert.ok(results[0].id.toString('hex') === results[1].id.toString('hex'), 'Ids to same queries should be the same');
        assert.ok(results[1].id.toString('hex') !== results[2].id.toString('hex'), 'Ids to different queries should be different');
        done();
      });
    });
  });
  
  describe('#executePrepared()', function () {
    it('should execute a prepared query without parameters', function (done) {
      con.prepare('select id, big_sample from sampletable1 where id = 200', function (err, result) {
        assert.ok(!err, err);
        con.executePrepared(result.id, function (err, result) {
          assert.ok(!err, err);
          assert.ok(result, 'it should return a result');
          assert.ok(result.rows, 'it should return a row Array');
          done();
        });
      })
    });

    it('should execute a prepared query with one parameter', function (done) {
      con.prepare('select id, big_sample from sampletable1 where id = ?', function (err, result) {
        assert.ok(!err, err);
        con.executePrepared(result.id, [200], types.consistencies.one, function (err, result) {
          assert.ok(!err, err);
          assert.ok(result, 'it should return a result');
          assert.ok(result.rows, 'it should return a row Array');
          done();
        });
      })
    });

    it('should serialize all types correctly', function (done) {
      function prepareInsertTest(idValue, columnName, columnValue, hint, compareFunc) {
        if (!compareFunc) {
          compareFunc = function (value) {return value};
        }
        var paramValue = columnValue;
        if (hint) {
          paramValue = {value: paramValue, hint: hint};
        }
        return function (callback) {
          con.prepare("INSERT INTO sampletable1 (id, " + columnName + ") VALUES (" + idValue + ", ?)", function (err, result) {
            assert.ok(!err, err);
            con.executePrepared(result.id, [paramValue], types.consistencies.one, function (err, result) {
              assert.ok(!err, err);
              con.execute("SELECT id, " + columnName + " FROM sampletable1 WHERE ID=" + idValue, function (err, result) {
                assert.ok(!err, err);
                assert.ok(result.rows && result.rows.length === 1, 'There must be a row');
                var newValue = compareFunc(result.rows[0].get(columnName));
                assert.strictEqual(newValue, compareFunc(columnValue), 'The value does not match: ' + newValue + ':' + compareFunc(columnValue));
                callback(err);
              });
            });
          });
        }
      }
      var toStringCompare = function (value) {  return value.toString(); };
      var toTimeCompare = function (value) {
        if (value instanceof Date) {
          return value.getTime();
        }
        if (value instanceof types.Long) {
          return Long.toBuffer(value).toString('hex');
        }
        return value;
      };
      var roundNumberCompare = function (digits) {
        return function toNumber(value) {
          return value.toFixed(digits);
        };
      };
      async.series([
        prepareInsertTest(300, 'text_sample', 'Señor Dexter', dataTypes.varchar),
        prepareInsertTest(301, 'text_sample', 'Morgan', dataTypes.ascii),
        prepareInsertTest(302, 'text_sample', null, dataTypes.varchar),
        prepareInsertTest(303, 'int_sample', 1500, dataTypes.int),
        prepareInsertTest(304, 'float_sample', 123.6, dataTypes.float, roundNumberCompare(1)),
        prepareInsertTest(305, 'float_sample', 123.001, dataTypes.float, roundNumberCompare(3)),
        prepareInsertTest(306, 'double_sample', 123.00123, dataTypes.double, roundNumberCompare(5)),
        prepareInsertTest(307, 'boolean_sample', true, dataTypes.boolean),
        //lists, sets and maps
        prepareInsertTest(310, 'list_sample', [1,2,80], dataTypes.list, toStringCompare),
        prepareInsertTest(311, 'set_sample', [1,2,80,81], dataTypes.set, toStringCompare),
        prepareInsertTest(312, 'list_sample', [], dataTypes.list, function (value) {
          if (value != null && value.length === 0) { 
            //empty sets and lists are stored as null values
            return null;
          }
          return value;
        }),
        prepareInsertTest(313, 'map_sample', {key1: "value1", key2: "value2"}, dataTypes.map, toStringCompare),
        prepareInsertTest(314, 'list_sample', [50,30,80], 'list<int>', toStringCompare),
        prepareInsertTest(315, 'set_sample', [1,5,80], 'set<int>', toStringCompare),
        prepareInsertTest(316, 'list_float_sample', [1,5], 'list<float>', util.inspect),
        //ip addresses
        prepareInsertTest(320, 'inet_sample', new Buffer([192,168,0,50]), dataTypes.inet, toStringCompare),
        //ip 6
        prepareInsertTest(321, 'inet_sample', new Buffer([1,0,0,0,1,0,0,0,1,0,0,0,192,168,0,50]), dataTypes.inet, toStringCompare),
        prepareInsertTest(330, 'big_sample', new types.Long(1010, 10), dataTypes.bigint, toStringCompare),
        prepareInsertTest(331, 'big_sample', 10, dataTypes.bigint, toStringCompare),
        prepareInsertTest(332, 'timestamp_sample', 1372753805600, dataTypes.timestamp, toTimeCompare),
        prepareInsertTest(333, 'timestamp_sample', new Date(2013,5,20,19,1,1,550), dataTypes.timestamp, toTimeCompare),
        prepareInsertTest(340, 'uuid_sample', types.uuid(), dataTypes.uuid, toStringCompare),
        prepareInsertTest(341, 'uuid_sample', types.timeuuid(), dataTypes.timeuuid, toStringCompare)
      ], function (err) {
        assert.ok(!err, err);
        done();
      });
    });

    it('should callback with err when param cannot be guessed', function (done) {
      var query = 'SELECT * FROM sampletable1 WHERE ID=?';
      var unspecifiedParam = {random: 'param'};
      con.execute(query, [unspecifiedParam], function (err) {
        assert.ok(err, 'An error must be yielded in the callback');
        prepareAndExecute(con, query, [unspecifiedParam], function (err) {
          assert.ok(err, 'An error must be yielded in the callback');
          done();
        });
      });
    });

    it('should callback per each row and at the end when rowCb is specified', function (done) {
      con.prepare('select * from system.schema_keyspaces', function (err, result) {
        assert.ok(!err, err);
        var rows = 0;
        con.executePrepared(result.id, [], types.consistencies.one, {
            byRow: true,
            rowCallback: function (n, row) {
              rows++;
            }
          }, function (err, rowCount) {
            assert.ok(rows > 0, 'it should callback per each row');
            done(err);
        });
      });
    });
  
    describe('Field streaming', function () {
      it('should stream the last column and be readable', function (done) {
        var blob = new Buffer(2*1024*1024);
        blob[0] = 245;
        var id = 400;
        insertAndStream(con, blob, id, true, function (err, row, stream) {
          assert.equal(row.get('id'), id);
          //test that stream is readable
          assertStreamReadable(stream, blob, done);
        });
      });
      
      it('should stream the last column and buffer if not read', function (done) {
        var blob = new Buffer(2048);
        blob[2047] = 0xFA;
        var id = 401;
        insertAndStream(con, blob, id, true, function (err, row, stream) {
          //delay some milliseconds the read of the stream and hope it's buffering
          setTimeout(function () {
            assertStreamReadable(stream, blob, done);
          }, 700);
        });
      });
      
      it('should return a null value when streaming a null field', function (done) {
        var id = 402;
        insertAndStream(con, null, id, true, function (err, row, stream) {
          assert.equal(stream, null, 'The stream must be null');
          done();
        });
      });
      
      it('should callback with an error when there is a bad input', function (done) {
        con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID = ?', function (err, result) {
          assert.ok(!err, err);
          con.executePrepared(result.id, ['BAD INPUT'], types.consistencies.one, {
              byRow: true,
              streamField: true,
              rowCallback: function (){
                assert.ok(false, 'row callback should not be called');
              }
            },
            function (err) {
              assert.ok(err, 'There must be an error returned, as the query is not valid.');
              done();
          });
        });
      });
        
      it('should callback when there is no match', function (done) {
        con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID = ?', function (err, result) {
          assert.ok(!err, err);
          con.executePrepared(result.id, [-1], types.consistencies.one, {
              byRow: true,
              streamField: true,
              rowCallback: function () {
                assert.ok(false, 'row callback should not be called');
              }
            }, done);
        });
      });

      it('should callback one time per row', function (done) {
        var values = [[410, new Buffer(1021)],[411, new Buffer(10*1024)],[412, new Buffer(4)],[413, new Buffer(256)]];
        values[1][1][0] = 100;
        function insert(item, callback) {
          con.execute('INSERT INTO sampletable1 (id, blob_sample) VALUES (?, ?)', item, callback);
        }

        function testInsertedValues() {
          var rowCounter = 0;
          var totalValues = values.length;
          var blobStreamArray = [];
          //index the values by id for easy access
          for (var i = 0; i < totalValues; i++) {
            values[values[i][0].toString()] = values[i][1];
          }
          con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID IN (?, ?, ?, ?)', function (err, result) {
            assert.ok(!err, err);
            con.executePrepared(result.id,
              [values[0][0], values[1][0], values[2][0], values[3][0]], types.consistencies.one, {
                byRow: true,
                streamField: true,
                rowCallback: function (n, row, stream) {
                  var originalBlob = values[row.get('id').toString()];
                  rowCounter++;
                  blobStreamArray.push([stream, originalBlob]);
                }
              },
              function (err, rowLength) {
                assert.ok(!err, err);
                assert.strictEqual(rowCounter, rowLength);
                assert.strictEqual(rowCounter, totalValues);
                async.mapSeries(blobStreamArray, function (item, next) {
                  assertStreamReadable(item[0], item[1], next);
                }, done);
              }
            );
          });
        }

        async.map(values, insert, function (err, results) {
          assert.ok(!err, err);
          testInsertedValues();
        });
      });

    });
  });

  describe('#executeBatch()', function () {
    it('should execute queries without params as an array of strings', function (done) {
      var queries = [
        "INSERT INTO sampletable1 (id, big_sample, float_sample, text_sample) VALUES " +
          "(451, 1, 1, 'one')",
        "INSERT INTO sampletable1 (id, big_sample, float_sample, text_sample) VALUES " +
          "(452, 2, 2, 'two')"
      ];
      con.executeBatch(queries, types.consistencies.one, null, function (err) {
        assert.ok(!err, err);
        con.execute('SELECT * FROM sampletable1 WHERE ID=452', function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows[0].text_sample, 'two');
          assert.strictEqual(result.rows[0].float_sample, 2);
          assert.strictEqual(result.rows[0].big_sample.toString(), '2');
          done();
        })
      });
    });

    it('should execute queries with bound params', function (done) {
      var queries = [
        {
          query: "INSERT INTO sampletable1 (id, big_sample, float_sample, text_sample) VALUES (?, ?, ?, ?)",
          params: [453, types.Long.fromNumber(3), {value: 3.3, hint: types.dataTypes.float}, 'three']
        },
        {
          query: "INSERT INTO sampletable1 (id) VALUES (454)"
          //no params
        }
      ];
      con.executeBatch(queries, types.consistencies.one, null, function (err) {
        assert.ok(!err, err);
        con.execute('SELECT * FROM sampletable1 WHERE ID=453', function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows[0].text_sample, 'three');
          assert.strictEqual(result.rows[0].float_sample.toFixed(1), '3.3');
          assert.strictEqual(result.rows[0].big_sample.toString(), '3');
          done();
        })
      });
    });

    it('should fail when no queries provided', function (done) {
      con.executeBatch(null, types.consistencies.one, null, function (err) {
        assert.ok(err, 'it should callback with err');
        assert.ok(!err.isServerUnhealthy, 'It should not contain the unhealthy flag');
        done();
      });
    });
  });

  describe('#stream()', function () {
    var queryId;
    before(function (done) {
      var insertQuery = 'INSERT INTO sampletable1 (id, text_sample) values (?, ?)';
      var selectQuery = 'SELECT * FROM sampletable1 where id IN (?, ?, ?, ?)';
      helper.batchInsert(
        con, insertQuery, [[500, '500-Z'], [501, '501-Z'], [502, '502-Z']], function (err) {
          if (err) done(err);
          con.prepare(selectQuery, function (err, result) {
            assert.ok(!err, err);
            queryId = result.id;
            done();
          });
        });
    });

    it('should stream rows', function (done) {
      var rows = [];
      var stream = con.stream(queryId, [500, 501, -1, -1], helper.throwop);
      stream
        .on('end', function () {
          assert.strictEqual(rows.length, 2);
          done();
        }).on('readable', function () {
          var row;
          while (row = this.read()) {
            assert.ok(row.get('id'), 'It should yield the id value');
            assert.strictEqual(row.get('id').toString()+'-Z', row.get('text_sample'),
              'The id and the text value should be related');
            rows.push(row);
          }
        }).on('error', done);
    });
  });
  
  describe('#close()', function () {
    it('should callback even if its already closed', function (done) {
      var localCon = new Connection(utils.extend({}, config));
      localCon.open(function (err) {
        assert.ok(!err, err);
        async.timesSeries(10, function (n, callback) {
          localCon.close(callback);
        }, done);
      });
    });
  });
  
  after(function (done) {
    con.close(done);
  });
});

function setRowsByKey(arr, key) {
  for (var i=0;i<arr.length;i++) {
  var row = arr[i];
  arr['key-' + row.get(key)] = row;
  }
  arr.get = function(key1) {
  return arr['key-' + key1];
  }
}

function insertAndStream(con, blob, id, streamField, callback) {
  con.execute('INSERT INTO sampletable1 (id, blob_sample) VALUES (?, ?)', [id, blob], function (err, result) {
    assert.ok(!err, err);
    con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE id = ?', function (err, result) {
      assert.ok(!err, err);
      con.executePrepared(result.id, [id], types.consistencies.one, {byRow: true, streamField: streamField}, callback);
    });
  });
}

function assertStreamReadable(stream, originalBlob, callback) {
  var length = 0;
  var firstByte = null;
  var lastByte = null;
  stream.on('readable', function () {
    var chunk = null;
    while (chunk = stream.read()) {
      length += chunk.length;
      if (firstByte === null) {
        firstByte = chunk[0];
      }
      if (length === originalBlob.length) {
        lastByte = chunk[chunk.length-1];
      }
    }
  });
  stream.on('end', function () {
    assert.strictEqual(length, originalBlob.length, 'The blob returned should be the same size ' + length + '<>' + originalBlob.length);
    assert.strictEqual(firstByte, originalBlob[0], 'The first byte of the stream and the blob dont match');
    assert.strictEqual(lastByte, originalBlob[originalBlob.length-1], 'The last byte of the stream and the blob dont match');
    callback();
  });
}

function prepareAndExecute(con, query, params, callback) {
  con.prepare(query, function (err, result) {
    if (err) return callback(err);
    con.executePrepared(result.id, params, callback);
  });
}

function compareRows(rowA, rowB, fieldList) {
  for (var i = 0; i < fieldList.length; i++) {
    var field = fieldList[i];
    assert.equal(util.inspect(rowA.get(field)), util.inspect(rowB.get(field)), '#' + rowA.get('id') + ' field ' + field + ' does not match');
  }
}