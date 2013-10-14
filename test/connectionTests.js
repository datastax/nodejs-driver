var util = require('util');
var async = require('async');
var Int64 = require('node-int64');
var uuid = require('node-uuid');

var Connection = require('../index.js').Connection;
var types = require('../lib/types.js');
var utils = require('../lib/utils.js');
var dataTypes = types.dataTypes;
var keyspace = new types.QueryLiteral('unittestkp1_1');

var con = new Connection({host:'localhost', username: 'cassandra', password: 'cassandra', port: 9042, maxRequests:32});
//declaration order is execution order in nodeunit
module.exports = {
  connect: function (test) {
    //con.on('log', console.log);
    var originalHost = con.options.host;
    con.options.host = 'not-existent-host';
    con.open(function (err) {
      test.ok(err, 'Must return a connection error');
      con.options.host = originalHost;
      con.open(function (err) {
        if (err) test.fail(err);
        test.done();
      });
    });
  },
  'drop keyspace': function(test) {
    con.execute("DROP KEYSPACE ?;", [keyspace], function(err) {
      test.done();
    });
  },
  'create keyspace': function(test) {
    con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keyspace], function(err) {
      if (err) test.fail(err, 'Error creating keyspace');
      test.done();
    });
  },
  'use keyspace': function(test) {
    con.execute("USE ?;", [keyspace], function(err) {
      if (err) test.fail(err);
      test.done();
    });
  },
  'use keyspace error test': function (test) {
    var localCon = new Connection(utils.extend({}, con.options, {keyspace: 'this__keyspace__does__not__exist'}));
    localCon.open(function (err) {
      test.ok(err, 'An error must be returned as the keyspace does not exist');
      closeAndEnd(test, localCon);
    });
  },
  'execute params': function (test) {
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
    ],
    //all finished
    function(err){
      if (err) fail(test, err);
      test.done();
    });
  },
  'create types table': function(test) {
    con.execute(
      "CREATE TABLE sampletable1 (" +
        "id int PRIMARY KEY," +
        "big_sample bigint," +
        "timestamp_sample timestamp," +
        "blob_sample blob," +
        "decimal_sample decimal," +
        "float_sample float," +
        "uuid_sample uuid," +
        "boolean_sample boolean," +
        "double_sample double," +
        "list_sample list<int>," +
        "list_float_sample list<float>," +
        "set_sample set<int>," +
        "map_sample map<text, text>," +
        "int_sample int," +
        "inet_sample inet," +
        "text_sample text);"
    , null, function(err) {
      if (err) test.fail(err, 'Error creating types table');
      test.done();
    });
  },
  'select empty result set': function (test) {
    con.execute('SELECT * FROM sampletable1 WHERE ID = -1', function (err, result) {
      if (err) {
        test.fail(err, 'Error inserting just the key');
      }
      else {
        test.ok(result.rows, 'Rows must be defined');
        if (result.rows) test.ok(result.rows.length === 0, 'The length of the rows array must be zero');
      }
      test.done();
    });
  },
  'select null values': function(test) {
    con.execute('INSERT INTO sampletable1 (id) VALUES(1)', null, 
      function(err) {
        if (err) return fail(test, err);
        con.execute('select * from sampletable1 where id=?', [1], function (err, result) {
          if (err) test.fail(err, 'selecting null values failed');
          test.ok(result.rows.length === 1);
          test.ok(result.rows[0].get('big_sample') === null &&
            result.rows[0].get('blob_sample') === null &&
            result.rows[0].get('decimal_sample') === null &&
            result.rows[0].get('list_sample') === null &&
            result.rows[0].get('map_sample') === null &&
            result.rows[0].get('set_sample') === null &&
            result.rows[0].get('text_sample') === null);
          test.done();
          return;
        });
    });
  },
  'execute malformed query': function(test) {
    con.execute("Malformed SLEECT SELECT * FROM sampletable1;", null, function(err) {
      test.ok(err, 'This query must yield an error.');
      if (err) {
        test.ok(!err.isServerUnhealthy, 'The connection should be reusable and the server should be healthy even if a query fails.');
        test.ok(err.name === 'ResponseError', 'The error should be of type ResponseError');
      }
      test.done();
    });
  },
  'bigints': function(test) {
    var big = new Int64('123456789abcdef0');
    con.execute('INSERT INTO sampletable1 (id, big_sample) VALUES(100, ?)', 
      [big], 
      function(err) {
        if (err) {
          test.fail(err, 'Error inserting bigint');
          test.done();
          return;
        }
        con.execute('select id, big_sample from sampletable1 where id=100;', null, function (err, result) {
          if (err) test.fail(err);
          else {
            test.equal(big.toOctetString(), result.rows[0].get('big_sample').toOctetString(), 'Retrieved bigint does not match.');
          }
          test.done();
          return;
        });
    });
  },
  'uuids': function (test) {
    var uuidValue = uuid.v4();
    con.execute('INSERT INTO sampletable1 (id, uuid_sample) VALUES(150, ?)', [uuidValue], function (err, result) {
      test.ok(!err, 'There was an error inserting a uuid');
      if (err) {test.done();return;}
      con.execute('SELECT id, uuid_sample FROM sampletable1 WHERE ID=150', function (err, result) {
        test.ok(!err, 'There was an error retrieving a uuid');
        test.ok(result.rows[0].get('uuid_sample') === uuidValue, 'uuid values do not match');
        test.done();
      });
    });
  },
  'insert literals': function(test) {
    var insertQueries = [
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample)" + 
      " values (200, 1, 0x48656c6c6f, 1, [1, 2, 3], [1.1, 1, 1.02], {1, 2, 3}, {'a': 'value a', 'b': 'value b'}, 'abc');"],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample)" + 
      " values (201, ?, ?, ?, ?, ?, ?, ?, ?);", [1, new Buffer('Hello', 'utf-8'), 1, 
        [1, 2, 3], {hint: 'list<float>', value: [1.1, 1, 1.02]}, {hint: 'set<int>', value: [1, 2, 3]}, {hint: 'map', value: {'a': 'value a', 'b': 'value b'}}, 'abc']],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample)" + 
      " values (202, NULL, NULL, NULL, NULL, NULL, NULL, NULL, null);"],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample)" + 
      " values (203, ?, ?, ?, ?, ?, ?, ?, ?);", [null, null, null, null, null, null, null, null]]
    ];
    async.each(insertQueries, function(query, callback) {
      con.execute(query[0], query[1], function(err, result) {
        if (err) {
          err.query = query[0];
          test.fail(err);
        }
        callback(err);
      });
    }, function (err) {
      if (err) return fail(test, err);
      con.execute("select id, big_sample, blob_sample, decimal_sample, list_sample, list_float_sample, set_sample, map_sample, text_sample from sampletable1 where id IN (200, 201, 202, 203);", null, function(err, result) {
        if (err) {
          test.fail(err, 'Error selecting');
          test.done();
          return;
        }
        setRowsByKey(result.rows, 'id');
        var row0 = result.rows.get(200);
        var row1 = result.rows.get(201);
        var row2 = result.rows.get(202);
        var row3 = result.rows.get(203);
        
        function compareFields(test, rowA, rowB, fieldList) {
          for (var i = 0; i < fieldList.length; i++) {
            try{
            var field = fieldList[i];
            test.equal(util.inspect(rowA.get(field)), util.inspect(rowB.get(field)), '#' + rowA.get('id') + ' field ' + field + ' does not match');
            }catch (e) {console.error(e);}
          }
        }
        //test that coming from parameters or hardcoded query, it stores and yields the same values
        compareFields(test, row0, row1, ['big_sample', 'blob_sample', 'decimal_sample', 'list_sample', 'list_float_sample', 'set_sample', 'map_sample', 'text_sample']);
        compareFields(test, row2, row3, ['big_sample', 'blob_sample', 'decimal_sample', 'list_sample', 'list_float_sample', 'set_sample', 'map_sample', 'text_sample']);
        test.done();
      });
    });
  },
  'execute multiple queries': function (test) {
    var callbackCounter = 0;
    var totalTimes = 15;
    async.times(totalTimes, function() {
      con.execute('SELECT * FROM sampletable1', [], function (err, result) {
        if (err) test.fail(err);
        callbackCounter++;
        if (callbackCounter === totalTimes) {
          test.done();
        }
      });
    });
  },
  'prepare query': function (test) {
    async.series([
      function (callback) {
        con.prepare("select id, big_sample from sampletable1 where id = ?", function (err, result) {
          callback(err, result.id);
        });
      }, 
      function (callback) {
        con.prepare("select id, big_sample from sampletable1 where id = ?", function (err, result) {
          callback(err, result.id);
        });
      }, 
      function (callback) {
        con.prepare("select id, big_sample, map_sample from sampletable1 where id = ?", function (err, result) {
          callback(err, result.id);
        });
      },
    ], function (err, ids) {
      if (err) test.fail(err);
      test.ok(ids[0].toString('hex').length > 0);
      test.ok(ids[0].toString('hex') === ids[1].toString('hex'), 'Ids to same queries should be the same');
      test.ok(ids[1].toString('hex') !== ids[2].toString('hex'), 'Ids to different queries should be different');
      test.done();
    });
  },
  'execute prepared queries': function (test) {
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
          if (err) {console.error(err);callback(err); return;}
          con.executePrepared(result.id, [paramValue], types.consistencies.quorum, function (err, result) {
            if (err) {console.error(err);callback(err); return;}
            con.execute("SELECT id, " + columnName + " FROM sampletable1 WHERE ID=" + idValue, function (err, result) {
              if (err) {console.error(err);callback(err); return}
              test.ok(result.rows.length === 1, 'There must be a row');
              var newValue = compareFunc(result.rows[0].get(columnName));
              //console.log(columnValue, newValue + ':' + compareFunc(columnValue));
              test.ok(newValue === compareFunc(columnValue), 'The value does not match: ' + newValue + ':' + compareFunc(columnValue));
              callback(err);
            });
          });
        });
      }
    };
    var toStringCompare = function (value) { 
      return value.toString();
    };
    var toTimeCompare = function (value) {
      if (value instanceof Date) {
        return value.getTime();
      }
      if (value instanceof Int64) {
        return value.valueOf();
      }
      return value;
    };
    var roundNumberCompare = function (digits) {
      return function toNumber(value) 
      {
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
        if (value != null && value.length === 0) 
          //empty sets and lists are stored as null values
          return null;
        return value;
      }),
      prepareInsertTest(313, 'map_sample', {key1: "value1", key2: "value2",}, dataTypes.map, toStringCompare),
      prepareInsertTest(314, 'list_sample', [50,30,80], 'list<int>', toStringCompare),
      prepareInsertTest(315, 'set_sample', [1,5,80], 'set<int>', toStringCompare),
      prepareInsertTest(316, 'list_float_sample', [1,5], 'list<float>', util.inspect),
      //ip addresses
      prepareInsertTest(320, 'inet_sample', new Buffer([192,168,0,50]), dataTypes.inet, toStringCompare),
      //ip 6
      prepareInsertTest(321, 'inet_sample', new Buffer([1,0,0,0,1,0,0,0,1,0,0,0,192,168,0,50]), dataTypes.inet, toStringCompare),
      prepareInsertTest(330, 'big_sample', new Int64(1010, 10), dataTypes.bigint, toStringCompare),
      prepareInsertTest(331, 'big_sample', 10, dataTypes.bigint, toStringCompare),
      prepareInsertTest(332, 'timestamp_sample', 1372753805600, dataTypes.timestamp, toTimeCompare),
      prepareInsertTest(333, 'timestamp_sample', new Date(2013,5,20,19,01,01,550), dataTypes.timestamp, toTimeCompare),
      prepareInsertTest(340, 'uuid_sample', uuid.v4(), dataTypes.uuid, toStringCompare),
      prepareInsertTest(341, 'uuid_sample', uuid.v1(), dataTypes.timeuuid, toStringCompare)
    ], function (err) {
      if (err) test.fail(err);
      test.done();
    });
  },
  'consume all streamIds': function (test) {
    //tests that max streamId is reached and the connection waits for a free id
    var options = utils.extend({}, con.options, {maxRequests: 10, maxRequestsRetry: 0});
    //total amount of queries to issue
    var totalQueries = 50;
    var timeoutId;
    var localCon = new Connection(options);
    localCon.open(function (err) {
      if (err) return fail(test, err);
      timeoutId = setTimeout(timePassed, 20000);
      for (var i=0; i<totalQueries; i++) {
        localCon.execute('SELECT * FROM ?.sampletable1 WHERE ID IN (?, ?, ?);', [keyspace, 1, 100, 200], selectCallback);
      }
    });
    var counter = 0;
    function selectCallback(err, result) {
      counter++;
      if (err) return fail(test, err);
      if (counter === totalQueries) {
        try{
        clearTimeout(timeoutId);
        closeAndEnd(test, localCon);
        }
        catch (e) {console.error(e)}
      }
    }
    
    function timePassed() {
      test.fail('Timeout: all callbacks havent been executed');
      closeAndEnd(test, localCon);
    }
  },
  'streaming column': function (test) {
    var blob = new Buffer(1024*1024);
    var id = 400;
    insertAndStream(test, con, blob, id, true, function (err, row, stream) {
      test.equal(row.get('id'), id);
      //test that stream is readable
      assertStreamReadable(test, stream, blob);
    });
  },
  'streaming delayed read': function (test) {
    var blob = new Buffer(2048);
    blob[2047] = 0xFA;
    var id = 401;
    insertAndStream(test, con, blob, id, true, function (err, row, stream) {
      //delay some milliseconds the read of the stream and hope it's buffering
      setTimeout(function () {
        assertStreamReadable(test, stream, blob);
      }, 700);
    });
  },
  'streaming null value': function (test) {
    var id = 402;
    insertAndStream(test, con, null, id, true, function (err, row, stream) {
      test.equal(stream, null, 'The stream must be null');
      test.done();
    });
  },
  'streaming error test': function (test) {
    con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID = ?', function (err, result) {
      if (err) return fail(test, err);
      con.executePrepared(result.id, ['BAD INPUT'], types.consistencies.one, {streamRows: true, streamField: true}, 
          function (err, row, stream) {
            test.ok(err, 'There must be an error returned, as the query is not valid.');
            test.ok(!row && !stream, 'There must not be a row nor stream returned');
            test.done();
      });
    });
  },
  'streaming empty result set': function (test) {
    con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID = ?', function (err, result) {
      if (err) return fail(test, err);
      con.executePrepared(result.id, [-1], types.consistencies.one, {streamRows: true, streamField: true},
          function (err, row, stream) {
            test.ok(!err && !row && !stream, 'There must be no error, row nor stream yielded by the query');
            test.done();
      });
    });
  },
  'streaming just rows': function (test) {
    var id = 403;
    var blob = new Buffer('Hello');
    insertAndStream(test, con, blob, id, false, function (err, row, stream) {
      test.equal(stream, null, 'The stream must be null');
      test.ok(row && row.get('blob_sample') && row.get('blob_sample').toString() === blob.toString(), 'The blob should be returned in the row.')
      test.done();
    });
  },
  'streaming multiple rows': function (test) {
    var values = [[410, new Buffer(1021)],[411, new Buffer(2048*1024)],[412, new Buffer(4)],[413, new Buffer(256)]];
    
    function insert(item, callback) {
      con.execute('INSERT INTO sampletable1 (id, blob_sample) VALUES (?, ?)', item, callback);
    }
    
    function testInsertedValues() {
      var counter = 0;
      var totalValues = values.length;
      //index the values by id for easy access
      for (var i = 0; i < totalValues; i++) {
        values[values[i][0].toString()] = values[i][1];
      }
      con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE ID IN (?, ?, ?, ?)', function (err, result) {
        if (err) return fail(test, err);
        con.executePrepared(result.id, 
          [values[0][0], values[1][0], values[2][0], values[3][0]], types.consistencies.one, {streamRows: true, streamField: true},
          function (err, row, stream) {
            if (err) return fail(test, err);
            var originalBlob = values[row.get('id').toString()];
            assertStreamReadable(test, stream, originalBlob, function () {
              counter++
              if (counter == totalValues) {
                test.done();
              }
            });
          }
        );
      });
    }
    
    async.map(values, insert, function (err, results) {
      if (err) return fail(test, err);
      testInsertedValues();
    });
  },
  /**
   * Executes last, closes the connection
   */
  'disconnect': function (test) {
    test.ok(con.connected, 'Connection not connected to host.');
    con.close(function () {
      test.ok(!con.connected, 'The connected flag of the connection must be false.');
      //it should be allowed to be call close multiple times.
      closeAndEnd(test, con);
    });
  }
};
function closeAndEnd(test, con) {
  con.close(function () {
    test.done();
  });
}

function setRowsByKey(arr, key) {
  for (var i=0;i<arr.length;i++) {
    var row = arr[i];
    arr['key-' + row.get(key)] = row;
  }
  arr.get = function(key1) {
    return arr['key-' + key1];
  }
}

function fail(test, err, con) {
  test.fail(err);
  if (con) {
    closeAndEnd(test, con);
  }
  else {
    test.done();
  }
}

function insertAndStream(test, con, blob, id, streamField, callback) {
  con.execute('INSERT INTO sampletable1 (id, blob_sample) VALUES (?, ?)', [id, blob], function (err, result) {
    if (err) return fail(test, err);
    con.prepare('SELECT id, blob_sample FROM sampletable1 WHERE id = ?', function (err, result) {
      if (err) return fail(test, err);
      con.executePrepared(result.id, [id], types.consistencies.one, {streamRows: true, streamField: streamField}, callback);
    });
  });
}

function assertStreamReadable(test, stream, originalBlob, callback) {
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
    test.equal(length, originalBlob.length, 'The blob returned should be the same size');
    test.equal(firstByte, originalBlob[0], 'The first byte of the stream and the blob dont match');
    test.equal(lastByte, originalBlob[originalBlob.length-1], 'The last byte of the stream and the blob dont match');
    if (!callback) {
      callback = test.done;
    }
    callback();
  });
}