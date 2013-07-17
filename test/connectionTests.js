var async = require('async');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;
var types = require('../lib/types.js');
var keySpace = new types.QueryLiteral('unittestkp1_1');

var con = new Connection({host:'localhost', port: 9042, maxRequests:32});
//declaration order is execution order in nodeunit
module.exports = {
  connect: function (test) {
    con.on('log', function(type, message) {
      //console.log(type, message);
    });
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
    con.execute("DROP KEYSPACE ?;", [keySpace], function(err) {
      test.done();
    });
  },
  'create keyspace': function(test) {
    con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keySpace], function(err) {
      if (err) test.fail(err, 'Error creating keyspace');
      test.done();
    });
  },
  'use keyspace': function(test) {
    con.execute("USE ?;", [keySpace], function(err) {
      if (err) test.fail(err);
      test.done();
    });
  },
  'create types table': function(test) {
    con.execute(
      "CREATE TABLE sampletable1 (" +
        "id int PRIMARY KEY,            "+
        "big_sample bigint,             "+
        "blob_sample blob,             "+
        "decimal_sample decimal,        "+
        "list_sample list<int>,         "+
        "set_sample set<int>,           "+
        "map_sample map<text, text>,    "+
        "text_sample text);"
    , null, function(err) {
      if (err) test.fail(err, 'Error creating types table');
      test.done();
    });
  },
  'select null values': function(test) {
    con.execute('INSERT INTO sampletable1 (id) VALUES(1)', null, 
      function(err) {
        if (err) {
          test.fail(err, 'Error inserting just the key');
          test.done();
          return;
        }
        con.execute('select * from sampletable1 where id=?', [1], function (err, result) {
          if (err) test.fail(err, 'selecting null values failed');
          else {
            test.ok(result.rows.length === 1);
            test.ok(result.rows[0].get('big_sample') === null &&
              result.rows[0].get('blob_sample') === null &&
              result.rows[0].get('decimal_sample') === null &&
              result.rows[0].get('list_sample') === null &&
              result.rows[0].get('map_sample') === null &&
              result.rows[0].get('set_sample') === null &&
              result.rows[0].get('text_sample') === null);
          }
          test.done();
          return;
        });
    });
  },
  'execute malformed query': function(test) {
    con.execute("Malformed SLEECT SELECT * FROM sampletable1;", null, function(err) {
      if (err) {
        test.ok(!err.isServerUnhealthy, 'The connection should be reusable and the server should be healthy even if a query fails.');
      }
      else {
        test.fail('This query must yield an error.');
      }
      test.done();
    });
  },
  'bigInts': function(test) {
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
  'insert literals': function(test) {
    var insertQueries = [
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (200, 1, 0x48656c6c6f, 1, [1, 2, 3], {1, 2, 3}, {'a': 'value a', 'b': 'value b'}, '202');"],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (201, ?, ?, ?, ?, ?, ?, ?);", [1, new Buffer('Hello', 'utf-8'), 1, [1, 2, 3], {hint:'set', value: [1, 2, 3]}, {hint: 'map', value: {'a': 'value a', 'b': 'value b'}}, '201']],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (202, NULL, NULL, NULL, NULL, NULL, NULL, '202');"],
      ["INSERT INTO sampletable1 (id, big_sample, blob_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (203, ?, ?, ?, ?, ?, ?, ?);", [null, null, null, null, null, null, '203']]
    ];
    //TODO: Check that returns correctly if there is an error
    async.each(insertQueries, function(query, callback) {
      con.execute(query[0], query[1], function(err, result) {
        if (err) {
          test.fail(err);
        }
        callback();
      });
    }, function () {
      con.execute("select * from sampletable1 where id IN (200, 201, 202, 203);", null, function(err, result) {
        setRowsByKey(result.rows, 'id');
        var row0 = result.rows.get(200);
        var row1 = result.rows.get(201);
        var row2 = result.rows.get(202);
        var row3 = result.rows.get(203);
        //test that coming from parameters or hardcoded query, it stores and yields the same values
        test.ok(row0.get('big_sample') == 1 &&
          row0.get('blob_sample').toString('utf-8') === 'Hello' &&
          row0.get('list_sample').length === 3 &&
          row0.get('list_sample').indexOf(2) >= 0 &&
          row0.get('set_sample').length === 3 &&
          row0.get('set_sample').indexOf(2) >= 0 &&
          row0.get('map_sample').a === 'value a'
          , 'First row results does not match.');
        test.ok(row1.get('big_sample') == 1 &&
          row1.get('blob_sample').toString('utf-8') === 'Hello' &&
          row1.get('list_sample').length === 3 &&
          row1.get('list_sample').indexOf(2) >= 0 &&
          row1.get('set_sample').length === 3 &&
          row1.get('set_sample').indexOf(2) >= 0 &&
          row1.get('map_sample').a === 'value a' &&
          row1.get('text_sample') === '201'
          , 'Second row results does not match.');
        test.ok(row2.get('big_sample') == null &&
          row2.get('blob_sample') === null &&
          row2.get('list_sample') === null &&
          row2.get('list_sample') === null &&
          row2.get('set_sample') === null &&
          row2.get('set_sample') === null &&
          row2.get('map_sample') === null &&
          row2.get('text_sample') === '202'
          , 'Third row results does not match.');
        test.ok(row3.get('big_sample') == null &&
          row3.get('blob_sample') === null &&
          row3.get('list_sample') === null &&
          row3.get('list_sample') === null &&
          row3.get('set_sample') === null &&
          row3.get('set_sample') === null &&
          row3.get('map_sample') === null &&
          row3.get('text_sample') === '203'
          , 'Fourth row results does not match.');
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
  /**
   * Executes last, closes the connection
   */
  'disconnect': function (test) {
    test.ok(con.connected, 'Connection not connected to host.');
    con.close(function () {
      test.ok(!con.connected, 'The connected flag of the connection must be false.');
      //it should be allowed to be call close multiple times.
      con.close(function () {
        test.done();
      });
    });
  }
};
function setRowsByKey(arr, key) {
  for (var i=0;i<arr.length;i++) {
    var row = arr[i];
    arr['key-' + row.get(key)] = row;
  }
  arr.get = function(key1) {
    return arr['key-' + key1];
  }
}