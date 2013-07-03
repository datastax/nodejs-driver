var async = require('async');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;
var types = require('../lib/types.js');
var keySpace = new types.QueryLiteral('unittestkp1_1');

var con = new Connection({host:'localhost', port: 9042, maxRequests: 1});
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
  dropKeySpace: function(test) {
    con.execute("DROP KEYSPACE ?;", [keySpace], function(err) {
      test.done();
    });
  },
  createKeySpace: function(test) {
    con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keySpace], function(err) {
      if (err) test.fail(err, 'Error creating keyspace');
      test.done();
    });
  },
  useKeySpace: function(test) {
    con.execute("USE ?;", [keySpace], function(err) {
      if (err) test.fail(err);
      test.done();
    });
  },
  createTypesTable: function(test) {
    con.execute(
      "CREATE TABLE sampletable1 (" +
        "id int PRIMARY KEY,            "+
        "big_sample bigint,             "+
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
  selectNullValues: function(test) {
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
  malformedQueryExecute: function(test) {
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
  bigInts: function(test) {
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
  insertLiterals: function(test) {
    var queries = [
      ["INSERT INTO sampletable1 (id, big_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (200, 1, 1, [1, 2, 3], {1, 2, 3}, {'a': 'value a', 'b': 'value b'}, 'text sample');"],
      /*["INSERT INTO sampletable1 (id, big_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (201, NULL, NULL, NULL, NULL, NULL, NULL);"],*/
      ["INSERT INTO sampletable1 (id, big_sample, decimal_sample, list_sample, set_sample, map_sample, text_sample)" + 
      " values (202, ?, ?, ?, ?, ?, ?);", [1, 1, [1, 2, 3], {hint:'set', value: [1, 2, 3]}, {hint: 'map', value: {'a': 'value a', 'b': 'value b'}}, 'text sample']]
    ];
    //TODO: Check that returns correctly if there is an error
    async.each(queries, function(query, callback) {
      con.execute(query[0], query[1], function(err, result) {
        console.log('returned');
        if (err) {
          test.fail(err);
        }
        callback();
      });
    }, function () {
      test.done();
    });
  },
  /**
   * Executes last, closes the connection
   */
  disconnect : function (test) {
    test.ok(con.connected, 'Connection not connected to host.');
    con.close(function () {
      test.done();
    });
  }
};