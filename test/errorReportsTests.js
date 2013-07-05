var async = require('async');
var Connection = require('../index.js').Connection;
var types = require('../lib/types.js');
var keyspace = new types.QueryLiteral('unittestkp1_error_reports');

var con = new Connection({host:'localhost', port: 9042, maxRequests: 1});
//declaration order is execution order in nodeunit
module.exports = {
  connect: function (test) {
    con.on('log', function(type, message) {
      //console.log(type, message);
    });
   
    con.open(function (err) {
      if (err) test.fail(err);
      test.done();
    });
  },
  createKeyspace: function(test) {
    con.execute("DROP KEYSPACE ?;", [keyspace], function(err) {
      con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keyspace], function(err) {
        if (err) test.fail(err, 'Error creating keyspace');
        test.done();
      });
    });
  },
  createTable1: function(test) {
    var buffer = new Buffer(102400);
    buffer.write('Hello world', 'utf-8');
    con.execute("CREATE TABLE ?.filters (id text PRIMARY KEY,created timestamp,domain text,json text,protobuf blob,search_spec text,type text,updated timestamp);", 
      [keyspace], function (err) {
      if (err) {
        test.fail(err);
        test.done();
      }
      con.execute("INSERT INTO ?.filters (id, created, domain, json, protobuf, search_spec, type, updated) " + 
        "VALUES ('1', '2013-06-05', 'domain text', '{a:1}', 0x" + buffer.toString('hex') + ", 'hello', 'more text', '2013-06-25 05:03')"
        //"VALUES ('1', '2013-06-05', '{a:\'value a\'}', " + buffer.toString('hex') + ", 'hello', 'more text', '2013-06-25 05:03')"
        , [keyspace], function (err) {
        if (err) test.fail(err);
        test.done();
      });
    });
  },
  selectTable1: function (test) {
    con.on('log', function(type, message) {
      //console.log(type, message);
    });
    con.execute("select * from ?.filters LIMIT 300;", [keyspace], types.consistencies.one, function (err, result) {
      if (err) {
        test.fail(err, 'Error selecting');
      }
      else {
        test.equal(result.rows[0].get('protobuf').toString('utf-8').substring(0, 11), 'Hello world');
      }
      test.done();
    });
  },
  disconnect: function (test) {
    con.close(function() {
      test.done();
    });
  }
}
