var async = require('async');
var util = require('util');
var Client = require('../index.js').Client;
var types = require('../lib/types.js');
var keyspace = new types.QueryLiteral('unittestkp1_2');

var client = null;

module.exports = {
  'setup keyspace': function(test) {
    client = new Client({hosts: ['localhost:9042', 'localhost:9042']});
    client.execute("DROP KEYSPACE ?;", [keyspace], function () {
      client.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keyspace], function (err) {
        if (err) {
          test.fail(err);
          test.done();
          return;
        }
        client.execute("USE ?;", [keyspace], function (err) {
          console.log('using keyspace');
          if (err) test.fail(err);
          test.done();
        });
      });
    });
  },
  'execute params': function (test) {
    async.series([
      function (callback) {
        //all params
        client.execute('SELECT * FROM system.schema_keyspaces', [], types.consistencies.one, function(err){
          callback(err);
        });
      }/*,
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
      }*/
    ],
    //all finished
    function(err){
      test.ok(err === null, err);
      test.done();
    });
  },
  'shutdown': function(test) {
    client.shutdown(function(){
      test.done();
    });
  }
}