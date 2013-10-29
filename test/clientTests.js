var async = require('async');
var util = require('util');
var Client = require('../index.js').Client;
var Connection = require('../index.js').Connection;
var utils = require('../lib/utils.js');
var types = require('../lib/types.js');
var config = require('./config.js');
var keyspace = new types.QueryLiteral('unittestkp1_2');

var client = null;
var clientOptions = {
  hosts: [config.host + ':' + config.port.toString(), config.host2 + ':' + config.port.toString()], 
  username: config.username, 
  password: config.password, 
  keyspace: keyspace
};
module.exports = {
  'setup keyspace': function(test) {
    setup(function () {
      client = new Client(clientOptions);
      createTable();
    });
    
    //recreates a keyspace, using a connection object
    function setup(callback) {
      var con = new Connection(utils.extend({}, config));
      con.open(function (err) {
        if (err) {
          con.close(function () {
            fail(test, err);
          });
        }
        else {
          con.execute("DROP KEYSPACE ?;", [keyspace], function () {
            createKeyspace(con, callback);
          });
        }
      });
    }
    
    function createKeyspace(con, callback) {
      con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keyspace], function (err) {
        if (err) {
          con.close(function () {
            fail(test, err);
          });
        }
        con.close(function () {
          callback();
        });
      });
    }
    
    function createTable() {
      client.execute(
        "CREATE TABLE sampletable2 (" +
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
        "set_sample set<int>," +
        "map_sample map<text, text>," +
        "int_sample int," +
        "inet_sample inet," +
        "text_sample text);", [], function (err) {
          if (err) return fail(test, err);
          test.done();
      });
    }
  },
  'execute params': function (test) {
    async.series([
      function (callback) {
        //all params
        client.execute('SELECT * FROM system.schema_keyspaces', [], types.consistencies.one, callback);
      },
      function (callback) {
        //no consistency specified
        client.execute('SELECT * FROM system.schema_keyspaces', [], callback);
      },
      function (callback) {
        //change the meaning of the second parameter to consistency
        client.execute('SELECT * FROM system.schema_keyspaces', types.consistencies.one, callback);
      },
      function (callback) {
        //query params but no params args, consistency specified, must fail
        client.execute('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', types.consistencies.one, function(err, result){
          if (!err) {
            callback(new Error('Consistency should not be treated as query parameters'));
          }
          else {
            callback(null, result);
          }
        });
      },
      function (callback) {
        //no query params
        client.execute('SELECT * FROM system.schema_keyspaces', function(err) {
          callback(err);
        });
      }
    ],
    //all finished
    function(err, results){
      test.ok(err === null, err);
      test.ok(results[1], 'The result of a query must not be null nor undefined');
      test.done();
    });
  },
  'response error test': function (test) {
    client.execute('SELECT WHERE Fail Miserable', function (err, result) {
      test.ok(err, 'Error must be defined and not null');
      test.done();
    });
  },
  'keyspace does not exist error test': function (test) {
    var localClient = getANewClient({keyspace: 'this__keyspace__does__not__exist'});
    localClient.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
      test.ok(err, 'It should return an error as the keyspace does not exist');
      shutDownEnd(test, localClient);
    });
  },
  /**
   * Test that it is possible to call connect multiple times in parallel.
   */
  'multiple connect calls': function (test) {
    var localClient = getANewClient();
    async.times(5, function (n, next) {
      localClient.connect(next);
    }, function (err) {
      if (err) return fail(test, err, localClient);
      shutDownEnd(test, localClient);
    });
  },
  'max execute retries': function (test) {
    var localClient = getANewClient();
    //Only 1 retry
    localClient.options.maxExecuteRetries = 1;
    localClient.options.getAConnectionTimeout = 300;
    //Change the behaviour so every err is a "server error"
    localClient._isServerUnhealthy = function (err) {
      return true;
    };

    localClient.execute('WILL FAIL AND EXECUTE THE METHOD FROM ABOVE', function (err, result, retryCount){
      test.ok(err, 'The execution must fail');
      test.equal(retryCount, localClient.options.maxExecuteRetries, 'It must retry executing the times specified ' + retryCount);
      shutDownEnd(test, localClient);
    });
  },
  'no initial connection callback': function (test) {
    var localClient = getANewClient({hosts: ['localhost:8080', 'localhost:8080']});
    var errors = [];
    async.series([function (callback){
      localClient.execute('badabing', function (err) {
        if (err) {
          errors.push(err);
          callback();
        }
      });
    }, function (callback){
      localClient.execute('badabang', function (err) {
        if (err) {
          errors.push(err);
        }
        callback();
      });
    }], function () {
      test.ok(errors.length === 2, 'There wasnt any good connection, it must callback with an err each time trying to execute');
      if (errors.length == 2) {
        test.ok(errors[0].name == 'PoolConnectionError', 'Errors should be of type PoolConnectionError');
      }
      shutDownEnd(test, localClient);
    });
  },
  'get a connection timeout': function (test) {
    var localClient = getANewClient();
    //wait for short amount of time
    localClient.options.getAConnectionTimeout = 200;
    //mark all connections as unhealthy
    localClient._isHealthy = function() {
      return false;
    };
    //disallow reconnection
    localClient._canReconnect = localClient._isHealthy;
    localClient.execute('badabing', function (err) {
      test.ok(err, 'Callback must return an error');
      test.ok(err.name === 'TimeoutError', 'The error must be a TimeoutError');
      shutDownEnd(test, localClient);
    });
  },
  'get a connection to prepared queries': function (test) {
    var localClient = getANewClient();
    var getAConnectionFlag = false;
    localClient.getAConnection = function(callback) {
      getAConnectionFlag = true;
      callback(null, null);
    };
    
    //start test flow
    testIndexes();

    function testIndexes() {
      async.series([localClient.getConnectionToPrepare.bind(localClient), 
        localClient.getConnectionToPrepare.bind(localClient),
        localClient.getConnectionToPrepare.bind(localClient),
        localClient.getConnectionToPrepare.bind(localClient)]
      , function (err, result) {
        test.ok(!err);
        test.ok(result.length == 4, 'There must be 4 connections returned');
        test.ok(result[0].indexInPool !== result[1].indexInPool, 'There must be 2 connections with different indexes');
        test.ok(result[0].indexInPool + result[1].indexInPool + result[2].indexInPool + result[3].indexInPool == 2, 'There should be one with the index 0 and the other with 1');
        testNotToGetAConnection();
      });
    }
    
    function testNotToGetAConnection() {
      localClient.getConnectionToPrepare(function (err, c) {
        test.ok(!getAConnectionFlag, 'Get a connection must not be called');
        testGetAConnection();
      });
    }
    
    function testGetAConnection() {
      localClient.unhealtyConnections.length = 100;
      localClient.getConnectionToPrepare(function (err, c) {
        test.ok(getAConnectionFlag, 'Get a connection must be called in this case');
        shutDownEnd(test, localClient);
      });
    }
  },
  'execute prepared': function (test) {
    client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], 
      types.consistencies.one, 
      function (err, result) {
        if (err) {
          test.fail(err);
          test.done();
          return;
        }
        test.ok(result.rows.length == 1, 'There should be a row');
        test.done();
    });
  },
  'execute prepared will retry': function (test) {
    var localClient = getANewClient();
    var counter = 0;
    localClient._isServerUnhealthy = function() {
      //change the behaviour set it to unhealthy the first time
      if (counter == 0) {
        counter++;
        return true
      }
      return false;
    };
    
    localClient.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], 
      types.consistencies.one, 
      function (err, result) {
        if (err) {
          test.fail(err);
          shutDownEnd(test, localClient);
          return;
        }
        test.ok(result.rows.length == 1, 'There should be one row');
        shutDownEnd(test, localClient);
    });
  },
  'executeAsPrepared parameters': function (test) {
    async.series([
      function (callback) {
        //all params
        client.executeAsPrepared('SELECT * FROM system.schema_keyspaces', [], types.consistencies.one, callback);
      },
      function (callback) {
        //no consistency specified
        client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], callback);
      },
      function (callback) {
        //change the meaning of the second parameter to consistency
        client.executeAsPrepared('SELECT * FROM system.schema_keyspaces', types.consistencies.one, callback);
      },
      function (callback) {
        //query params but no params args, consistency specified, must fail
        client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', types.consistencies.one, function(err, result){
          if (!err) {
            callback(new Error('Consistency should not be treated as query parameters'));
          }
          else {
            callback(null, result);
          }
        });
      },
      function (callback) {
        //no query params
        client.executeAsPrepared('SELECT * FROM system.schema_keyspaces', function(err, result) {
          if (!result || !result.rows) {
            test.fail(result, 'Expected rows');
          }
          else {
            test.ok(result.rows.length > 0, 'There should at least a row returned');
          }
          callback(err, result);
        });
      }
    ],
    //all finished
    function(err, results){
      test.ok(!err, err);
      test.ok(results, 'The result of the queries must not be null nor undefined');
      test.done();
    });
  },
  'socket error test': function (test) {
    //the client must reconnect and continue
    var localClient = getANewClient();
    if (localClient.connections.length < 2) {
      test.fail('The test requires 2 or more connections.');
      return test.done();
    }
    async.timesSeries(15, function (n, next) {
      if (n == 2) {
        //The next write attempt will fail for this connection.
        localClient.connections[0].netClient.destroy();
      }
      else if (n == 6) {
        //Force to no more IO on these socket.
        localClient.connections[0].netClient.destroy();
        localClient.connections[1].netClient.destroy();
      }
      else if (n == 9 && localClient.connections.length > 1) {
        //Force to no more IO on this socket. The next write attempt will fail
        localClient.connections[0].netClient.end();
        localClient.connections[1].netClient.end();
      }
      localClient.execute('SELECT * FROM system.schema_keyspaces', function (err) {
        next(err);
      })
    }, function (err) {
      test.ok(!err, 'There must not be an error returned. It must retried.');
      shutDownEnd(test, localClient);
    });
  },
  'shutdown': function (test) {
    shutDownEnd(test, client);
  }
}

function shutDownEnd(test, client, callback) {
  client.shutdown(function(){
    test.done();
    if (callback) {
      callback();
    }
  });
}

function fail (test, err, client) {
  test.fail(err);
  if (client) {
    shutDownEnd(test, client);
  }
  else {
    test.done();
  }
}

function getANewClient (options) {
  if (!options) {
    options = {};
  }
  return new Client(utils.extend({}, clientOptions, options));
}