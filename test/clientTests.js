var async = require('async');
var util = require('util');
var Client = require('../index.js').Client;
var Connection = require('../index.js').Connection;
var utils = require('../lib/utils.js');
var types = require('../lib/types.js');
var config = require('./config.js');
var keyspace = new types.QueryLiteral('unittestkp1_2');
types.consistencies.getDefault = function () {return this.one};

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
      con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '3'};", [keyspace], function (err) {
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
  'pool size test': function (test) {
    var poolSize = 3;
    var localClient = getANewClient({hosts: ['host1', 'host2', 'host3'], poolSize: poolSize});
    var connections = localClient.connections;
    //The pool should created like [conHost1, conHost2, conHost3, conHost1, conHost2, conHost3, conHost1, ...]
    test.ok(connections.length, 9, 'There must be 9 connections (amount hosts * pool size)');
    for (var i = 0; i < poolSize; i++) {
      test.ok(
        connections[0 + (i * poolSize)].options.host === 'host1' &&
        connections[1 + (i * poolSize)].options.host === 'host2' &&
        connections[2 + (i * poolSize)].options.host === 'host3', 'The connections inside the pool are not correctly ordered');
    }
    test.done();
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
  'execute prepared': function (test) {
    client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], 
      types.consistencies.one, 
      function (err, result) {
        if (err) {
          fail(test, err);
          return;
        }
        test.ok(result.rows.length == 1, 'There should be a row');
        test.done();
    });
  },
  'execute prepared will retry': function (test) {
    var localClient = getANewClient({maxExecuteRetries: 3, staleTime: 150});
    var counter = -1;
    localClient._isServerUnhealthy = function() {
      //change the behaviour set it to unhealthy the first time
      if (counter < 2) {
        counter++;
        return true
      }
      return false;
    };
    
    localClient.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], 
      types.consistencies.one, 
      function (err, result) {
        if (err) {
          fail(test, err, localClient);
          return;
        }
        test.ok(result.rows.length == 1, 'There should be one row');
        shutDownEnd(test, localClient);
    });
  },
  'execute prepared stops retrying': function (test) {
    var localClient = getANewClient({maxExecuteRetries: 4, staleTime: 150});
    var counter = -1;
    localClient._isServerUnhealthy = function() {
      //change the behaviour set it to unhealthy the first time
      counter++;
      return true;
    };
    
    localClient.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace.toString()], 
      types.consistencies.one, 
      function (err, result) {
        test.ok(!result, 'It must stop retrying and callback without a result');
        test.equal(counter, localClient.options.maxExecuteRetries, 'It must retry an specific amount of times');
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
  'socket error serial test': function (test) {
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
  'socket error parallel test': function (test) {
    //the client must reconnect and continue
    var localClient = getANewClient();
    if (localClient.connections.length < 2) {
      test.fail('The test requires 2 or more connections.');
      return test.done();
    }
    async.times(10, function (n, next) {
      localClient.execute('SELECT * FROM system.schema_keyspaces', function (err) {
        if (n === 3) {
          localClient.connections[0].netClient.destroy();
          localClient.connections[1].netClient.destroy();
        }
        next(err);
      })
    }, function (err) {
      test.ok(!err, 'There must not be an error returned. It must retried.');
      shutDownEnd(test, localClient);
    });
  },
  'streaming just rows': function (test) {
    var id = 100;
    var blob = new Buffer('Frank Gallagher');
    insertAndStream(test, client, blob, id, false, function (err, row, stream) {
      if (err) fail(test, err);
      test.equal(stream, null, 'The stream must be null');
      test.ok(row && row.get('blob_sample') && row.get('blob_sample').toString() === blob.toString(), 'The blob should be returned in the row.')
      test.done();
    });
  },
  'streaming field': function (test) {
    var id = 110;
    var blob = new Buffer('Freaks and geeks 1999');
    insertAndStream(test, client, blob, id, true, function (err, row, blobStream) {
      if (err) fail(test, err);
      assertStreamReadable(test, blobStream, blob);
    });
  },
  'streaming null field': function (test) {
    var id = 120;
    var blob = null;
    insertAndStream(test, client, blob, id, true, function (err, row, blobStream) {
      if (err) fail(test, err);
      test.equal(row.get('id'), id, 'The row must be retrieved');
      test.ok(blobStream === null, 'The file stream must be NULL');
      test.done();
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

function fail (test, err, localClient) {
  test.fail(err);
  if (localClient) {
    shutDownEnd(test, localClient);
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

function insertAndStream (test, client, blob, id, streamField, callback) {
  var streamFunction = client.streamRows;
  if (streamField) {
    streamFunction = client.streamField;
  }
  client.execute('INSERT INTO sampletable2 (id, blob_sample) VALUES (?, ?)', [id, blob], function (err, result) {
    if (err) return fail(test, err);
    streamFunction.call(client, 'SELECT id, blob_sample FROM sampletable2 WHERE id = ?', [id], callback);
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