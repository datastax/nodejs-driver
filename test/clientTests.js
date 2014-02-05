var assert = require('assert');
var async = require('async');
var util = require('util');
var Client = require('../index.js').Client;
var Connection = require('../index.js').Connection;
var utils = require('../lib/utils.js');
var types = require('../lib/types.js');
var config = require('./config.js');
var helper = require('./testHelper.js');
var keyspace = 'unittestkp1_2';
types.consistencies.getDefault = function () { return this.one; };

var client = null;
var clientOptions = {
  hosts: [config.host + ':' + config.port.toString(), config.host2 + ':' + config.port.toString()], 
  username: config.username, 
  password: config.password, 
  keyspace: keyspace
};

describe('Client', function () {
  before(function (done) {
    setup(function () {
      client = new Client(clientOptions);
      createTable();
    });
    
    //recreates a keyspace, using a connection object
    function setup(callback) {
      var con = new Connection(utils.extend({}, config));
      con.open(function (err) {
        if (err) throw err;
        else {
          var query = util.format("DROP KEYSPACE %s;", keyspace)
          con.execute(query, function () {
            createKeyspace(con, callback);
          });
        }
      });
    }
    
    function createKeyspace(con, callback) {
      var query = util.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy','replication_factor': '3'};", keyspace);
      con.execute(query, function (err) {
        if (err) throw err;
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
          if (err) throw err;
          done();
      });
    }
  });
  
  describe('constructor', function () {
    it('should create the connection pool', function () {
      //The pool should be created like [conHost1, conHost2, conHost3, conHost1, conHost2, conHost3, conHost1, ...]
      var poolSize = 3;
      var localClient = getANewClient({hosts: ['host1', 'host2', 'host3'], poolSize: poolSize});
      var connections = localClient.connections;
      assert.ok(connections.length, 9, 'There must be 9 connections (amount hosts * pool size)');
      for (var i = 0; i < poolSize; i++) {
        assert.ok(
          connections[0 + (i * poolSize)].options.host === 'host1' &&
          connections[1 + (i * poolSize)].options.host === 'host2' &&
          connections[2 + (i * poolSize)].options.host === 'host3', 'The connections inside the pool are not correctly ordered');
      }
    });
  });
  
  describe('#connect()', function () {
    it('possible to call connect multiple times in parallel', function (done) {
      var localClient = getANewClient();
      async.times(5, function (n, next) {
        localClient.connect(next);
      }, function (err) {
        assert.ok(!err, err);
        localClient.shutdown(done);
      });
    });
  });
  
  describe('#execute()', function () {
    it('should allow different argument lengths', function (done) {
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
        assert.strictEqual(err, null, err);
        assert.ok(results[1], 'The result of a query must not be null nor undefined');
        done();
      });
    });
    
    it('should callback with error for Syntax error in the query', function (done) {
      client.execute('SELECT WHERE Fail Miserable', function (err, result) {
        assert.ok(err, 'Error must be defined and not null');
        done();
      });
    });
    
    it('should retry in case the node is down', function (done) {
      var localClient = getANewClient();
      //Only 1 retry
      localClient.options.maxExecuteRetries = 1;
      localClient.options.getAConnectionTimeout = 300;
      //Change the behaviour so every err is a "server error"
      localClient._isServerUnhealthy = function (err) {
        return true;
      };

      localClient.execute('WILL FAIL AND EXECUTE THE METHOD FROM ABOVE', function (err, result, retryCount){
        assert.ok(err, 'The execution must fail');
        assert.equal(retryCount, localClient.options.maxExecuteRetries, 'It must retry executing the times specified ' + retryCount);
        localClient.shutdown(done);
      });
    });
    
    it('should callback after timeout passed', function (done) {
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
        assert.ok(err, 'Callback must return an error');
        assert.strictEqual(err.name, 'TimeoutError', 'The error must be a TimeoutError');
        localClient.shutdown(done);
      });
    });
    
    it('should callback when the keyspace does not exist', function (done) {
      var localClient = getANewClient({keyspace: 'this__keyspace__does__not__exist'});
      localClient.execute('SELECT * FROM system.schema_keyspaces', function (err, result) {
        assert.ok(err, 'It should return an error as the keyspace does not exist');
        localClient.shutdown(done);
      });
    });
    
    it('should callback when it was not possible to connect to any host', function (done) {
      var localClient = getANewClient({hosts: ['localhost:8080', 'localhost:8080']});
      var errors = [];
      async.series([
        function (callback){
          localClient.execute('badabing', function (err) {
            if (err) {
              errors.push(err);
              callback();
            }
          });
        }, 
        function (callback){
          localClient.execute('badabang', function (err) {
            if (err) {
              errors.push(err);
            }
            callback();
          });
        }
      ],
      function () {
        assert.strictEqual(errors.length, 2, 'It must callback with an err each time trying to execute');
        if (errors.length === 2) {
          assert.strictEqual(errors[0].name, 'PoolConnectionError', 'Errors should be of type PoolConnectionError');
        }
        localClient.shutdown(done);
      });
    });
  });
  
  describe('#executeAsPrepared()', function () {
    it('should prepare and execute a query', function (done) {
      client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace],
        types.consistencies.one, 
        function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows.length, 1, 'There should be a row');
          done();
      });
    });
    
    it('should retry in case the node goes down', function (done) {
      var localClient = getANewClient({maxExecuteRetries: 3, staleTime: 150});
      var counter = -1;
      localClient._isServerUnhealthy = function() {
        //change the behaviour set it to unhealthy the first time
        if (counter < 2) {
          counter++;
          return true;
        }
        return false;
      };
      
      localClient.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace],
        types.consistencies.one, 
        function (err, result) {
          assert.ok(!err, err);
          assert.ok(result && result.rows.length === 1, 'There should be one row');
          localClient.shutdown(done);
      });
    });
    
    it('should stop retrying when the limit has been reached', function (done) {
      var localClient = getANewClient({maxExecuteRetries: 4, staleTime: 150});
      var counter = -1;
      localClient._isServerUnhealthy = function() {
        //change the behaviour set it to unhealthy the first time
        counter++;
        return true;
      };
      
      localClient.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace],
        types.consistencies.one, 
        function (err, result) {
          assert.ok(!result, 'It must stop retrying and callback without a result');
          assert.strictEqual(counter, localClient.options.maxExecuteRetries, 'It must retry an specific amount of times');
          localClient.shutdown(done);
      });
    });
    
    it('should allow from 2 to 4 arguments', function (done) {
      async.series([
        function (callback) {
          //all params
          client.executeAsPrepared('SELECT * FROM system.schema_keyspaces', [], types.consistencies.one, callback);
        },
        function (callback) {
          //no consistency specified
          client.executeAsPrepared('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?', [keyspace], callback);
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
            assert.ok(result && result.rows && result.rows.length, 'There should at least a row returned');
            callback(err, result);
          });
        }
      ],
      //all finished
      function(err, results){
        assert.ok(!err, err);
        done();
      });
    });

    it('should callback with error when the parameter can not be guessed', function (done) {
      client.executeAsPrepared(
        'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?',
        //pass an object: should not be guessed
        [{whatever: 1}],
        function (err, result){
          assert(err, 'It should callback with error');
          done()
      });
    });
    
    it('should failover to other nodes and reconnect', function (done) {
      this.timeout(10000);
      //the client must reconnect and continue
      var localClient = getANewClient();
      assert.ok(localClient.connections.length > 1, 'There should be more than 1 connection to test failover');
      async.timesSeries(12, function (n, next) {
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
        });
      }, function (err) {
        assert.ok(!err, 'There must not be an error returned. It must retried.');
        localClient.shutdown(done);
      });
    });
    
    it('should failover to other nodes and reconnect, in parallel executes', function (done) {
      //the client must reconnect and continue
      var localClient = getANewClient();
      assert.ok(localClient.connections.length > 1, 'There should be more than 1 connection to test failover');
      async.times(10, function (n, next) {
        localClient.execute('SELECT * FROM system.schema_keyspaces', function (err) {
          if (n === 3) {
            localClient.connections[0].netClient.destroy();
            localClient.connections[1].netClient.destroy();
          }
          next(err);
        })
      }, function (err) {
        assert.ok(!err, 'There must not be an error returned. All parallel queries should be retried.');
        localClient.shutdown(done);
      });
    });
    
    it('should prepare query again when expired from the server', function (done) {
      var query = 'SELECT * FROM system.schema_keyspaces';
      var localClient = getANewClient({hosts: [config.host + ':' + config.port]});
      var con = localClient.connections[0];
      async.series([
        localClient.connect.bind(localClient),
        function (next) {
          localClient.executeAsPrepared(query, next);
        },
        function (next) {
          var queryId = localClient.preparedQueries[query].getConnectionInfo(con.indexInPool).queryId;
          //Change the queryId
          queryId[0] = 0;
          queryId[1] = 0;
          next();
        },
        function (next) {
          localClient.executeAsPrepared(query, next);
        }
      ], done);
    });

    it('should add the query to the error object', function (done) {
      var query = 'SELECT WILL FAIL MISERABLY';
      client.executeAsPrepared(query, function (err) {
        assert.ok(err, 'There should be an error.');
        assert.ok(err.query, query);
        done();
      });
    });
  });

  describe('#executeBatch()', function () {
    it('should execute multiple DML queries', function (done) {
      var queries = [
        {query: queryBlobInsert, params: [50, new Buffer('Walter')]},
        {query: queryBlobInsert, params: [51, new Buffer('White')]}
      ];
      client.executeBatch(queries, types.consistencies.one, null, function (err) {
        assert.ok(!err, err);
        client.execute(queryBlobSelect, [51], function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows[0].blob_sample.toString(), 'White');
          done();
        });
      });
    });

    it('should fail on CQL syntax error', function (done) {
      var queries = [{query: 'SELECT TO FAIL MISERABLY', params: [1]}];
      client.executeBatch(queries, types.consistencies.one, null, function (err) {
        assert.ok(err, 'It should fail when there is a CQL syntax error');
        done();
      });
    });
  });

  describe('#streamField()', function () {
    it('should yield a readable stream', function (done) {
      var id = 100;
      var blob = new Buffer('Freaks and geeks 1999');
      async.series([
        function (next) {
          client.execute(queryBlobInsert, [id, blob], next);
        },
        function (next) {
          client.streamField(queryBlobSelect, [id], function (n, row, blobStream) {
            assert.strictEqual(row.get('id'), id, 'The row must be retrieved');
            assert.strictEqual(n, 0);
            assertStreamReadable(blobStream, blob, next);
          });
        }
      ], done);
    });

    it('should callback with error', function (done) {
      client.streamField('SELECT TO FAIL MISERABLY', function (n, row, stream) {
        assert.ok(fail, 'It should never execute the row callback');
      }, function (err) {
        assert.ok(err, 'It should yield an error');
        done();
      });
    });
    
    it('should yield a null stream when the field is null', function (done) {
      var id = 110;
      var blob = null;

      async.series([
        function (next) {
          client.execute(queryBlobInsert, [id, blob], next);
        },
        function (next) {
          client.streamField(queryBlobSelect, [id], function (n, row, blobStream) {
            assert.strictEqual(row.get('id'), id, 'The row must be retrieved');
            assert.equal(blobStream, null, 'The file stream must be NULL');
            next();
          });
        }
      ], done);
    });
  });

  describe('#eachRow()', function () {
    it('should callback and return all fields', function (done) {
      var id = 150;
      var blob = new Buffer('Frank Gallagher');

      async.series([
        function (next) {
          client.execute(queryBlobInsert, [id, blob], next);
        },
        function (next) {
          client.eachRow(queryBlobSelect, [id], function (n, row) {
            assert.ok(row && row.get('id') && row.get('blob_sample') && row.get('blob_sample').toString() === blob.toString());
          }, function (err, totalRows) {
            assert.strictEqual(totalRows, 1);
            next(err);
          });
        }
      ], done);
    });

    it('should callback once per row', function (done) {
      var id = 160;
      var blob = new Buffer('Jack Bauer');
      var counter = 0;
      async.timesSeries(4, function (n, next) {
        client.execute(queryBlobInsert, [id+n, blob], next);
      }, function (err) {
        if (err) return done(err);
        client.eachRow('SELECT id, blob_sample FROM sampletable2 WHERE id IN (?, ?, ?, ?);', [id, id+1, id+2, id+3], function (n, row) {
          assert.strictEqual(n, counter);
          assert.ok(row && row.get('id') && row.get('blob_sample'));
          counter++;
        }, function (err, totalRows) {
          assert.strictEqual(totalRows, counter);
          done(err);
        });
      });
    });

    it('should callback when there is an error', function (done) {
      async.series([
        function prepareQueryFail(next) {
          //it should fail when preparing
          client.eachRow('SELECT TO FAIL MISERABLY', [], function () {
            assert.ok(false, 'This callback should not be called');
          }, function (err) {
            assert.ok(err, 'There should be an error yielded');
            next();
          });
        },
        function executeQueryFail(next) {
          //it should fail when executing the query: There are more parameters than expected
          client.eachRow('SELECT * FROM system.schema_keyspaces', [1, 2, 3], function () {
            assert.ok(false, 'This callback should not be called');
          }, function (err) {
            assert.ok(err, 'There should be an error yielded');
            next();
          });
        }
      ], done);

    });
  });

  describe('#stream()', function () {
    var selectInQuery = 'SELECT * FROM sampletable2 where id IN (?, ?, ?, ?)';

    before(function (done) {
      var insertQuery = 'INSERT INTO sampletable2 (id, text_sample) values (?, ?)';
      helper.batchInsert(client, insertQuery, [[200, '200-Z'], [201, '201-Z'], [202, '202-Z']], done);
    });

    it('should stream rows', function (done) {
      var rows = [];
      var stream = client.stream(selectInQuery, [200, 201, -1, -1], helper.throwop);
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

    it('should end when no rows', function (done) {
      var stream = client.stream(selectInQuery, [-1, -1, -1, -1], helper.throwop);
      stream
        .on('end', done)
        .on('readable', function () {
          assert.ok(false, 'Readable event should not be fired');
        }).on('error', done);
    });

    it('should emit a ResponseError', function (done) {
      var counter = 0;
      var stream = client.stream(selectInQuery, [0], function (err) {
        assert.ok(err, 'It should callback with error');
        if (++counter === 2) done();
      });
      stream.on('error', function (err) {
        assert.strictEqual(err.name, 'ResponseError');
        if (++counter === 2) done();
      });
    });

    it('bad query should emit a ResponseError', function (done) {
      var counter = 0;
      var stream = client.stream('SELECT SHOULD FAIL', function (err) {
        assert.ok(err, 'It should callback with error');
        if (++counter === 2) done();
      });
      stream.on('error', function (err) {
        assert.strictEqual(err.name, 'ResponseError');
        if (++counter === 2) done();
      });
    });

    it('should be optional to provide a callback', function (done) {
      var rows = [];
      client.stream(selectInQuery, [200, 201, 202, -1])
        .on('end', function () {
          assert.strictEqual(rows.length, 3);
          done();
        })
        .on('readable', function () {
          var row;
          while (row = this.read()) {
            rows.push(row);
          }
        }).on('error', done);
    });
  });

  after(function (done) {
    client.shutdown(done);
  });
});

function getANewClient (options) {
  return new Client(utils.extend({}, clientOptions, options || {}));
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
    assert.equal(length, originalBlob.length, 'The blob returned should be the same size');
    assert.equal(firstByte, originalBlob[0], 'The first byte of the stream and the blob dont match');
    assert.equal(lastByte, originalBlob[originalBlob.length-1], 'The last byte of the stream and the blob dont match');
    callback();
  });
}

var queryBlobInsert = 'INSERT INTO sampletable2 (id, blob_sample) VALUES (?, ?)';
var queryBlobSelect = 'SELECT id, blob_sample FROM sampletable2 WHERE id = ?';