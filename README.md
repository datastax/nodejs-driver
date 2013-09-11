## Node.js CQL Driver for Apache Cassandra

node-cassandra-cql is a Node.js CQL driver for [Apache Cassandra](http://cassandra.apache.org/)'s native protocol with a small dependency tree written in pure javascript.

  [![Build Status](https://secure.travis-ci.org/jorgebay/node-cassandra-cql.png)](http://travis-ci.org/jorgebay/node-cassandra-cql)

## Installation

    $ npm install node-cassandra-cql

## Features
- Connection pooling to multiple hosts
- Parameters in queries (even for sets/lists/maps collections)
- Plain Old Javascript: no need to generate thrift files
- Get cell by column name: `row.get('first_name')`
- Bigints (using [node-int64](https://github.com/broofa/node-int64)) and uuid support
- Prepared statements

## Using it
```javascript
// Creating a new connection pool to multiple hosts.
var Client = require('node-cassandra-cql').Client;
var hosts = ['host1:9042', 'host2:9042', 'host3', 'host4'];
var client = new Client({hosts: hosts, keyspace: 'Keyspace1'});
```
Client() accepts an object with these slots, only `hosts` is required:
```
                hosts: String list in host:port format. Port is optional (defaults to 9042).
             keyspace: Name of keyspace to use.
             username: User for authentication.
             password: Password for authentication.
              version: Currently only '3.0.0' is supported.
            staleTime: Time in milliseconds before trying to reconnect.
    maxExecuteRetries: Maximum amount of times an execute can be retried
                       using another connection, in case the server is unhealthy.
getAConnectionTimeout: Maximum time in milliseconds to wait for a connection from the pool.
```
Queries are performed using the `execute()` method. For example:
```javascript
// Reading
client.execute('SELECT key, email, last_name FROM user_profiles WHERE key=?', ['jbay'],
  function(err, result) {
    if (err) console.log('execute failed');
    else console.log('got user profile with email ' + result.rows[0].get('email'));
  }
);

// Writing
client.execute('UPDATE user_profiles SET birth=? WHERE key=?', [new Date(1950, 5, 1), 'jbay'], 
  types.consistencies.quorum,
  function(err) {
    if (err) console.log("failure");
    else console.log("success");
  }
);
```
`execute()` and `executeAsPrepared()` accept the following arguments

        cqlQuery : The cql query to execute, with ? as parameters
        arguments: Array of arguments that will replace the ? placeholders. Optional.
     consistency : The level of consistency. Optional, defaults to quorum.
        callback : The callback function with 2 arguments: err and result

```javascript
// Shutting down a pool
cqlClient.shutdown(function() { console.log("connection pool shutdown"); });
```

### API
#### Client
- `execute(query, args, consistency, callback)`   
Executes a CQL query.
- `executeAsPrepared(query, args, consistency, callback)`   
Prepares (once) and executes the prepared query.
- `shutdown(callback)`   
Shutdowns the pool (normally it would be called once in your app lifetime).

`execute()` and `executeAsPrepared()` accepts the following arguments
```
   query:       The cql query to execute, with ? as parameters
   arguments:   Array of arguments that will replace the ? placeholders. Optional.
   consistency: The level of consistency. Optional, defaults to quorum.
   callback:    The callback function with 2 arguments: err and result
```

#### Connection
- `open(callback)`   
Establishes a connection, authenticates and sets a keyspace.
- `close(callback)`   
Closes the connection.
- `execute(query, args, consistency, callback)`   
Executes a CQL query.
- `prepare(query, callback)`   
Prepares a CQL query.
- `executePrepared(queryId, args, consistency, callback)`   
Executes a previously prepared query (determined by the queryId).


### Connections
The `Client` maintains a pool of opened connections to the hosts to avoid several time-consuming steps that are involed with the set up of a CQL binary protocol connection (socket connection, startup message, authentication, ...).

If you want to get lower level fine-grained control you could use the `Connection` class.
```javascript
var Connection = require('node-cassandra-cql').Connection;
var con = new Connection({host:'host1', port:9042, username:'cassandra', password:'cassandra'});
con.open(function(err) {
  if(err) {
    console.error(err);
  }
  else {
    var query = 'SELECT key, email, last_name FROM user_profiles WHERE key=?';
    con.execute(query, ['jbay'], function(err, result){
      if (err) console.log('execute failed');
      else console.log('got user profile with email ' + result.rows[0].get('email'));
      con.close();
    });
  }
});
```

### Logging

Instances of `Client()` and `Connection()` are `EventEmitter`'s and emit `log` events:
```javascript
var Connection = require('node-cassandra-cql').Connection;
var con = new Connection({host:'host1', port:9042, keyspace:'Keyspace1'});
con.on('log', function(level, message) {
  console.log('log event: %s -- %j', level, message);
});
```
The `level` being passed to the listener can be `info` or `error`.

### Data types

Cassandra's bigint data types are parsed as [int64](https://github.com/broofa/node-int64).

List / Set datatypes are encoded from / decoded to Javascript Arrays.

Map datatype are encoded from / decoded to Javascript objects with keys as props.

Decimal and Varint are not parsed yet, they are yielded as byte Buffers.


## License

node-cassandra-cql is distributed under the [MIT license](http://opensource.org/licenses/MIT).

## Contributions

Feel free to join in if you feel like helping this project progress!

## Acknowledgements

FrameReader and FrameWriter are based on [node-cql3](https://github.com/isaacbwagner/node-cql3)'s FrameBuilder and FrameParser.
