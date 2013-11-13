## Node.js CQL Driver for Apache Cassandra

Node.js CQL driver for [Apache Cassandra](http://cassandra.apache.org/) with a small dependency tree written in pure javascript.

The driver uses Cassandra's binary protocol which was introduced in Cassandra version 1.2.


  [![Build Status](https://secure.travis-ci.org/jorgebay/node-cassandra-cql.png)](http://travis-ci.org/jorgebay/node-cassandra-cql)

## Installation

    $ npm install node-cassandra-cql

## Features
- Connection pooling to multiple hosts
- Plain Old Javascript: no need to generate thrift files
- [Bigints](https://github.com/broofa/node-int64) and [uuid](https://github.com/broofa/node-uuid) support
- Load balancing and automatic failover
- Prepared statements

## Using it
```javascript
// Creating a new connection pool to multiple hosts.
var cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['host1:9042', 'host2:9042'], keyspace: 'keyspace1'});
// Reading
client.execute('SELECT key, email, last_name FROM user_profiles WHERE key=?', ['jbay'],
  function(err, result) {
    if (err) console.log('execute failed');
    else console.log('got user profile with email ' + result.rows[0].get('email'));
  }
);

// Writing
client.execute('UPDATE user_profiles SET birth=? WHERE key=?', [new Date(1950, 5, 1), 'jbay'], 
  cql.types.consistencies.quorum,
  function(err) {
    if (err) console.log("failure");
    else console.log("success");
  }
);
```

## API
### Client

The `Client` maintains a pool of opened connections to the hosts to avoid several time-consuming steps that are involved with the set up of a CQL binary protocol connection (socket connection, startup message, authentication, ...).

*The Client is the recommended driver class to interact with Cassandra nodes*.

#### new Client(options)

Constructs a new client object.

`options` is an object with these slots, only `hosts` is required:
```
                hosts: Array of string in host:port format. Port is optional (default 9042).
             keyspace: Name of keyspace to use.
             username: User for authentication.
             password: Password for authentication.
            staleTime: Time in milliseconds before trying to reconnect to a node.
    maxExecuteRetries: Maximum amount of times an execute can be retried
                       using another connection, in case the server is unhealthy.
getAConnectionTimeout: Maximum time in milliseconds to wait for a connection from the pool.
             poolSize: Number of connections to open for each host (default 1)
```

#### client.connect([callback])

Connects / warms up the pool.

It ensures the pool is connected. It is not required to call it, internally the driver will call to `connect` when executing a query.

The optional `callback` parameter will be executed when the pool is connected. If the pool is already connected, it will be called instantly. 

#### client.execute(query, [params], [consistency], callback)  

Executes a CQL query.

The `query` is the cql query to execute, with `?` placeholders as parameters.

Use once of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

Callback should take two arguments err and result.

*The driver will replace the placeholders with the `params`, strigified into the query*.

#### client.executeAsPrepared(query, [params], [consistency], callback)

Prepares (the first time) and executes the prepared query.

To execute a prepared query, the `params` are binary serialized. Using **prepared statements increases performance**, especially for repeated queries.

Use once of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

Callback should take two arguments err and result.

#### client.shutdown([callback])

Disconnects the pool.

Closes all connections in the pool. Normally, it should be called once in your application lifetime.

The optional `callback` parameter will be executed when the pool is disconnected.

----


### Connection

In the case that you need lower level fine-grained control you could use the `Connection` class.

It represents a connection to a Cassandra node. The consumer has to take care of open and close it.

#### new Connection(options)

Constructs a new connection object.

#### open(callback) 

Establishes a connection, authenticates and sets a keyspace.

#### close(callback)

Closes the connection to a Cassandra node.

#### execute(query, args, consistency, callback)

Executes a CQL query.

#### prepare(query, callback)

Prepares a CQL query.

#### executePrepared(queryId, args, consistency, callback)

Executes a previously prepared query (determined by the queryId).

## Logging

Instances of `Client()` and `Connection()` are `EventEmitter`'s and emit `log` events:
```javascript
client.on('log', function(level, message) {
  console.log('log event: %s -- %j', level, message);
});
```
The `level` being passed to the listener can be `info` or `error`.

## Data types

Cassandra's bigint data types are parsed as [int64](https://github.com/broofa/node-int64).

List / Set datatypes are encoded from / decoded to Javascript Arrays.

Map datatype are encoded from / decoded to Javascript objects with keys as props.

Decimal and Varint are not parsed yet, they are yielded as byte Buffers.

[Check the documentation for data type support →](https://github.com/jorgebay/node-cassandra-cql/wiki/Data-types)

## FAQ

#### How can specify the target data type of a query parameter?
The driver try to guess the target data type, if you want to set the target data type use a param object with the **hint** and **value** hint. For example: 
````javascript
client.executeAsPrepared('SELECT * from users where key=?', [{value: key, hint: 'int'}], callback);
````
#### Should I shutdown the pool after executing a query?
No, you should only call `client.shutdown` once in your application lifetime.

## License

node-cassandra-cql is distributed under the [MIT license](http://opensource.org/licenses/MIT).

## Contributions

Feel free to join in to help this project grow!

## Acknowledgements

FrameReader and FrameWriter are based on [node-cql3](https://github.com/isaacbwagner/node-cql3)'s FrameBuilder and FrameParser.
