## Node.js CQL Driver for Apache Cassandra

Node.js CQL driver for [Apache Cassandra](http://cassandra.apache.org/) with a small dependency tree written in pure javascript.

The driver uses Cassandra's binary protocol which was introduced in Cassandra version 1.2.

## Installation

    $ npm install node-cassandra-cql

[![Build Status](https://secure.travis-ci.org/jorgebay/node-cassandra-cql.png)](http://travis-ci.org/jorgebay/node-cassandra-cql)

## Features
- Connection pooling to multiple hosts
- Load balancing and automatic failover
- Plain Old Javascript: no need to generate thrift files
- [Long][1] and [uuid][0] support
- Row and field streaming
- Prepared statements and query batches

## Using it
```javascript
//Creating a new connection pool to multiple hosts.
var cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['host1', 'host2'], keyspace: 'keyspace1'});
client.execute('SELECT key, email, last_name FROM user_profiles WHERE key=?', ['jbay'],
  function(err, result) {
    if (err) console.log('execute failed');
    else console.log('got user profile with email ' + result.rows[0].email);
  }
);
```

## API
### Client

The `Client` maintains a pool of opened connections to the hosts to avoid several time-consuming steps that are involved with the setup of a CQL binary protocol connection (socket connection, startup message, authentication, ...).

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

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

The driver will execute the query in a connection to a node. In case the Cassandra node becomes unreachable,
it will automatically retry it on another connection until `maxExecuteRetries` is reached.

Callback should take two arguments err and result.

#### client.executeAsPrepared(query, [params], [consistency], callback)

Prepares (the first time) and executes the prepared query.

Using **prepared statements increases performance** compared to plain executes, especially for repeated queries.

In the case the query is already being prepared on a host, it queues the executing of a prepared statement on that
host until the preparing finished (the driver will not issue a request to prepare statement more than once).
In case the Cassandra node becomes unreachable, it will automatically retry it on another connection until `maxExecuteRetries` is reached.

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

Callback should take two arguments err and result.

##### Example: Updating a row
```javascript
var query = 'UPDATE user_profiles SET birth=? WHERE key=?';
var params = [new Date(1950, 5, 1), 'jbay'];
var consistency = cql.types.consistencies.quorum;
client.executeAsPrepared(query, params, consistency, function(err) {
  if (err) console.log('Something when wrong and the row was not updated');
  else {
    console.log('Updated on the cluster');
  }
});
```

#### client.executeBatch(queries, [consistency], [options], callback)

Executes batch of queries on an available connection.

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

In case the Cassandra node becomes unreachable before a response,
it will automatically retry it on another connection until `maxExecuteRetries` is reached.

Callback should take two arguments err and result.

##### Example: Update multiple column families
```javascript
var userId = cql.types.uuid();
var messageId = cql.types.uuid();
var queries = [
  {
    query: 'INSERT INTO users (id, name) values (?, ?)',
    params: [userId, 'jbay']
  },
  {
    query: 'INSERT INTO messages (id, user_id, body) values (?, ?, ?)',
    params: [messageId, userId, 'Message from user jbay']
  }
];
var consistency = cql.types.consistencies.quorum;
client.executeBatch(queries, consistency, function(err) {
  if (err) console.log('The rows were not inserted on the cluster');
  else {
    console.log('Data updated on cluster');
  }
});
```

#### client.eachRow(query, [params], [consistency], rowCallback, endCallback)

Prepares (the first time), executes the prepared query and streams the rows as soon as they are received.

It executes `rowCallback(n, row)` per each row received, where `n` is the index of the row.

It executes `endCallback(err, rowLength)` when all rows have been received or there is an error retrieving the row.

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.


##### Example: Streaming query rows
```javascript
client.eachRow('SELECT event_time, temperature FROM temperature WHERE station_id=', ['abc'],
  function(n, row) {
    //the callback will be invoked per each row as soon as they are received
    console.log('temperature value', n, row.temperature);
  },
  function (err, rowLength) {
    if (err) console.log('Oh dear...');
    console.log('%d rows where returned', rowLength);
  }
);
```

#### client.streamField(query, [params], [consistency], rowCallback, [endCallback])

Prepares (the first time), executes the prepared query and streams the last field of each row.

It executes `rowCallback(n, row, streamField)` per each row as soon as the first chunk of the last field is received, where `n` is the index of the row.

The `stream` is a [Readable Streams2](http://nodejs.org/api/stream.html#stream_class_stream_readable) object that contains the raw bytes of the field value.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

The `row` object is similar to the one provided on `eachRow`, except that it does not contain the definition of the last column.

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

It executes `endCallback(err, rowLength)` when all rows have been received or there is an error retrieving the row.

##### Example: Streaming the contents of a field
```javascript
client.streamField('SELECT key, photo FROM user_profiles WHERE key=', ['jbay'],
  function(err, row, photoStream) {
    //the callback will be invoked per each row as soon as they are received.
    if (err) console.log('Shame...');
    else {
      //The stream is a Readable Stream2 object
      stdout.pipe(photoStream);
    }
  }
);
```

#### client.stream(query, [params], [consistency], [callback])

Returns a [Readable Streams2](http://nodejs.org/api/stream.html#stream_class_stream_readable) object in `objectMode`.
When a row can be read from the stream, it will emit a `readable` event.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

Prepares (the first time), executes the prepared query.

Use one of the values defined in `types.consistencies` for  `consistency`, defaults to quorum.

It executes `callback(err)` when all rows have been received or there is an error retrieving the row.

##### Example: Reading the whole resultset as stream
```javascript
client.stream('SELECT time1, value1 FROM timeseries WHERE key=', ['key123'])
  .on('readable', function () {
    //readable is emitted as soon a row is received and parsed
    var row;
    while (row = this.read()) {
      console.log('time %s and value %s', row.time1, row.value1);
    }
  })
  .on('end', function () {
    //stream ended, there aren't any more rows
  })
  .on('error', function (err) {
    //Something went wrong: err is a response error from Cassandra
  });
```

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

----

### types

The `types` module contains field definitions that are useful to interact with Cassandra nodes.

#### consistencies

Object that contains the CQL consistencies defined as properties. For example: `consistencies.one`, `consistencies.quorum`, ...

#### dataTypes

Object that contains all the [CQL data types](http://cassandra.apache.org/doc/cql3/CQL.html#types) defined as properties.

#### responseErrorCodes

Object containing all the possible response error codes returned by Cassandra defined as properties.

#### Long()

Constructs a 64-bit two's-complement integer. See [Long API Documentation](http://docs.closure-library.googlecode.com/git/class_goog_math_Long.html).

#### timeuuid()

Function to generate a uuid __v1__. It uses [node-uuid][0] module to generate and accepts the same arguments.

#### uuid()

Function to generate a uuid __v4__. It uses [node-uuid][0] module to generate and accepts the same arguments.

----

## Logging

Instances of `Client()` and `Connection()` are `EventEmitter`'s and emit `log` events:
```javascript
client.on('log', function(level, message) {
  console.log('log event: %s -- %j', level, message);
});
```
The `level` being passed to the listener can be `info` or `error`.

## Data types

Cassandra's bigint data types are parsed as [Long][1].

List / Set datatypes are encoded from / decoded to Javascript Arrays.

Map datatype are encoded from / decoded to Javascript objects with keys as props.

Decimal and Varint are not parsed yet, they are yielded as byte Buffers.

[Check the documentation for data type support →](https://github.com/jorgebay/node-cassandra-cql/wiki/Data-types)

## FAQ
#### Which Cassandra versions does this driver support?
It supports any Cassandra version greater than 1.2.0.

If you are using Cassandra 2.x and you want to enable all the latest features in the Cassandra binary protocol v2 (ie: batches), you should reference version **0.5.x**:

```bash
$ npm install node-cassandra-cql@protocol2
```

#### Which CQL version does this driver support?
It supports [CQL3](http://cassandra.apache.org/doc/cql3/CQL.html).

#### How can specify the target data type of a query parameter?
The driver tries to guess the target data type, if you want to set the target data type use a param object with
the **hint** and **value** properties.

All the cassandra data types are defined in the object `types.dataTypes`.

For example:

```javascript
//hint as string
var keyParam = {value: key, hint: 'int'};
client.executeAsPrepared('SELECT * from users where k=?', [keyParam], callback);

//hint using dataTypes
var keyParam = {value: key, hint: types.dataTypes.int};
client.executeAsPrepared('SELECT * from users where k=?', [keyParam], callback);
```

#### Should I shutdown the pool after executing a query?
No, you should only call `client.shutdown` once in your application lifetime.

## License

node-cassandra-cql is distributed under the [MIT license](http://opensource.org/licenses/MIT).

## Contributions

Feel free to join in to help this project grow!

Check the [Issue tracker](https://github.com/jorgebay/node-cassandra-cql/issues), there are issues even marked "New Contributors Welcome" :)

## Acknowledgements

FrameReader and FrameWriter are based on [node-cql3](https://github.com/isaacbwagner/node-cql3)'s FrameBuilder and FrameParser.

[0]: https://github.com/broofa/node-uuid
[1]: https://github.com/dcodeIO/Long.js