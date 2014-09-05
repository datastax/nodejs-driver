# DataStax Node.js Driver for Apache Cassandra

Node.js driver for [Apache Cassandra][cassandra]. This driver works exclusively with the Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol.

## Installation

```bash
$ npm install cassandra-driver
```

## Features

- Nodes discovery
- Configurable load balancing
- Transparent failover
- Tunability
- Row streaming
- Prepared statements and query batches

## Basic usage

```javascript
var driver = require('cassandra-driver');
var client = new driver.Client({contactPoints: ['host1', 'host2'], keyspace: 'ks1'});
var query = 'SELECT email, last_name FROM user_profiles WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  console.log('got user profile with email ' + result.rows[0].email);
});

```

## API
### Client

The `Client` maintains a pool of opened connections to the hosts to avoid several time-consuming steps that are involved with the setup of a CQL binary protocol connection (socket connection, startup message, authentication, ...).

*Usually you need one Client instance per Cassandra cluster.*

#### new Client(options)

Constructs a new client object.

#### client.connect([callback])

Connects / warms up the pool.

It ensures the pool is connected. It is not required to call it, internally the driver will call to `connect` when executing a query.

The optional `callback` parameter will be executed when the pool is connected. If the pool is already connected, it will be called instantly. 

#### client.execute(query, [params], [queryOptions], callback)

The `query` is the cql query to execute, with `?` marker for parameter placeholder.

To prepare an statement, provide {prepare: true} in the queryOptions. It will prepare (the first time) and execute the prepared statement.
                                                                                                                     
Using prepared statements increases performance compared to plain executes, especially for repeated queries. 
It has the additional benefit of providing metadata of the parameters to the driver, 
**allowing better type mapping between javascript and Cassandra** without the need of additional info (hints) from the user. 

In the case the query is already being prepared on a host, it queues the executing of a prepared statement on that
host until the preparing finished (the driver will not issue a request to prepare statement more than once).

`callback` should take two arguments err and result.

`queryOptions` is an Object that may contain the following optional properties:

- `prepare`: if set, prepares the query (once) and executes the prepared statement.
- `consistency`: the consistency level for the operation (defaults to one).
The possible consistency levels are defined in `driver.types.consistencies`.
- `fetchSize`: The maximum amount of rows to be retrieved per request (defaults to 5000)

##### Example: Updating a row
```javascript
var query = 'UPDATE user_profiles SET birth=? WHERE key=?';
var queryOptions = {
  consistency: driver.types.consistencies.quorum,
  prepare: true};
var params = [new Date(1942, 10, 1), 'jimi-hendrix'];
client.execute(query, params, queryOptions, function(err) {
  if (err) return console.log('Something when wrong', err);
  console.log('Row updated on the cluster');
});
```

#### client.batch(queries, [queryOptions], callback)

Executes batch of queries on an available connection.

Callback should take two arguments err and result.

#####Example: Update multiple column families

```javascript
var userId = driver.types.uuid();
var messageId = driver.types.uuid();
var queries = [
  {
    query: 'INSERT INTO users (id, name) values (?, ?)',
    params: [userId, 'jimi-hendrix']
  },
  {
    query: 'INSERT INTO messages (id, user_id, body) values (?, ?, ?)',
    params: [messageId, userId, 'Message from user jimi-hendrix']
  }
];
var queryOptions: { consistency: driver.types.consistencies.quorum };
client.batch(queries, queryOptions, function(err) {
  if (err) return console.log('The rows were not inserted', err);
  console.log('Data updated on cluster');
});
```

#### client.eachRow(query, [params], [queryOptions], rowCallback, endCallback)

Executes a query and streams the rows as soon as they are received.

It executes `rowCallback(n, row)` per each row received, where `n` is the index of the row.

It executes `endCallback(err, rowLength)` when all rows have been received or there is an error retrieving the row.


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

#### client.stream(query, [params], [queryOptions])

Executes the query and returns a [Readable Streams2](http://nodejs.org/api/stream.html#stream_class_stream_readable) object in `objectMode`.
When a row can be read from the stream, it will emit a `readable` event.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

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

Function to generate a uuid __v1__. It uses [node-uuid][uuid] module to generate and accepts the same arguments.

#### uuid()

Function to generate a uuid __v4__. It uses [node-uuid][uuid] module to generate and accepts the same arguments.


### policies

The `policies` module contains load balancing, retry and reconnection classes.

### auth

The `auth` module provides the classes required for authentication. 
----

## Logging

Instances of `Client()` is an `EventEmitter` and emits `log` events:
```javascript
client.on('log', function(level, message) {
  console.log('log event: %s -- %j', level, message);
});
```
The `level` being passed to the listener can be `info` or `error`.

## Data types

Cassandra's bigint data types are parsed as [Long][long].

List / Set datatypes are encoded from / decoded to Javascript Arrays.

Map datatype are encoded from / decoded to Javascript objects with keys as props.

Decimal and Varint are not parsed yet, they are yielded as byte Buffers.

## FAQ
#### Which Cassandra versions does this driver support?
It supports any Cassandra version greater than 2.0 and above.

#### Which CQL version does this driver support?
It supports [CQL3](http://cassandra.apache.org/doc/cql3/CQL.html).

#### Should I shutdown the pool after executing a query?
No, you should only call `client.shutdown` once in your application lifetime.

## Credits

This driver is based on the original work of [Jorge Bay][jorgebay] on [node-cassandra-cql][old-driver] and adds a series of advanced features that are common across all other [DataStax drivers][drivers] for Apache Cassandra.

The development effort to provide an up to date, high performance, fully featured Node.js Driver for Apache Cassandra will continue on this project, while [node-cassandra-cql][old-driver] will be discontinued.

## License

Copyright 2014 DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


[uuid]: https://github.com/broofa/node-uuid
[long]: https://github.com/dcodeIO/Long.js
[cassandra]: http://cassandra.apache.org/
[old-driver]: https://github.com/jorgebay/node-cassandra-cql
[jorgebay]: https://github.com/jorgebay
[drivers]: https://github.com/datastax