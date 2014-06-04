## DataStax Node.js Driver for Apache Cassandra

Node.js CQL driver for [Apache Cassandra](http://cassandra.apache.org/).

The driver uses Cassandra's binary protocol which was introduced in Cassandra version 1.2.

## API
### Client

The `Client` maintains a pool of opened connections to the hosts to avoid several time-consuming steps that are involved with the setup of a CQL binary protocol connection (socket connection, startup message, authentication, ...).

*The Client is the recommended driver class to interact with Cassandra nodes*.

#### new Client(options)

Constructs a new client object.

#### client.connect([callback])

Connects / warms up the pool.

It ensures the pool is connected. It is not required to call it, internally the driver will call to `connect` when executing a query.

The optional `callback` parameter will be executed when the pool is connected. If the pool is already connected, it will be called instantly. 

#### client.execute(query, [params], [options], callback)

Prepares (the first time) and executes the prepared query.

The `query` is the cql query to execute, with `?` placeholders as parameters.


Using **prepared statements increases performance** compared to plain executes, especially for repeated queries.

In the case the query is already being prepared on a host, it queues the executing of a prepared statement on that
host until the preparing finished (the driver will not issue a request to prepare statement more than once).

The driver will execute the query in a connection to a node. In case the Cassandra node becomes unreachable,
it will automatically retry it on another connection until `maxExecuteRetries` is reached.

Callback should take two arguments err and result.

##### Example: Updating a row
```javascript
var query = 'UPDATE user_profiles SET birth=? WHERE key=?';
var params = [new Date(1950, 5, 1), 'jbay'];
var consistency = cql.types.consistencies.quorum;
client.execute(query, params, {consistency: consistency}, function(err) {
  if (err) console.log('Something when wrong and the row was not updated');
  else {
    console.log('Updated on the cluster');
  }
});
```

#### client.executeBatch(queries, [options], callback)

Executes batch of queries on an available connection.

In case the Cassandra node becomes unreachable before a response,
it will automatically retry it on another connection until `maxExecuteRetries` is reached.

Callback should take two arguments err and result.

#### client.eachRow(query, [params], [options], rowCallback, endCallback)

Prepares (the first time), executes the prepared query and streams the rows as soon as they are received.

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

#### client.stream(query, [params], [options])

Returns a [Readable Streams2](http://nodejs.org/api/stream.html#stream_class_stream_readable) object in `objectMode`.
When a row can be read from the stream, it will emit a `readable` event.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

Prepares (the first time), executes the prepared query.

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

Cassandra's bigint data types are parsed as [Long][long].

List / Set datatypes are encoded from / decoded to Javascript Arrays.

Map datatype are encoded from / decoded to Javascript objects with keys as props.

Decimal and Varint are not parsed yet, they are yielded as byte Buffers.

## FAQ
#### Which Cassandra versions does this driver support?
It supports any Cassandra version greater than 1.2.0.

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
client.execute('SELECT * from users where k=?', [keyParam], callback);

//hint using dataTypes
var keyParam = {value: key, hint: types.dataTypes.int};
client.execute('SELECT * from users where k=?', [keyParam], callback);
```

#### Should I shutdown the pool after executing a query?
No, you should only call `client.shutdown` once in your application lifetime.

## License

Copyright 2014 DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


[uuid]: https://github.com/broofa/node-uuid
[long]: https://github.com/dcodeIO/Long.js