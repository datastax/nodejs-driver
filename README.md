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

## Documentation

- [Documentation index][doc-index]
- [CQL types to javascript types][doc-datatypes]
- [FAQ][faq]

## Getting Help

You can use the project [Mailing list][mailinglist] or create a ticket on the [Jira issue tracker][jira].

## What's coming next

- SSL support
- Automatic paging
- Token-aware load balancing policy

## API

- [Client](#client) class
- [types](#types) module
- [policies](#policies) module
- [auth](#auth) module


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

#### client.eachRow(query, [params], [queryOptions], rowCallback, endCallback)

Executes a query and streams the rows as soon as they are received.

It executes `rowCallback(n, row)` per each row received, where `n` is the index of the row.

It executes `callback(err, rowLength)` when all rows have been received or there is an error retrieving the row.


##### Example: Reducing a result set
```javascript
client.eachRow('SELECT time, temperature FROM temperature WHERE station_id=', ['abc'],
  function(n, row) {
    //the callback will be invoked per each row as soon as they are received
    minTemperature = Math.min(row.temperature, minTemperature);
  },
  function (err, rowLength) {
    if (err) console.error(err);
    console.log('%d rows where returned', rowLength);
  }
);
```

#### client.batch(queries, [queryOptions], callback)

Executes batch of queries on an available connection, where `queries` is an Array of string containing the CQL queries
 or an Array of objects containing the query and the parameters.

`callback` should take two arguments err and result.

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

#### client.stream(query, [params], [queryOptions])

Executes the query and returns a [Readable Streams2](http://nodejs.org/api/stream.html#stream_class_stream_readable) object in `objectMode`.
When a row can be read from the stream, it will emit a `readable` event.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

It executes `callback(err)` when all rows have been received or there is an error retrieving the row.

##### Example: Reading the whole result as stream
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

#### loadBalancing

Load balancing policies lets you decide which node of the cluster will be used for each query.

- **RoundRobinPolicy**: This policy yield nodes in a round-robin fashion (default).
- **DCAwareRoundRobinPolicy**: A data-center aware round-robin load balancing policy. This policy provides round-robin 
queries over the node of the local data center. It also includes in the query plans returned a configurable 
number of hosts in the remote data centers, but those are always tried after the local nodes.

To use it, you must provide load balancing policy the instance in the `clientOptions` of the `Client` instance.

```javascript
//You can specify the local dc relatively to the node.js app
var localDc = 'us-east';
var lbPolicy = new driver.policies.loadBalancing.DCAwareRoundRobinPolicy(localDc);
var clientOptions = {
  policies: {loadBalancing: loadBalancingPolicy}
};
var client = new driver.Client(clientOptions);
```

Load balancing policy classes inherit from **LoadBalancingPolicy**. If you want make your own policy, you should use the same base class.

#### retry

Retry policies lets you configure what the driver should do when there certain types of exceptions from Cassandra are received.

- **RetryPolicy**: Default policy and base class for retry policies. 
It retries once in case there is a read or write timeout and the alive replicas are enough to satisfy the consistency level. 

#### reconnection

Reconnection policies lets you configure when the driver should try to reconnect to a Cassandra node that appears to be down.

- **ConstantReconnectionPolicy**: It waits a constant time between each reconnection attempt.
- **ExponentialReconnectionPolicy**: waits exponentially longer between each reconnection attempt, until maximum delay is reached. 

### auth

The `auth` module provides the classes required for authentication.

- **PlainTextAuthProvider**: Authentication provider for Cassandra's PasswordAuthenticator.

Using an authentication provider on an auth-enabled Cassandra cluster:

```javascript
var authProvider = new driver.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');
//Setting the auth provider in the clientOptions
var client = new driver.Client({authProvider: authProvider});
```

Authenticator provider classes inherit from **AuthProvider**. If you want to create your own auth provider, use the that as your base class. 

----

## Logging

Instances of `Client()` are `EventEmitter` and emit `log` events:
```javascript
client.on('log', function(level, className, message, furtherInfo) {
  console.log('log event: %s -- %s', level, message);
});
```
The `level` being passed to the listener can be `verbose`, `info`, `warning` or `error`.

## Data types

See [documentation on CQL data types and ECMAScript types][doc-datatypes].

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
[mailinglist]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[jira]: https://datastax-oss.atlassian.net/browse/NODEJS
[doc-index]: http://datastax.github.io/nodejs-driver/
[doc-datatypes]: http://datastax.github.io/nodejs-driver/datatypes
[faq]: http://datastax.github.io/nodejs-driver/faq