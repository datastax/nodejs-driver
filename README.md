# DataStax Node.js Driver for Apache Cassandra

A modern, [feature-rich](#features) and highly tunable Node.js client library for Apache Cassandra (1.2+) using exclusively Cassandra's binary protocol and Cassandra Query Language v3. _Use the [DSE Node.js driver][dse-driver] for better compatibility and support for DataStax Enterprise_.

## Installation

```bash
$ npm install cassandra-driver
```

[![Build Status](https://travis-ci.org/datastax/nodejs-driver.svg?branch=master)](https://travis-ci.org/datastax/nodejs-driver) [![Build status](https://ci.appveyor.com/api/projects/status/m21t2tfdpmkjex1l/branch/master?svg=true)](https://ci.appveyor.com/project/datastax/nodejs-driver/branch/master)


## Features

- Simple, Prepared, and [Batch][batch] statements
- Asynchronous IO, parallel execution, request pipelining
- [Connection pooling][pooling]
- Auto node discovery
- Automatic reconnection
- Configurable [load balancing][load-balancing] and [retry policies][retry]
- Works with any cluster size
- Both [promise and callback-based API][doc-promise-callback]
- [Row streaming and pipes](#row-streaming-and-pipes)

## Documentation

- [Documentation index][doc-index]
- [CQL types to JavaScript types][doc-datatypes]
- [API docs][doc-api]
- [FAQ][faq]

## Getting Help

You can use the [project mailing list][mailinglist] or create a ticket on the [Jira issue tracker][jira]. Additionally, you can use the `#datastax-drivers` channel in the [DataStax Academy Slack][slack].

## Basic usage

```javascript
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['h1', 'h2'], keyspace: 'ks1' });

const query = 'SELECT name, email FROM users WHERE key = ?';
client.execute(query, [ 'someone' ])
  .then(result => console.log('User with email %s', result.rows[0].email));
```

Alternatively, you can use the callback-based execution for all asynchronous methods of the API.

```javascript
client.execute(query, [ 'someone' ], function(err, result) {
  assert.ifError(err);
  console.log('User with email %s', result.rows[0].email);
});
```

### Prepare your queries

Using prepared statements provides multiple benefits.
Prepared statements are parsed and prepared on the Cassandra nodes and are ready for future execution.
Also, when preparing, the driver retrieves information about the parameter types which
 **allows an accurate mapping between a JavaScript type and a Cassandra type**.

The driver will prepare the query once on each host and execute the statement with the bound parameters.

```javascript
// Use query markers (?) and parameters
const query = 'UPDATE users SET birth = ? WHERE key=?'; 
const params = [ new Date(1942, 10, 1), 'jimi-hendrix' ];
// Set the prepare flag in the query options
client.execute(query, params, { prepare: true })
  .then(result => console.log('Row updated on the cluster'));
```

### Row streaming and pipes

When using `#eachRow()` and `#stream()` methods, the driver parses each row as soon as it is received,
 yielding rows without buffering them.

```javascript
// Reducing a large result
client.eachRow('SELECT time, val FROM temperature WHERE station_id=', ['abc'],
  function(n, row) {
    // The callback will be invoked per each row as soon as they are received
    minTemperature = Math.min(row.val, minTemperature);
  },
  function (err) {
    assert.ifError(err);
  }
);
```

The `#stream()` method works in the same way but instead of callback it returns a [Readable Streams2][streams2] object
 in `objectMode` that emits instances of `Row`.
It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

```javascript
client.stream('SELECT time, val FROM temperature WHERE station_id=', [ 'abc' ])
  .on('readable', function () {
    // 'readable' is emitted as soon a row is received and parsed
    var row;
    while (row = this.read()) {
      console.log('time %s and value %s', row.time, row.val);
    }
  })
  .on('end', function () {
    // Stream ended, there aren't any more rows
  })
  .on('error', function (err) {
    // Something went wrong: err is a response error from Cassandra
  });
```

### User defined types

[User defined types (UDT)][cql-udt] are represented as JavaScript objects.

For example:
Consider the following UDT and table

```cql
CREATE TYPE address (
  street text,
  city text,
  state text,
  zip int,
  phones set<text>
);
CREATE TABLE users (
  name text PRIMARY KEY,
  email text,
  address frozen<address>
);
```

You can retrieve the user address details as a regular JavaScript object.

```javascript
const query = 'SELECT name, address FROM users WHERE key = ?';
client.execute(query, [ key ], { prepare: true })
  .then(result => {
    const row = result.first();
    const address = row.address;
    console.log('User lives in %s, %s - %s', address.street, address.city, address.state); 
  });
```

Read more information  about using [UDTs with the Node.js Driver][doc-udt].

### Paging

All driver methods use a default `fetchSize` of 5000 rows, retrieving only first page of results up to a
 maximum of 5000 rows to shield an application against accidentally large result sets. To retrieve the following
 records you can use the `autoPage` flag in the query options of `#eachRow()` and `#stream()` methods.

```javascript
//Imagine a column family with millions of rows
const query = 'SELECT * FROM largetable';
client.eachRow(query, [], { autoPage: true }, function (n, row) {
  // This function will be invoked per each of the rows in all the table
}, endCallback);
```

### Batch multiple statements

You can execute multiple statements in a batch to update/insert several rows atomically even in different column families.

```javascript
const queries = [
  {
    query: 'UPDATE user_profiles SET email=? WHERE key=?',
    params: [ emailAddress, 'hendrix' ]
  },
  {
    query: 'INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)',
    params: [ 'hendrix', 'Changed email', new Date() ]
  }
];
client.batch(queries, { prepare: true })
  .then(result => console.log('Data updated on cluster'));
```

----

## Data types

There are few data types defined in the ECMAScript specification, this usually represents a problem when you are trying
 to deal with data types that come from other systems in JavaScript.

The driver supports all the CQL data types in Apache Cassandra (3.0 and below) even for types with no built-in
JavaScript representation, like decimal, varint and bigint. Check the documentation on working with
 [numerical values][doc-numerical], [uuids][doc-uuid] and [collections][doc-collections].

## Logging

Instances of `Client()` are `EventEmitter` and emit `log` events:

```javascript
client.on('log', function(level, className, message, furtherInfo) {
  console.log('log event: %s -- %s', level, message);
});
```

The `level` being passed to the listener can be `verbose`, `info`, `warning` or `error`.

## Compatibility

- Apache Cassandra versions 2.0 and above.
- DataStax Enterprise versions 4.5 and above.
- Node.js versions 0.10 and above.

Note: DataStax products do not support big-endian systems.

## Feedback Requested

**Help us focus our efforts!** Provide your input on the [Platform and Runtime Survey][survey] (we kept it short).

## Credits

This driver is based on the original work of [Jorge Bay][jorgebay] on [node-cassandra-cql][old-driver] and adds a series of advanced features that are common across all other [DataStax drivers][drivers] for Apache Cassandra.

The development effort to provide an up to date, high performance, fully featured Node.js Driver for Apache Cassandra will continue on this project, while [node-cassandra-cql][old-driver] will be discontinued.

## License

Copyright 2014-2017 DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[cassandra]: http://cassandra.apache.org/
[doc-api]: http://docs.datastax.com/en/developer/nodejs-driver/latest/api/
[doc-index]: http://docs.datastax.com/en/developer/nodejs-driver/latest/
[doc-datatypes]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/datatypes/
[doc-numerical]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/datatypes/numerical/
[doc-uuid]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/datatypes/uuids/
[doc-collections]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/datatypes/collections/
[doc-udt]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/datatypes/udts/
[doc-promise-callback]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/promise-callback/
[faq]: http://docs.datastax.com/en/developer/nodejs-driver/latest/faq/
[load-balancing]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/tuning-policies/#load-balancing-policy
[retry]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/tuning-policies/#retry-policy
[pooling]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/connection-pooling/
[batch]: http://docs.datastax.com/en/developer/nodejs-driver/latest/features/batch/
[upgrade1]: https://github.com/datastax/nodejs-driver/blob/master/doc/upgrade-guide-2.0.md
[old-driver]: https://github.com/jorgebay/node-cassandra-cql
[jorgebay]: https://github.com/jorgebay
[drivers]: https://github.com/datastax
[mailinglist]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[jira]: https://datastax-oss.atlassian.net/projects/NODEJS/issues
[streams2]: http://nodejs.org/api/stream.html#stream_class_stream_readable
[cql-udt]: http://cassandra.apache.org/doc/latest/cql/types.html#udts
[survey]: http://goo.gl/forms/f216tY3Ebr
[dse-driver]: https://github.com/datastax/nodejs-dse-driver
[slack]: https://academy.datastax.com/slack
[dse]: https://www.datastax.com/products/datastax-enterprise
