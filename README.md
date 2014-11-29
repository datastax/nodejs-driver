# DataStax Node.js Driver for Apache Cassandra

Node.js driver for [Apache Cassandra][cassandra]. This driver works exclusively with the Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol.

## Installation

```bash
$ npm install cassandra-driver
```

[![Build Status](https://travis-ci.org/datastax/nodejs-driver.svg?branch=master)](https://travis-ci.org/datastax/nodejs-driver)

## Features

- Node discovery
- Configurable load balancing
- Transparent failover
- Tunability
- Paging
- Client-to-node SSL support
- Row streaming
- Prepared statements and query batches

## Documentation

- [Documentation index][doc-index]
- [CQL types to javascript types][doc-datatypes]
- [API docs][doc-api]
- [FAQ][faq]

## Getting Help

You can use the [project mailing list][mailinglist] or create a ticket on the [Jira issue tracker][jira].

## Basic usage

```javascript
var cassandra = require('cassandra-driver');
var client = new cassandra.Client({contactPoints: ['host1', 'h2'], keyspace: 'ks1'});
var query = 'SELECT email, last_name FROM user_profiles WHERE key=?';
client.execute(query, ['guy'], function(err, result) {
  assert.ifError(err);
  console.log('got user profile with email ' + result.rows[0].email);
});
```

### Prepare your queries

Use prepared statements to obtain best performance. The driver will prepare the query once on each host and
 execute the statement with the bound parameters.

```javascript
//When using parameter markers ? the prepared query will be reused
var query = 'UPDATE user_profiles SET birth=? WHERE key=?'; 
var params = [new Date(1942, 10, 1), 'jimi-hendrix'];
//Set the prepare flag in the query options
client.execute(query, params, {prepare: true}, function(err) {
  assert.ifError(err);
  console.log('Row updated on the cluster');
});
```

### Avoid buffering

When using `#eachRow()` and `#stream()` methods, the driver parses each row as soon as it is received,
 yielding rows without buffering them.

```javascript
//Reducing a large result
client.eachRow('SELECT time, val FROM temperature WHERE station_id=', ['abc'],
  function(n, row) {
    //the callback will be invoked per each row as soon as they are received
    minTemperature = Math.min(row.val, minTemperature);
  },
  function (err) {
    assert.ifError(err);
  }
);
```

The `#stream()` method works in the same way but instead of callback it returns a [Readable Streams2][streams2] object
 in `objectMode` that emits instances of `Row`. It can be **piped** downstream and provides automatic pause/resume logic (it buffers when not read).

### Paging

All driver methods use a default `fetchSize` of 5000 rows, retrieving only first page of results up to a
 maximum of 5000 rows to shield an application against accidentally large result sets. To retrieve the following
 records you can use the `autoPage` flag in the query options of `#eachRow()` and `#stream()` methods.

```javascript
//Imagine a column family with millions of rows
var query = 'SELECT * FROM largetable';
client.eachRow(query, [], {autoPage: true}, function (n, row) {
  //This function will be called per each of the rows in all the table
}, endCallback);
```

### Batch multiple statements

You can execute multiple statements in a batch to update/insert several rows atomically even in different column families.

```javascript
var queries = [
  {
    query: 'UPDATE user_profiles SET email=? WHERE key=?',
    params: [emailAddress, 'hendrix']
  },
  {
    query: 'INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)',
    params: ['hendrix', 'Changed email', new Date()]
  }
];
var queryOptions = { consistency: cassandra.types.consistencies.quorum };
client.batch(queries, queryOptions, function(err) {
  assert.ifError(err);
  console.log('Data updated on cluster');
});
```

----

## Data types

There are few data types defined in the ECMAScript standard, this usually represents a problem when you are trying to
 deal with data types that come from other systems in javascript. 

You should read the [documentation on CQL data types and ECMAScript types][doc-datatypes] for more information.

## Logging

Instances of `Client()` are `EventEmitter` and emit `log` events:
```javascript
client.on('log', function(level, className, message, furtherInfo) {
  console.log('log event: %s -- %s', level, message);
});
```
The `level` being passed to the listener can be `verbose`, `info`, `warning` or `error`.

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
[doc-index]: http://www.datastax.com/documentation/developer/nodejs-driver/1.0/
[doc-datatypes]: http://www.datastax.com/documentation/developer/nodejs-driver/1.0/nodejs-driver/reference/nodejs2Cql3Datatypes.html
[doc-api]: http://www.datastax.com/drivers/nodejs/1.0/Client.html
[start]: http://datastax.github.io/nodejs-driver/getting-started
[faq]: http://www.datastax.com/documentation/developer/nodejs-driver/1.0/nodejs-driver/faq/njdFaq.html
[old-driver]: https://github.com/jorgebay/node-cassandra-cql
[jorgebay]: https://github.com/jorgebay
[drivers]: https://github.com/datastax
[mailinglist]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/nodejs-driver-user
[jira]: https://datastax-oss.atlassian.net/browse/NODEJS
[streams2]: http://nodejs.org/api/stream.html#stream_class_stream_readable
