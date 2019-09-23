# Getting started

Getting started with the DataStax Node.js driver for Apache Cassandra.

## Connecting to a cluster

To connect to an Apache Cassandra cluster, you need to provide the address or host name of at least one node
in the cluster and the local data center name.  The driver will discover all the nodes in the cluster after
it connects to one node in given list.
 
Typically, you should create only a single `Client` instance for a given Cassandra cluster and use it across
your application.

```javascript
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ 
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1'
});

client.connect()
  .then(() => console.log('Connected!'));
```

At this point, the driver will be connected to all the nodes in the local data center and discovered the rest
of the nodes in your cluster.

Even though calling `#connect()` is not required (the execute method internally calls to connect), it is recommended you
call to `#connect()` on application startup, this way you can ensure that you start your app once your are connected to
your cluster.

## Retrieving data

The `#execute()` method can be used to send a CQL query to a Cassandra node, a simple way to use would be to provide a
query.

```javascript
const query = "SELECT name, email, birthdate FROM users WHERE key = 'mick-jagger'";

client.execute(query)
  .then(result => {
    const row = result.first();
    
    // The row is an Object with column names as property keys. 
    console.log('My name is %s and my email is %s', row.name, row.email);
  });
```

### Using query parameters and prepared statements

Instead of hard coding your parameters in your query, you can use parameter markers in your queries and provide the
parameters as an Array.

```javascript
const query = 'SELECT name, email, birthdate FROM users WHERE key = ?';
client.execute(query, ['mick-jagger']);
```

This way you can reuse the query and forget about escaping / stringifying the parameters in your query. 

Additionally, if you plan to reuse a query within your application (it is generally the case, your parameter value
changes but there is only a small number of different queries for a given schema), **you can benefit from using prepared
statements**.
 
Using prepared statements increases performance compared to plain executes, especially for repeated queries, as the query
only needs to be parsed once by the Cassandra node. It has the **additional benefit of providing metadata of the
parameters to the driver, allowing better type mapping between JavaScript and Cassandra** without the need of
additional info (hints) from the user.

```javascript
const query = 'SELECT name, email, birthdate FROM users WHERE key = ?';

// Set the prepare flag in your queryOptions
client.execute(query, ['mick-jagger'], { prepare: true });
```

See the [data types documentation to see how CQL types are mapped to JavaScript types][datatypes]. 

## Inserting data

You can use the `#execute()` method to execute any CQL query.

```javascript
const query = 'INSERT INTO users (key, name, email, birthdate) VALUES (?, ?, ?)';
const params = ['mick-jagger', 'Sir Mick Jagger', 'mick@rollingstones.com', new Date(1943, 6, 26)];

client.execute(query, params, { prepare: true });
```

### Setting the consistency level

To specify how consistent the data must be for a given read or write operation, you can set the
[consistency level][consistency] per query

```javascript
const { types } = cassandra;

client.execute(query, params, { consistency: types.consistencies.localQuorum, prepare: true })
  .then(() => {
    // The Promise will be resolved once it has been written in the number of replicas
    // satisfying the consistency level specified.
  });
```

Or you can provide a default consistency level for all your queries when creating the `Client` instance (defaults to
`localOne`).

```javascript
const client = new Client({
  contactPoints,
  localDataCenter, 
  queryOptions: { consistency: types.consistencies.localQuorum }
});
```

## Mapper

The driver provides a built-in [object mapper](../features/mapper) that lets you interact with your data like you would 
interact with a set of documents.

Retrieving objects from the database:

```javascript
const videos = await videoMapper.find({ userId });

for (let video of videos) {
  console.log(video.name);
}
```

Updating an object from the database:

```javascript
await videoMapper.update({ id, userId, name, addedDate, description });
```

You can read more information about [getting started with the Mapper in our
documentation](../features/mapper/getting-started).

## Authentication

You can set the credentials to connect to an Apache Cassandra cluster secured using a `PasswordAuthenticator` or to a
DSE cluster secured with `DseAuthenticator`, with plain-text authentication as default scheme.

```javascript
const client = new cassandra.Client({
  contactPoints,
  localDataCenter,
  credentials: { username: 'my_username', password: 'my_p@ssword1!' }
});
```

[consistency]: https://docs.datastax.com/en/dse/6.0/dse-arch/datastax_enterprise/dbInternals/dbIntConfigConsistency.html
[datatypes]: /features/datatypes/
