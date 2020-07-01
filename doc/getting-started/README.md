# Getting started

Getting started with the DataStax Node.js driver for Apache Cassandra.

## Connecting to a cluster

To connect to an Apache Cassandra cluster, you need to provide the address or host name of at least one node
in the cluster and the local data center (DC) name.  The driver will discover all the nodes in the cluster and
connect to all the nodes in the local data center.
 
Typically, you should create only a single `Client` instance for a given Cassandra cluster and use it across
your application.

```javascript
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ 
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1'
});

client.connect();
```

At this point, the driver will be connected to all the nodes in the local data center and discovered the rest
of the nodes in your cluster.

Even though calling `connect()` is not required (the `execute()` method internally calls to connect), it is
recommended you call to `#connect()` on application startup, this way you can ensure that you start your app once
your are connected to your cluster.

When using [DataStax Astra][astra] you can configure your client by setting the secure bundle and the user credentials:

```javascript
const client = new cassandra.Client({
  cloud: { secureConnectBundle: 'path/to/secure-connect-DATABASE_NAME.zip' },
  credentials: { username: 'user_name', password: 'p@ssword1' }
});
```

## Retrieving data

The `execute()` method can be used to send a CQL query to a Cassandra node.

```javascript
const query = "SELECT name, email, birthdate FROM users WHERE key = 'mick-jagger'";
client.execute(query)
  .then(result => {
    const row = result.first();
    
    // The row is an Object with column names as property keys. 
    console.log('My name is %s and my email is %s', row['name'], row['email']);
  });
```

Execution methods in the driver return a `Promise`, you can await on the promise to be fulfilled using [async
functions][async-functions]. Note that for the rest of the documentation, Promise method `then()` and `await` will be
used interchangeably.

### Using query parameters and prepared statements

Instead of hard-coding your parameters in your query, you can use parameter markers in your queries and provide the
parameters as an Array.

```javascript
const query = 'SELECT name, email, birthdate FROM users WHERE key = ?';
const result = await client.execute(query, ['mick-jagger']);
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
// Recommended: use query markers for parameters
const query = 'SELECT name, email, birthdate FROM users WHERE key = ?';

// Recommended: set the prepare flag in your queryOptions
const result = await client.execute(query, ['mick-jagger'], { prepare: true });
```

See the [data types documentation to see how CQL types are mapped to JavaScript types][datatypes]. 

## Inserting data

You can use the `#execute()` method to execute any CQL query.

```javascript
const query = 'INSERT INTO users (key, name, email, birthdate) VALUES (?, ?, ?)';
const params = ['mick-jagger', 'Sir Mick Jagger', 'mick@rollingstones.com', new Date(1943, 6, 26)];

await client.execute(query, params, { prepare: true });
```

The promise is fulfilled when the data is inserted.

### Setting the consistency level

To specify how consistent the data must be for a given read or write operation, you can set the
[consistency level][consistency] per query.

```javascript
const { types } = cassandra;

await client.execute(query, params, { consistency: types.consistencies.quorum });
```

The promise is fulfilled when the data has been written in the number of replicas satisfying the consistency level
specified.

You can also provide a default consistency level for all your queries when creating the `Client` instance (defaults to
`localOne`).

```javascript
const client = new Client({
  queryOptions: { consistency: types.consistencies.localQuorum },
  // ... rest of the options
});
```

## Mapper (optional)

The driver provides [a built-in object mapper][mapper] that lets you interact with your data like you would interact
with a set of documents.

```javascript
const userVideos = await videoMapper.find({ userId });
for (let video of userVideos) {
  console.log(video.name);
}
```

Visit the [Getting Started with the Mapper Guide][mapper-guide] for more information. 

## Authentication (optional)

Using an authentication provider on an auth-enabled Cassandra cluster:

```javascript
const authProvider = new cassandra.auth.PlainTextAuthProvider('my_user', 'p@ssword1!');

//Set the auth provider in the clientOptions when creating the Client instance
const client = new Client({ contactPoints, localDataCenter, authProvider });
```

## Working with mixed workloads

The driver features [Execution Profiles](../features/execution-profiles) that provide a mechanism to group together
a set of configuration options and reuse them across different query executions.

[Execution Profiles](../features/execution-profiles) are specially useful when dealing with different workloads like
Graph and CQL workloads, allowing you to use a single `Client` instance for all workloads, for example:

```javascript
const client = new cassandra.Client({ 
  contactPoints: ['host1'],
  localDataCenter: 'oltp-us-west',
  profiles: [
    new ExecutionProfile('time-series', {
      consistency: consistency.localOne,
      readTimeout: 30000,
      serialConsistency: consistency.localSerial
    }),
    new ExecutionProfile('graph', {
      loadBalancing: new DefaultLoadBalancingPolicy('graph-us-west'),
      consistency: consistency.localQuorum,
      readTimeout: 10000,
      graphOptions: { name: 'myGraph' }
    })
  ]
});

// Use an execution profile for a CQL query
client.execute('SELECT * FROM system.local', null, { executionProfile: 'time-series' });

// Use an execution profile for a gremlin query
client.executeGraph('g.V().count()', null, { executionProfile: 'graph' });
```

[consistency]: https://docs.datastax.com/en/dse/6.7/dse-arch/datastax_enterprise/dbInternals/dbIntConfigConsistency.html
[datatypes]: /features/datatypes/
[mapper]: /features/mapper/
[mapper-guide]: /features/mapper/getting-started/
[async-functions]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function 
[astra]: https://www.datastax.com/products/datastax-astra