# Three simple rules for coding with the driver

When writing code that uses the driver, there are three simple rules that you should follow that make your code
efficient:

- Only use one `Client` instance per keyspace or use a single Client and explicitly specify the keyspace in your queries
and reuse it in across your modules in the application lifetime.
- If you execute a statement more than once, use a prepared statement.
- In some situations you can reduce the number of network roundtrips and also have atomic operations by using batches.

## Client 

The `Client` instance allows you to configure different important aspects of the way connections and queries are
handled. At this level, you can configure everything from contact points (address of the nodes to be contacted initially
before the driver performs node discovery), the request routing policy, retry and reconnection policies, and so on.
Generally such settings are set once at the application level.

```javascript
const dse = require('dse-driver');
const DCAwareRoundRobinPolicy = dse.policies.loadBalancing.DCAwareRoundRobinPolicy;
const client = new dse.Client({
   contactPoints: ['10.1.1.3', '10.1.1.4', '10.1.1.5'], 
   localDataCenter: 'eu-west-3'
});
```

A `Client` instance is a long-lived object, and it should not be used in a request-response, short-lived fashion.

Your code should share the same `Client` instance across your application.

## Prepared statements 

Using prepared statements provides multiple benefits. A prepared statement is parsed and prepared on the Cassandra nodes
and is ready for future execution. When binding parameters are provided, only they (and the query id) are sent over the wire. These
performance gains add up when using the same queries (with different parameters) repeatedly. Additionally, when
preparing, the driver retrieves information about the parameter types which allows an accurate mapping between a
JavaScript type and a CQL type.

Preparing and executing statements in the driver does not require two chained asynchronous calls. You can set the
`prepare` flag in the query options and the driver handles the rest.

```javascript
const query = 'SELECT id, name FROM users WHERE id = ?';
client.execute(query, [ id ], { prepare: true }, callback);
```

## Batch statements 

The batch statement combines multiple data modification statements (`INSERT`, `UPDATE`, or `DELETE`) into a single logical
operation that is sent to the server in a single request. Batching together multiple operations also ensures that they
are executed in an atomic way, (that is, either all succeed or none). To make the best use of `batch()`, read about
[atomic batches in Cassandra 1.2](http://www.datastax.com/dev/blog/atomic-batches-in-cassandra-1-2), [static columns
and batching of conditional updates](http://www.datastax.com/dev/dev/blog/cql-in-2-0-6),
and [CQL documentation][batches].  But take into account that incorrect use of batch statements may increase load to servers.

Starting with Cassandra 2.0, prepared statements can be used in batch operations.

```javascript
const queries = [
   { query: 'UPDATE user_profiles SET email=? WHERE key=?',
      params: [emailAddress, 'hendrix']},
   { query: 'INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)',
      params: ['hendrix', 'Changed email', new Date()]}
];
const queryOptions = { prepare: true, consistency: dse.types.consistencies.quorum };
client.batch(queries, queryOptions, function(err) {
   assert.ifError(err);
   console.log('Data updated on cluster');
});
```

[batches]: https://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatchTOC.html
