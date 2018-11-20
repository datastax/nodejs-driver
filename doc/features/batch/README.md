## Batch statements

It's common for applications to require atomic batching of multiple `INSERT`, `UPDATE`, or `DELETE` statements, even in
different partitions or column families. Thanks to the Cassandra protocol changes introduced in Cassandra 2.0, the
driver allows you to execute multiple statements efficiently without the need to concatenate multiple queries.

The method `batch()` accepts the queries as first parameter:

```javascript
const query1 = 'UPDATE user_profiles SET email = ? WHERE key = ?';
const query2 = 'INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)';
const queries = [
   { query: query1, params: [emailAddress, 'hendrix'] },
   { query: query2, params: ['hendrix', 'Changed email', new Date()] } 
];
// Promise-based call
client.batch(queries, { prepare: true })
  .then(function() {
    // All queries have been executed successfully
  })
  .catch(function(err) {
    // None of the changes have been applied
  });
```

Or using the callback-based invocation

```javascript
client.batch(queries, { prepare: true }, function (err) {
   // All queries have been executed successfully
   // Or none of the changes have been applied, check err
});
```

By preparing your queries, you will get the best performance and your JavaScript parameters correctly mapped to 
Cassandra types. The driver will prepare each query once on each host and execute the batch every time with the
different parameters provided.

Note that Cassandra batches are not suitable for bulk loading, there are dedicated tools for that. Batches allow you
to group related updates in a single request, so keep the batch size small (in the order of tens).
Starting from Cassandra version 2.0.8, the server issues a warning if the batch size is greater than 5K.
Refer to [CQL documentation][batches] for information about correct and incorrect use of batches.

[batches]: https://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatchTOC.html
