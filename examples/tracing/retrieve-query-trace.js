"use strict";
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Creates a table and retrieves its information
 */
client.connect()
  .then(function () {
    const query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication =" +
      "{'class': 'SimpleStrategy', 'replication_factor': '1' }";
    return client.execute(query);
  })
  .then(function () {
    const query = "CREATE TABLE IF NOT EXISTS examples.trace_tbl1 (id uuid, txt text, PRIMARY KEY(id))";
    return client.execute(query);
  })
  .then(function () {
    const query = "INSERT INTO examples.trace_tbl1 (id, txt) VALUES (?, ?)";
    return client.execute(query, [cassandra.types.Uuid.random(), 'hello trace'], { traceQuery: true});
  })
  .then(function (result) {
    const traceId = result.info.traceId;
    return client.metadata.getTrace(traceId);
  })
  .then(function (trace) {
    console.log('Trace for the execution of the query:');
    console.log(trace);
    console.log('The trace was retrieved successfully');
    client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error', err);
    return client.shutdown();
  });