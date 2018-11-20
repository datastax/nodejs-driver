"use strict";
const dse = require('dse-driver');

const client = new dse.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });

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
    const query = "CREATE TABLE IF NOT EXISTS examples.meta_tbl1" +
      " (id1 uuid, id2 timeuuid, txt text, val int, PRIMARY KEY(id1, id2))";
    return client.execute(query);
  })
  .then(function () {
    return client.metadata.getTable('examples', 'meta_tbl1');
  })
  .then(function (table) {
    console.log('Table information');
    console.log('- Name: %s', table.name);
    console.log('- Columns:', table.columns);
    console.log('- Partition keys:', table.partitionKeys);
    console.log('- Clustering keys:', table.clusteringKeys);
    console.log('Shutting down');
    return client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error', err);
    return client.shutdown().then(() => { throw err; });
  });