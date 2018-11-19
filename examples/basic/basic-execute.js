"use strict";
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });

/**
 * Example using Promise.
 * See basic-execute-flow.js for an example using callback-based execution.
 */
client.connect()
  .then(function () {
    return client.execute('SELECT * FROM system.local');
  })
  .then(function (result) {
    const row = result.rows[0];
    console.log('Obtained row: ', row);
    console.log('Shutting down');
    return client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error when connecting', err);
    return client.shutdown().then(() => { throw err; });
  });