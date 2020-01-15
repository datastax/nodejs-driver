"use strict";
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'dc1' });

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
  })
  .finally(() => client.shutdown());


// Exit on unhandledRejection
process.on('unhandledRejection', (reason) => { throw reason; });