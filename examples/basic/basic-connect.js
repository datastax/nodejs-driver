"use strict";
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });
client.connect()
  .then(function () {
    console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
    console.log('Keyspaces: %j', Object.keys(client.metadata.keyspaces));
    console.log('Shutting down');
    return client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error when connecting', err);
    return client.shutdown().then(() => { throw err; });
  });