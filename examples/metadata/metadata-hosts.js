"use strict";
const dse = require('dse-driver');

const client = new dse.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });
client.connect()
  .then(function () {
    console.log('Connected to cluster with %d host(s): %j', client.hosts.length);
    client.hosts.forEach(function (host) {
      console.log('Host %s v%s on rack %s, dc %s', host.address, host.cassandraVersion, host.rack, host.datacenter);
    });
    console.log('Shutting down');
    return client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error when connecting', err);
    return client.shutdown().then(() => { throw err; });
  });