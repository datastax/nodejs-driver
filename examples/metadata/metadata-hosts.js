"use strict";
var cassandra = require('cassandra-driver');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

client.connect(function (err) {
  if (err) {
    client.shutdown();
    return console.error('There was an error when connecting', err);
  }
  console.log('Connected to cluster with %d host(s): %j', client.hosts.length);
  client.hosts.forEach(function (host) {
    console.log('Host %s v%s on rack %s, dc %s', host.address, host.cassandraVersion, host.rack, host.datacenter);
  });
  console.log('Shutting down');
  client.shutdown();
});