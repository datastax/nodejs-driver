"use strict";
var cassandra = require('cassandra-driver');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

client.connect(function (err) {
  if (err) {
    client.shutdown();
    return console.error('There was an error when connecting', err);
  }
  console.log('Connected, listing keyspaces:');
  for (var name in client.metadata.keyspaces) {
    if (!client.metadata.keyspaces.hasOwnProperty(name)) continue;
    var keyspace = client.metadata.keyspaces[name];
    console.log('- %s:\n\tstrategy %s\n\tstrategy options %j', keyspace.name, keyspace.strategy,  keyspace.strategyOptions);
  }
  console.log('Shutting down');
  client.shutdown();
});