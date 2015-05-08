"use strict";
var cassandra = require('../../');
//replace by    require('cassandra-driver');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Example using nested callbacks.
 * See basic-execute-flow.js for a more elegant example.
 */
client.connect(function (err) {
  if (err) {
    client.shutdown();
    return console.error('There was an error when connecting', err);
  }
  client.execute('SELECT * FROM system.local', function (err, result) {
    if (err) {
      client.shutdown();
      return console.error('There was while trying to retrieve data from system.local', err);
    }
    var row = result.rows[0];
    console.log('Obtained row: ', row);
    console.log('Shutting down');
    client.shutdown();
  });
});