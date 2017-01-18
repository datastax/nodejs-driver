/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const dse = require('dse-driver');

const client = new dse.Client({ contactPoints: ['127.0.0.1']});

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
    const row = result.rows[0];
    console.log('Obtained row: ', row);
    console.log('Shutting down');
    client.shutdown();
  });
});