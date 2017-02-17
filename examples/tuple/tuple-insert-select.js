"use strict";
var cassandra = require('cassandra-driver');
var async = require('async');
var assert = require('assert');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Creates a table with a Tuple type, inserts a row and selects a row.
 */
client.connect()
  .then(function () {
    const query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication =" +
      "{'class': 'SimpleStrategy', 'replication_factor': '1' }";
    return client.execute(query);
  })
  .then(function () {
    const query = "CREATE TABLE IF NOT EXISTS examples.tuple_forex " +
      "(name text, time timeuuid, currencies frozen<tuple<text, text>>, value decimal, PRIMARY KEY (name, time))";
    return client.execute(query);
  })
  .then(function () {
    console.log('Inserting');
    // Create a new instance of a Tuple
    const currencies = new cassandra.types.Tuple('USD', 'EUR');
    const query = 'INSERT INTO examples.tuple_forex (name, time, currencies, value)  VALUES (?, ?, ?, ?)';
    const params = [ 'market1', cassandra.types.TimeUuid.now(), currencies, new cassandra.types.BigDecimal(11, 1) ];
    return client.execute(query, params, { prepare: true});
  })
  .then(function () {
    const query = 'SELECT name, time, currencies, value FROM examples.tuple_forex where name = ?';
    return client.execute(query, ['market1'], { prepare: true });
  })
  .then(function (result) {
    const row = result.first();
    console.log('%s to %s: %s', row['currencies'].get(0), row['currencies'].get(1), row['value']);
    return client.shutdown();
  })
  .catch(function (err) {
    console.error('There was an error', err);
    return client.shutdown();
  });