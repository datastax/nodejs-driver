"use strict";
var cassandra = require('cassandra-driver');
var async = require('async');
var assert = require('assert');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Creates a table with a Tuple type, inserts a row and selects a row.
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 */
async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS examples.tuple_forex (name text, time timeuuid, currencies frozen<tuple<text, text>>, value decimal, PRIMARY KEY (name, time))";
    client.execute(query, next);
  },
  function insertData(next) {
    console.log('Inserting');
    //create a new instance of a Tuple
    var currencies = new cassandra.types.Tuple('USD', 'EUR');
    var query = 'INSERT INTO examples.tuple_forex (name, time, currencies, value)  VALUES (?, ?, ?, ?)';
    var params = ['market1', cassandra.types.TimeUuid.now(), currencies, new cassandra.types.BigDecimal(11, 1)];
    client.execute(query, params, { prepare: true}, next);
  },
  function selectingData(next) {
    var query = 'SELECT name, time, currencies, value FROM examples.tuple_forex where name = ?';
    client.execute(query, ['market1'], { prepare: true}, function (err, result) {
      if (err) return next(err);
      var row = result.first();
      console.log('%s to %s: %s', row.currencies.get(0), row.currencies.get(1), row.value);
      next();
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});