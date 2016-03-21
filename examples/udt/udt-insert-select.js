"use strict";
//replace by    require('cassandra-driver');
var cassandra = require('../../');
var async = require('neo-async');
var assert = require('assert');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Creates a table with a user-defined type, inserts a row and selects a row.
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
  function createType(next) {
    var query = "CREATE TYPE IF NOT EXISTS examples.address (street text, city text, state text, zip int, phones set<text>)";
    client.execute(query, next);
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS examples.udt_tbl1 (name text PRIMARY KEY, email text, address frozen<address>)";
    client.execute(query, next);
  },
  function insertData(next) {
    console.log('Inserting');
    var address = {
      city: 'Santa Clara',
      state: 'CA',
      street: '3975 Freedom Circle',
      zip: 95054,
      phones: ['650-389-6000']
    };
    var query = 'INSERT INTO examples.udt_tbl1 (name, address) VALUES (?, ?)';
    client.execute(query, ['The Rolling Stones', address], { prepare: true}, next);
  },
  function selectingData(next) {
    var query = 'SELECT name, address FROM examples.udt_tbl1 WHERE name = ?';
    client.execute(query, ['The Rolling Stones'], { prepare: true}, function (err, result) {
      if (err) return next(err);
      var row = result.first();
      console.log('Retrieved row: %j', row);
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