"use strict";
const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');

const client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 * Alternately you can use the Promise-based API.
 *
 * Inserts a row and retrieves a row
 */
const id = cassandra.types.Uuid.random();

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    var query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
  },
  function createTable(next) {
    var query = "CREATE TABLE IF NOT EXISTS examples.basic (id uuid, txt text, val int, PRIMARY KEY(id))";
    client.execute(query, next);
  },
  function insert(next) {
    var query = 'INSERT INTO examples.basic (id, txt, val) VALUES (?, ?, ?)';
    client.execute(query, [ id, 'Hello!', 100 ], { prepare: true}, next);
  },
  function select(next) {
    var query = 'SELECT id, txt, val FROM examples.basic WHERE id = ?';
    client.execute(query, [ id ], { prepare: true}, function (err, result) {
      if (err) return next(err);
      var row = result.first();
      console.log('Obtained row: ', row);
      assert.strictEqual(row.id.toString(), id.toString());
      assert.strictEqual(row.txt, 'Hello!');
      assert.strictEqual(row.val, 100);
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