"use strict";
const dse = require('dse-driver');
const async = require('async');
const assert = require('assert');

const client = new dse.Client({ contactPoints: ['127.0.0.1']});

/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 * Alternately you can use the Promise-based API.
 *
 * Inserts a row and retrieves a row
 */
const id = dse.types.Uuid.random();

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    const query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
  },
  function createTable(next) {
    const query = "CREATE TABLE IF NOT EXISTS examples.basic (id uuid, txt text, val int, PRIMARY KEY(id))";
    client.execute(query, next);
  },
  function insert(next) {
    const query = 'INSERT INTO examples.basic (id, txt, val) VALUES (?, ?, ?)';
    client.execute(query, [ id, 'Hello!', 100 ], { prepare: true}, next);
  },
  function select(next) {
    const query = 'SELECT id, txt, val FROM examples.basic WHERE id = ?';
    client.execute(query, [ id ], { prepare: true}, function (err, result) {
      if (err) return next(err);
      const row = result.first();
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