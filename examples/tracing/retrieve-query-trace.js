"use strict";
var cassandra = require('cassandra-driver');
var async = require('async');
var assert = require('assert');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1']});

/**
 * Creates a table and retrieves its information
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
    var query = "CREATE TABLE IF NOT EXISTS examples.trace_tbl1 (id uuid, txt text, PRIMARY KEY(id))";
    client.execute(query, next);
  },
  function retrieveTrace(next) {
    var query = 'INSERT INTO examples.trace_tbl1 (id, txt) VALUES (?, ?)';
    client.execute(query, [cassandra.types.Uuid.random(), 'hello trace'], { traceQuery: true}, function (err, result) {
      if (err) return next(err);
      var traceId = result.info.traceId;
      client.metadata.getTrace(traceId, function (err, trace) {
        console.log('Trace for the execution of the query: "%s":', query);
        console.log(trace);
        next();
      });
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});