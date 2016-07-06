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
    var query = "CREATE TABLE IF NOT EXISTS examples.meta_tbl1 (id1 uuid, id2 timeuuid, txt text, val int, PRIMARY KEY(id1, id2))";
    client.execute(query, next);
  },
  function retrieveMetadata(next) {
    client.metadata.getTable('examples', 'meta_tbl1', function (err, table) {
      if (err) return next(err);
      console.log('Table information');
      console.log('- Name: %s', table.name);
      console.log('- Columns:', table.columns);
      console.log('- Partition keys:', table.partitionKeys);
      console.log('- Clustering keys:', table.clusteringKeys);
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