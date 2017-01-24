/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const dse = require('dse-driver');
const async = require('async');
const Point = dse.geometry.Point;

const client = new dse.Client({ contactPoints: ['127.0.0.1']});

/**
 * Example using async library for avoiding nested callbacks
 * See https://github.com/caolan/async
 *
 * Inserts a row containing a PointType and retrieves the row.
 */

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    const query = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    client.execute(query, next);
  },
  function createTable(next) {
    const query = "CREATE TABLE IF NOT EXISTS examples.geotypes (name text PRIMARY KEY, coords 'PointType')";
    client.execute(query, next);
  },
  function insert(next) {
    const query = 'INSERT INTO examples.geotypes (name, coords) VALUES (?, ?)';
    client.execute(query, ['Eiffel Tower', new Point(48.8582, 2.2945)], { prepare: true }, next);
  },
  function select(next) {
    const query = 'SELECT name, coords FROM examples.geotypes WHERE name = ?';
    client.execute(query, ['Eiffel Tower'], { prepare: true}, function (err, result) {
      if (err) {
        return next(err);
      }
      const row = result.first();
      console.log('Obtained row: ', row);
      const p = row.coords;
      console.log('"Coords" column value is instance of Point: %s', p instanceof Point);
      console.log('Accessing point properties: x = %d, y = %d', p.x, p.y);
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