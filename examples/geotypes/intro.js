/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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