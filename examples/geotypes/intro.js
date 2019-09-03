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

const cassandra = require('cassandra-driver');
const Point = cassandra.geometry.Point;

const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'dc1' });

/**
 * Inserts a row containing a PointType and retrieves the row.
 */

client.connect()
  .then(() => client.execute("CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }"))
  .then(() => client.execute("CREATE TABLE IF NOT EXISTS examples.geotypes (name text PRIMARY KEY, coords 'PointType')"))
  .then(() => {
    const query = 'INSERT INTO examples.geotypes (name, coords) VALUES (?, ?)';
    return client.execute(query, ['Eiffel Tower', new Point(48.8582, 2.2945)], { prepare: true });
  })
  .then(() => {

    const query = 'SELECT name, coords FROM examples.geotypes WHERE name = ?';
    return client.execute(query, ['Eiffel Tower'], { prepare: true});
  })
  .then(result => {
    const row = result.first();
    console.log('Obtained row: ', row);
    const p = row.coords;
    console.log('"Coords" column value is instance of Point: %s', p instanceof Point);
    console.log('Accessing point properties: x = %d, y = %d', p.x, p.y);
  })
  .finally(() => client.shutdown());

process.on('unhandledRejection', (reason) => { throw reason; });