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
import assert from "assert";
import types from "../../../../lib/types/index";
import helper from "../../../test-helper";
import tableMappingsModule from "../../../../lib/mapping/table-mappings";
import Mapper from "../../../../lib/mapping/mapper";
import Client from "../../../../lib/client";
import utils from "../../../../lib/utils";


const Uuid = types.Uuid;
const UnderscoreCqlToCamelCaseMappings = tableMappingsModule.UnderscoreCqlToCamelCaseMappings;
const videoColumnsToProperties = new Map([ ['videoid', 'id'], ['userid', 'userId'], ['added_date', 'addedDate'],
  ['location_type', 'locationType'], ['preview_image_location', 'preview'], ['preview_thumbnails', 'thumbnails']]);

let hasBeenSetup = false;

const mapperHelper = {
  setupOnce: function (testInstance) {

    testInstance.timeout(60000);

    if (hasBeenSetup) {
      return;
    }

    hasBeenSetup = true;

    const queries = [
      // Create tables
      `CREATE TABLE videos (videoid uuid, userid uuid, name varchar, description varchar, location text,
      location_type int, preview_thumbnails map<text,text>, tags set<varchar>, added_date timestamp,
      PRIMARY KEY (videoid))`,
      `CREATE TABLE user_videos (userid uuid, added_date timestamp, videoid uuid, name text,
      preview_image_location text, PRIMARY KEY (userid, added_date, videoid))
      WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,
      `CREATE TABLE latest_videos (yyyymmdd text, added_date timestamp, videoid uuid, name text,
      preview_image_location text, PRIMARY KEY (yyyymmdd, added_date, videoid))
      WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,
      `CREATE TABLE users (userid uuid, firstname varchar, lastname varchar, email text, created_date timestamp,
      PRIMARY KEY (userid))`,
      `CREATE TABLE video_rating (videoid uuid, rating_counter counter, rating_total counter, PRIMARY KEY (videoid))`,
      `CREATE TABLE table_clustering1 (id1 text, id2 text, id3 text, value text, PRIMARY KEY (id1, id2, id3))`,
      `CREATE TABLE table_clustering2 (id1 text, id3 text, id2 text, value text, PRIMARY KEY (id1, id3, id2))`,
      `CREATE TABLE table_static1 (id1 text, id2 text, s text static, value text, PRIMARY KEY (id1, id2))`,
      `CREATE TABLE table_static2 (id1 text, id2 text, s0 text static, s1 text static, value text, PRIMARY KEY (id1, id2))`,

      // Insert test data
      `INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags,
     added_date) VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,'My funny cat',
     d0f60aa8-54a9-4840-b70c-fe562b68842b, 'My cat likes to play the piano! So funny.',
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401',1,{'10':'/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401'},
     {'cats','piano','lol'},'2012-06-01 06:00:00Z')`,
      `INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location) VALUES 
     (d0f60aa8-54a9-4840-b70c-fe562b68842b,99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 06:00:00Z','My funny cat',
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401')`,
      `INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location) VALUES ('2012-06-01',
     99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 06:00:00Z','My funny cat',
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');`,
      `INSERT INTO table_clustering1 (id1, id2, id3, value) VALUES ('a', 'b', 'c', 'value_abc_table1')`,
      `INSERT INTO table_clustering1 (id1, id2, id3, value) VALUES ('a', 'z', 'z', 'value_azz_table1')`,
      `INSERT INTO table_clustering2 (id1, id2, id3, value) VALUES ('a', 'b', 'c', 'value_abc_table2')`,
      `INSERT INTO table_clustering2 (id1, id2, id3, value) VALUES ('a', 'z', 'z', 'value_azz_table2')`
    ];

    helper.setup(1, { queries, keyspace: mapperHelper.keyspace, removeClusterAfter: false });
  },
  videoIds: [ Uuid.fromString('99051fe9-6a9c-46c2-b949-38ef78858dd0') ],
  userIds: [ Uuid.fromString('d0f60aa8-54a9-4840-b70c-fe562b68842b') ],
  addedDates: [ new Date('2012-06-01T06:00:00Z') ],
  yyyymmddBuckets: ['2012-06-01'],
  keyspace: 'ks_mapper_killrvideo',

  /**
   * Gets the rows matching the doc
   * @param {Client} client
   * @param {Object} doc
   * @param {String} [columns='*']
   * @returns {Promise<Array<Row>>}
   */
  getVideoRows: function (client, doc, columns) {
    columns = columns || '*';

    const queries = [
      [`SELECT ${columns} FROM videos WHERE videoid = ?`, [doc.id]]
    ];

    if (doc.userId && doc.addedDate) {
      queries.push([`SELECT ${columns} FROM user_videos WHERE userid = ? AND added_date = ? AND videoid = ?`,
        [doc.userId, doc.addedDate, doc.id]]);
    }

    if (doc.yyyymmdd && doc.addedDate) {
      queries.push([`SELECT ${columns} FROM latest_videos WHERE yyyymmdd = ? AND added_date = ? AND videoid = ?`,
        [doc.yyyymmdd, doc.addedDate, doc.id]]);
    }

    return Promise.all(queries.map(q => client.execute(q[0], q[1], {prepare: true})))
      .then(results => results.map(rs => rs.first()));
  },
  insertVideoRows: function (client, doc) {
    const queries = [{
      query: 'INSERT INTO videos (videoid, userid, added_date, name, description) VALUES (?, ?, ?, ?, ?)',
      params: [doc.id, doc.userId, doc.addedDate, doc.name, doc.description ]
    }];

    if (doc.addedDate && doc.userId) {
      queries.push({
        query: 'INSERT INTO user_videos (videoid, userid, added_date, name) VALUES (?, ?, ?, ?)',
        params: [doc.id, doc.userId, doc.addedDate, doc.name]
      });
    }

    if (doc.yyyymmdd) {
      queries.push({
        query: 'INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name) VALUES (?, ?, ?, ?)',
        params: [doc.yyyymmdd, doc.id, doc.addedDate, doc.name]
      });
    }

    return client.batch(queries, { prepare: true });
  },

  getMapper: function (options) {
    const keyspace = mapperHelper.keyspace;
    const client = new Client(utils.extend({ keyspace }, helper.baseOptions));
    before(() => client.connect());
    after(() => client.shutdown());

    const videoColumns = {};
    videoColumnsToProperties.forEach((v, k) => videoColumns[k] = v);

    return new Mapper(client, options || {
      models: {
        'Video': {
          tables: [
            { name: 'videos', isView: false },
            { name: 'user_videos', isView: false },
            { name: 'latest_videos', isView: false }
          ],
          columns: videoColumns
        },
        'User': {
          tables: [{ name: 'users', isView: false }],
          columns: {
            'userid': 'id',
            'firstname': 'firstName',
            'lastname': 'lastName'
          },
          mappings: new UnderscoreCqlToCamelCaseMappings()
        },
        'VideoRating': {
          tables: ['video_rating'],
          columns: {
            'videoid': 'id'
          },
          mappings: new UnderscoreCqlToCamelCaseMappings()
        },
        'Clustering': { tables: ['table_clustering1', 'table_clustering2'] },
        'Static': { tables: ['table_static1'] },
        'Static2': { tables: ['table_static2'] }
      }
    });
  },

  getPropertyName: function (columnName) {
    const mappedName = videoColumnsToProperties.get(columnName);
    return mappedName === undefined ? columnName : mappedName;
  },

  assertRowMatchesDoc: function (row, doc) {
    assert.ok(row);
    Object.keys(row).forEach(columnName => {
      assert.deepEqual(row[columnName], doc[mapperHelper.getPropertyName(columnName)]);
    });
  }
};


export default mapperHelper;