'use strict';

const assert = require('assert');
const types = require('../../../../lib/types');
const Uuid = types.Uuid;
const helper = require('../../../test-helper');
const TableMappingInfo = require('../../../../lib/mapper/table-mapping-info');
const Mapper = require('../../../../lib/mapper/mapper');
const Client = require('../../../../lib/client');
const utils = require('../../../../lib/utils');

const colsToProps = new Map([ ['videoid', 'id'], ['userid', 'userId'], ['added_date', 'addedDate'],
  ['location_type', 'locationType'], ['preview_image_location', 'preview'], ['preview_thumbnails', 'thumbnails']]);

let hasBeenSetup = false;

const mapperHelper = module.exports = {
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
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');`
    ];

    helper.setup(1, { queries, keyspace: mapperHelper.keyspace, removeClusterAfter: false });
  },
  videoIds: [ Uuid.fromString('99051fe9-6a9c-46c2-b949-38ef78858dd0') ],
  userIds: [ Uuid.fromString('d0f60aa8-54a9-4840-b70c-fe562b68842b') ],
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

  getMapper: function () {
    const keyspace = mapperHelper.keyspace;
    const client = new Client(utils.extend({ keyspace }, helper.baseOptions));
    before(() => client.connect());
    after(() => client.shutdown());
    return new Mapper(client);
  },

  getVideosMappingInfo: function () {
    const tables = [
      { name: 'videos', isView: false },
      { name: 'user_videos', isView: false },
      { name: 'latest_videos', isView: false }
    ];

    return new TableMappingInfo(mapperHelper.keyspace, tables, null, colsToProps);
  },

  getUserModelMapper(mapper) {
    const tables = [
      { name: 'users', isView: false }
    ];

    const cols = new Map([['videoid', 'id'], ['userid', 'id'], ['created_date', 'createdDate'],
      ['firstname', 'firstName'], ['lastname', 'lastName']]);

    return mapper.forModel('User', new TableMappingInfo(mapperHelper.keyspace, tables, null, cols));
  },

  getPropertyName: function (columnName) {
    const mappedName = colsToProps.get(columnName);
    return mappedName === undefined ? columnName : mappedName;
  },

  assertRowMatchesDoc: function (row, doc) {
    assert.ok(row);
    Object.keys(row).forEach(columnName => {
      assert.deepEqual(row[columnName], doc[mapperHelper.getPropertyName(columnName)]);
    });
  }
};