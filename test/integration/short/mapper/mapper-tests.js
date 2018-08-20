'use strict';

const assert = require('assert');
const helper = require('../../../test-helper');
const Client = require('../../../../lib/client');
const utils = require('../../../../lib/utils');
const types = require('../../../../lib/types');
const Uuid = types.Uuid;
const Mapper = require('../../../../lib/mapper/mapper');
const TableMappingInfo = require('../../../../lib/mapper/table-mapping-info');
const q = require('../../../../lib/mapper/q').q;

const colsToProps = new Map([
  ['videoid', 'id'], ['userid', 'userId'], ['added_date', 'addedDate'], ['location_type', 'locationType'],
  ['preview_image_location', 'preview'], ['preview_thumbnails', 'thumbnails']]);

describe('Mapper', function () {
  this.timeout(60000);

  const queries = [
    `CREATE TABLE videos (videoid uuid, userid uuid, name varchar, description varchar, location text,location_type int,
     preview_thumbnails map<text,text>, tags set<varchar>, added_date timestamp,PRIMARY KEY (videoid))`,
    `CREATE TABLE user_videos (userid uuid, added_date timestamp, videoid uuid, name text, preview_image_location text,
     PRIMARY KEY (userid, added_date, videoid)) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,
    `CREATE TABLE latest_videos (yyyymmdd text, added_date timestamp, videoid uuid, name text,
     preview_image_location text, PRIMARY KEY (yyyymmdd, added_date, videoid))
     WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC)`,

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

  const videoIds = [ Uuid.fromString('99051fe9-6a9c-46c2-b949-38ef78858dd0') ];
  const userIds = [ Uuid.fromString('d0f60aa8-54a9-4840-b70c-fe562b68842b') ];
  const yyyymmddBuckets = ['2012-06-01'];

  const setupInfo = helper.setup(1, { queries });

  context('with videos tables', () => {
    // videos -> videoid
    // user_videos -> userid, added_date, videoid
    // latest_videos -> yyyymmdd, added_date, videoid
    const mapper = getMapper(setupInfo);
    const client = mapper.client;
    const videoMapper = mapper.forModel('Video', getVideosMappingInfo(setupInfo));

    describe('#find()', () => {
      it('should use the correct table', () => {
        const doc = { id: videoIds[0] };
        return videoMapper.find(doc, null, 'default').then(result => {
          assert.ok(result.first());
        });
      });

      it('should use the another table', () => {
        const doc = { userId: userIds[0] };
        return videoMapper.find(doc, { fields: ['id', 'userId', 'name'] }, 'default').then(result => {
          assert.ok(result.first());
        });
      });

      it('should use the provided fields and order by when defined', () => {
        const doc = { yyyymmdd: 'NOTEXISTENT' };
        return videoMapper.find(doc, { fields: ['id', 'yyyymmdd', 'name'], orderBy: { 'addedDate': 'asc'} }, 'default')
          .then(result => {
            assert.strictEqual(result.first(), null);
          });
      });

      it('should support query operators', () => {
        const testItems = [
          { op: q.in_, value: [new Date('2012-06-01 06:00:00Z')], expected: 1 },
          { op: q.gt, value: new Date('2012-06-01 05:00:00Z'), expected: 1 },
          { op: q.gt, value: new Date('2012-06-01 06:00:00Z'), expected: 0 },
          { op: q.gt, value: new Date('2012-06-01 07:00:00Z'), expected: 0 },
          { op: q.gte, value: new Date('2012-06-01 05:00:00Z'), expected: 1 },
          { op: q.gte, value: new Date('2012-06-01 06:00:00Z'), expected: 1 },
          { op: q.gte, value: new Date('2012-06-01 07:00:00Z'), expected: 0 },
          { op: q.lt, value: new Date('2012-06-01 05:00:00Z'), expected: 0 },
          { op: q.lt, value: new Date('2012-06-01 06:00:00Z'), expected: 0 },
          { op: q.lt, value: new Date('2012-06-01 07:00:00Z'), expected: 1 },
          { op: q.lte, value: new Date('2012-06-01 05:00:00Z'), expected: 0 },
          { op: q.lte, value: new Date('2012-06-01 06:00:00Z'), expected: 1 },
          { op: q.lte, value: new Date('2012-06-01 07:00:00Z'), expected: 1 }
        ];

        return Promise.all(testItems.map((item, index) => {
          const doc = { yyyymmdd: yyyymmddBuckets[0], addedDate: item.op(item.value) };
          return videoMapper.find(doc, 'default')
            .then(result => {
              assert.strictEqual(result.toArray().length, item.expected,
                `Failed for g.${item.op.name}(), item at index ${index}: expected ${item.expected}; ` +
                `obtained ${result.toArray().length}`);
            });
        }));
      });
    });

    describe('#insert()', () => {
      it('should insert on all tables where the partition and clustering keys are specified', () => {
        const doc = {
          id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'Video insert sample 1',
          description: 'description insert 1', yyyymmdd: new Date().toISOString().substr(0, 10), tags: ['a', 'b'],
          location: 'a/b/c', locationType: 1, preview: 'a/preview/c', thumbnails: { 'p1': 'd/e/f' }
        };

        return videoMapper.insert(doc, null, 'default')
          .then(() => getVideoRows(client, doc))
          .then(rows => {
            // It should have been inserted on the 3 tables
            assert.strictEqual(rows.length, 3);
            rows.forEach(row => assertRowMatchesDoc(row, doc));
          });
      });

      it('should insert on some of the tables when those keys are not specified', () => {
        const doc = {
          id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'Video insert 2 not in latest',
          description: 'This video will not be added to latest_videos table',
        };

        return videoMapper.insert(doc, null, 'default')
          .then(() => getVideoRows(client, doc))
          .then(rows => {
            // Inserted on "videos" and "user_videos" tables
            assert.strictEqual(rows.length, 2);
            rows.forEach(row => assertRowMatchesDoc(row, doc));
          });
      });

      it('should insert a single table when it only matches one table', () => {
        const doc = { id: Uuid.random(), name: 'Video insert 3' };

        return videoMapper.insert(doc, null, 'default')
          .then(() => getVideoRows(client, doc))
          .then(rows => {
            // Inserted only on "videos" table
            assert.strictEqual(rows.length, 1);
            rows.forEach(row => assertRowMatchesDoc(row, doc));
          });
      });

      it('should consider fields filter', () => {
        const doc = {
          id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'Video insert sample 4',
          description: 'description insert 4', yyyymmdd: new Date().toISOString().substr(0, 10), locationType: 1,
          preview: 'a/preview/c'
        };

        return videoMapper.insert(doc, { fields: ['id', 'userId', 'addedDate', 'name']}, 'default')
          .then(() => getVideoRows(client, doc))
          .then(rows => {
            // Inserted on "videos" and "user_videos" tables
            assert.strictEqual(rows.length, 3);
            // It retrieved a empty ResultSet
            assert.strictEqual(rows[2], null);

            // Use a doc with undefined values for fields not included
            const expectedDoc = { id: doc.id, userId: doc.userId, addedDate: doc.addedDate, name: doc.name };

            rows.slice(0, 2).forEach(row => assertRowMatchesDoc(row, expectedDoc));
          });
      });
    });
  });
});

/** @return {Mapper} */
function getMapper(setupInfo) {
  const client = new Client(utils.extend({ keyspace: setupInfo.keyspace }, helper.baseOptions));
  before(() => client.connect());
  after(() => client.shutdown());
  return new Mapper(client);
}

function getVideosMappingInfo(setupInfo) {
  const tables = [
    { name: 'videos', isView: false },
    { name: 'user_videos', isView: false },
    { name: 'latest_videos', isView: false }
  ];

  return new TableMappingInfo(setupInfo.keyspace, tables, null, colsToProps);
}

function getPropertyName(columnName) {
  const mappedName = colsToProps.get(columnName);
  return mappedName === undefined ? columnName : mappedName;
}

function assertRowMatchesDoc(row, doc) {
  assert.ok(row);
  Object.keys(row).forEach(columnName => {
    assert.deepEqual(row[columnName], doc[getPropertyName(columnName)]);
  });
}

/** @returns {Promise<Array<Row>>} */
function getVideoRows(client, doc) {
  const queries = [
    ['SELECT * FROM videos WHERE videoid = ?', [doc.id]]
  ];

  if (doc.userId && doc.addedDate) {
    queries.push(['SELECT * FROM user_videos WHERE userid = ? AND added_date = ? AND videoid = ?',
      [doc.userId, doc.addedDate, doc.id]]);
  }

  if (doc.yyyymmdd && doc.addedDate) {
    queries.push(['SELECT * FROM latest_videos WHERE yyyymmdd = ? AND added_date = ? AND videoid = ?',
      [doc.yyyymmdd, doc.addedDate, doc.id]]);
  }

  return Promise.all(queries.map(q => client.execute(q[0], q[1], {prepare: true})))
    .then(results => results.map(rs => rs.first()));
}