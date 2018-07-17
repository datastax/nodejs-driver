'use strict';

const assert = require('assert');
const helper = require('../../../test-helper');
const Client = require('../../../../lib/client');
const utils = require('../../../../lib/utils');
const types = require('../../../../lib/types');
const Mapper = require('../../../../lib/mapper/mapper');
const TableMappingInfo = require('../../../../lib/mapper/table-mapping-info');

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
     {'cats','piano','lol'},'2012-06-01 08:00:00')`,
    `INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location) VALUES 
     (d0f60aa8-54a9-4840-b70c-fe562b68842b,99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 08:00:00','My funny cat',
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401')`,
    `INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location) VALUES ('2012-06-01',
     99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 08:00:00','My funny cat',
     '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');`
  ];

  const videoIds = [
    types.Uuid.fromString('99051fe9-6a9c-46c2-b949-38ef78858dd0')
  ];

  const userIds = [
    types.Uuid.fromString('d0f60aa8-54a9-4840-b70c-fe562b68842b')
  ];

  const setupInfo = helper.setup(1, { queries });
  //const setupInfo = { keyspace: 'ks0507795538724718' };

  context('with videos tables', () => {
    // videos -> video id
    // user_videos -> userid, added_date, videoid
    // latest_videos -> yyyymmdd, added_date, videoid
    const mapper = getMapper(setupInfo);
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
    });
  });
});

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

  const columns = new Map([['videoid', 'id'], ['userid', 'userId']]);
  return new TableMappingInfo(setupInfo.keyspace, tables, null, columns);
}