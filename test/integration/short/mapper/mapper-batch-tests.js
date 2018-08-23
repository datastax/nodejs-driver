'use strict';

const assert = require('assert');
const types = require('../../../../lib/types');
const Uuid = types.Uuid;
const mapperTestHelper = require('./mapper-test-helper');
const assertRowMatchesDoc = mapperTestHelper.assertRowMatchesDoc;


describe('Mapper', function () {

  mapperTestHelper.setupOnce(this);

  describe('#batch()', () => {

    const mapper = mapperTestHelper.getMapper();
    const client = mapper.client;
    const videoMapper = mapper.forModel('Video', mapperTestHelper.getVideosMappingInfo());

    it('should execute a batch containing updates to multiple tables from the same doc', () => {
      const doc = { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'hello!' };

      return mapper.batch([ videoMapper.batching.update(doc) ])
        .then(() => mapperTestHelper.getVideoRows(client, doc, 'videoid, userid, added_date, name'))
        .then(rows => {
          assert.strictEqual(rows.length, 2);
          rows.forEach(row => assertRowMatchesDoc(row, doc));
        });
    });

    it('should execute a batch containing updates/inserts/deletes to multiple tables from the same doc', () => {
      const docs = [
        { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'doc 1' },
        { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'doc 2' },
        { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'doc to be deleted' }
      ];

      const columns = 'videoid, userid, added_date, name';

      const items = [
        videoMapper.batching.update(docs[0]),
        videoMapper.batching.insert(docs[1]),
        videoMapper.batching.remove(docs[2], { fields: [ 'id', 'userId', 'addedDate' ]})
      ];

      // Insert the rows that are going to be used for DELETE
      return mapperTestHelper.insertVideoRows(client, docs[2])
        .then(() => mapper.batch(items))
        .then(() => Promise.all([
          mapperTestHelper.getVideoRows(client, docs[0], columns),
          mapperTestHelper.getVideoRows(client, docs[1], columns),
          mapperTestHelper.getVideoRows(client, docs[2], columns),
        ]))
        .then(results => {
          assert.strictEqual(results.length, 3);
          results.forEach((rows, index) => {
            assert.strictEqual(rows.length, 2);
            if (index !== 2) {
              rows.forEach(row => assertRowMatchesDoc(row, docs[index]));
            }
            else {
              // We should not be able to retrieve the rows that were deleted
              rows.forEach(row => assert.strictEqual(row, null));
            }
          });
        });
    });
  });
});