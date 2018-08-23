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
  });
});