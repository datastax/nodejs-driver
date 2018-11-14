'use strict';

const assert = require('assert');
const types = require('../../../../lib/types');
const utils = require('../../../../lib/utils');
const Mapper = require('../../../../lib/mapping/mapper');
const Client = require('../../../../lib/client');
const Uuid = types.Uuid;
const mapperTestHelper = require('./mapper-test-helper');
const assertRowMatchesDoc = mapperTestHelper.assertRowMatchesDoc;
const helper = require('../../../test-helper');
const Result = require('../../../../lib/mapping/result');
const q = require('../../../../lib/mapping/q').q;

describe('Mapper', function () {

  mapperTestHelper.setupOnce(this);

  describe('#batch()', () => {

    const mapper = mapperTestHelper.getMapper();
    const client = mapper.client;
    const videoMapper = mapper.forModel('Video');
    const userMapper = mapper.forModel('User');
    const ratingsMapper = mapper.forModel('VideoRating');

    it('should execute a batch containing updates to multiple tables from the same doc', () => {
      const doc = { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'hello!' };

      return mapper.batch([ videoMapper.batching.update(doc) ])
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.ok(result.wasApplied());
          assert.strictEqual(result.length, 0);
        })
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

      // Insert the rows that are going to be used for DELETE
      return mapperTestHelper.insertVideoRows(client, docs[2])
        .then(() => mapper.batch([
          videoMapper.batching.update(docs[0]),
          videoMapper.batching.insert(docs[1]),
          videoMapper.batching.remove(docs[2], { fields: [ 'id', 'userId', 'addedDate' ]})
        ]))
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

    it('should execute a batch containing mutations from multiple docs', () => {
      const videoDoc = { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'new doc' };
      const userDoc = { id: Uuid.random(), firstName: 'hey', lastName: 'joe', email: 'hey@joe.com' };

      return mapper
        .batch([
          videoMapper.batching.update(videoDoc),
          userMapper.batching.update(userDoc)
        ])
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.ok(result.wasApplied());
          assert.strictEqual(result.length, 0);
        })
        .then(() => Promise.all([
          mapperTestHelper.getVideoRows(client, videoDoc, 'videoid, userid, added_date, name'),
          client.execute('SELECT * FROM users WHERE userid = ?', [ userDoc.id ], { prepare: true })
            .then(result => result.rows)
        ]))
        .then(results => {
          const videoRows = results[0];
          assert.strictEqual(videoRows.length, 2);
          videoRows.forEach(row => assertRowMatchesDoc(row, videoDoc));
          const userRows = results[1];
          assert.strictEqual(userRows.length, 1);
          assert.strictEqual(userRows[0]['firstname'], userDoc.firstName);
        });
    });

    it('should return a Result instance containing applied information of a LWT operation', () => {
      const userDoc = { id: Uuid.random(), firstName: 'Neil', lastName: 'Young', email: 'info@example.com' };

      return mapper
        .batch([
          userMapper.batching.update(userDoc, { ifExists: true })
        ])
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.strictEqual(result.wasApplied(), false);
          assert.strictEqual(result.length, 0);
        });
    });

    it('should adapt results of a LWT operation', () => {
      const doc = { id: Uuid.random(), firstName: 'hey', lastName: 'joe', email: 'hey@example.com' };

      const insertQuery = 'INSERT INTO users (userid, firstname, lastname, email) VALUES (?, ?, ?, ?)';

      return client.execute(insertQuery, [doc.id, doc.firstName, doc.lastName, doc.email], { prepare: true })
        .then(() => mapper.batch([
          userMapper.batching.update(doc, { when: { firstName: 'a', lastName: 'b' }})
        ]))
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.strictEqual(result.wasApplied(), false);
          assert.strictEqual(result.length, 1);
          const lwtDoc = result.first();
          assert.strictEqual(lwtDoc.firstName, doc.firstName);
          assert.strictEqual(lwtDoc.lastName, doc.lastName);
        });
    });

    it('should support updating counters in batch', () => {
      const doc1 = { id: Uuid.random(), ratingCounter: q.incr(1), ratingTotal: q.incr(5) };
      const doc2 = { id: Uuid.random(), ratingCounter: q.incr(2), ratingTotal: q.incr(8) };
      const selectQuery = 'SELECT * FROM video_rating WHERE videoid = ?';

      const items = [
        ratingsMapper.batching.update(doc1),
        ratingsMapper.batching.update(doc2)
      ];

      return mapper.batch(items)
        .then(() => Promise.all([doc1.id, doc2.id].map(id => client.execute(selectQuery, [ id ], { prepare: true }))))
        .then(results => {
          const expected = [ [1, 5], [2, 8] ];
          results.forEach((rs, index) => {
            const row = rs.first();
            assert.equal(row['rating_counter'], expected[index][0]);
            assert.equal(row['rating_total'], expected[index][1]);
          });
        });
    });
  });

  describe('#forModel()', () => {
    it('should be able to query on a Client instance not connected', () => {
      const userid = Uuid.random();
      const items = [
        ['insert', { userid }], ['remove', { userid }], ['update', { userid, email: 'info@example.com' }],
        ['find', { userid }], ['findAll'] ];

      return Promise.all(items.map(item => {
        const client = new Client(utils.extend({ keyspace: mapperTestHelper.keyspace }, helper.baseOptions));
        const mapper = new Mapper(client, { models: { 'User': { tables: ['users'] }}});
        const userMapper = mapper.forModel('User');

        const methodName = item[0];

        return userMapper[methodName](item[1])
          .then(result => {
            helper.assertInstanceOf(result, Result);
            assert.strictEqual(typeof result.length, 'number');
            assert.strictEqual(result.wasApplied(), true);
          })
          .then(() => client.shutdown());
      }));
    });
  });
});