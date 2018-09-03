'use strict';

const assert = require('assert');
const helper = require('../../../test-helper');
const mapperTestHelper = require('./mapper-test-helper');
const types = require('../../../../lib/types');
const Uuid = types.Uuid;
const q = require('../../../../lib/mapper/q').q;
const assertRowMatchesDoc = mapperTestHelper.assertRowMatchesDoc;
const Result = require('../../../../lib/mapper/result');
const vit = helper.vit;

describe('ModelMapper', function () {

  mapperTestHelper.setupOnce(this);

  const mapper = mapperTestHelper.getMapper();
  const client = mapper.client;
  const videoMapper = mapper.forModel('Video');
  const userMapper = mapper.forModel('User');

  describe('#find()', () => {
    it('should use the correct table', () => {
      const doc = { id: mapperTestHelper.videoIds[0] };
      return videoMapper.find(doc, null, 'default').then(result => {
        assert.ok(result.first());
      });
    });

    it('should use the another table', () => {
      const doc = { userId: mapperTestHelper.userIds[0] };
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

    it('should throw an error when the table does not exist', () => testTableNotFound(mapper, 'find'));

    vit('3.0', 'should support query operators', () => {
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
        const doc = { yyyymmdd: mapperTestHelper.yyyymmddBuckets[0], addedDate: item.op(item.value) };
        return videoMapper.find(doc, 'default')
          .then(result => {
            assert.strictEqual(result.toArray().length, item.expected,
              `Failed for g.${item.op.name}(), item at index ${index}: expected ${item.expected}; ` +
              `obtained ${result.toArray().length}`);
          });
      }));
    });
  });

  describe('#get()', () => {
    it('should return the first document that matches the query');

    it('should support an array parameter to select');

    it('should throw an error when the table does not exist', () => testTableNotFound(mapper, 'get'));
  });

  describe('#insert()', () => {
    it('should insert on all tables where the partition and clustering keys are specified', () => {
      const doc = {
        id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'Video insert sample 1',
        description: 'description insert 1', yyyymmdd: new Date().toISOString().substr(0, 10), tags: ['a', 'b'],
        location: 'a/b/c', locationType: 1, preview: 'a/preview/c', thumbnails: { 'p1': 'd/e/f' }
      };

      return videoMapper.insert(doc, null, 'default')
        .then(() => mapperTestHelper.getVideoRows(client, doc))
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
        .then(() => mapperTestHelper.getVideoRows(client, doc))
        .then(rows => {
          // Inserted on "videos" and "user_videos" tables
          assert.strictEqual(rows.length, 2);
          rows.forEach(row => assertRowMatchesDoc(row, doc));
        });
    });

    it('should insert a single table when it only matches one table', () => {
      const doc = { id: Uuid.random(), name: 'Video insert 3' };

      return videoMapper.insert(doc, null, 'default')
        .then(() => mapperTestHelper.getVideoRows(client, doc))
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
        .then(() => mapperTestHelper.getVideoRows(client, doc))
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

    it('should support conditional statements on a single table', () => {
      // Description column is only present on a table
      const doc = { id: Uuid.random(), name: 'Conditional inserted', description: 'description inserted' };

      return videoMapper.insert(doc, { ifNotExists: true })
        .then(() => mapperTestHelper.getVideoRows(client, doc, 'videoid, name, description'))
        .then(rows => {
          assertRowMatchesDoc(rows[0], doc);
        });
    });

    it('should fail when conditional statements affect multiple tables', () => {
      const doc = {
        id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'Video insert conditional',
        description: 'description insert 5', yyyymmdd: new Date().toISOString().substr(0, 10), locationType: 1,
        preview: 'a/preview/c'
      };

      let error;

      return videoMapper.insert(doc, { ifNotExists: true }, 'default')
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, Error);
          assert.strictEqual(error.message, 'Batch with ifNotExists conditions cannot span multiple tables');
        });
    });

    it('should throw an error when the table does not exist', () => testTableNotFound(mapper, 'find'));
  });

  describe('#update()', () => {
    it('should update on all tables where the partition and clustering keys are specified');

    it('should update on some of the tables when those keys are not specified');

    it('should insert a single table when it only matches one table');

    it('should consider fields filter');

    it('should support conditional statements on a single table', () => {
      const doc = {
        id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'video cond 1',
        description: 'description 1', yyyymmdd: new Date().toISOString().substr(0, 10) };

      const docUpdated = { id: doc.id, description: 'description updated' };

      return mapperTestHelper.insertVideoRows(client, doc)
        .then(() => videoMapper.update(docUpdated, { when: { name: 'video cond 1'}}))
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.strictEqual(result.wasApplied(), true);
          assert.strictEqual(result.length, 0);
        })
        .then(() => mapperTestHelper.getVideoRows(client, docUpdated, 'videoid, description'))
        .then(rows => {
          assertRowMatchesDoc(rows[0], docUpdated);
        });
    });

    it('should fail when conditional statements affect multiple tables', () => {
      const doc = { id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'video cond 2' };
      const docUpdated = { id: doc.id, userId: doc.userId, addedDate: doc.addedDate, name: 'name updated' };

      let error;

      return mapperTestHelper.insertVideoRows(client, doc)
        .then(() => videoMapper.update(docUpdated, { when: { name: 'video cond 2'}}))
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, Error);
          assert.strictEqual(error.message, 'Batch with when or ifExists conditions cannot span multiple tables');
        });
    });

    it('should adapt results of a LWT operation', () => {
      function assertNotApplied(result) {
        helper.assertInstanceOf(result, Result);
        assert.strictEqual(result.wasApplied(), false);
        assert.strictEqual(result.length, 1);
      }

      const doc = { id: Uuid.random(), firstName: 'hey', lastName: 'joe', email: 'hey@example.com' };

      const insertQuery = 'INSERT INTO users (userid, firstname, lastname, email) VALUES (?, ?, ?, ?)';

      return client.execute(insertQuery, [ doc.id, doc.firstName, doc.lastName, doc.email ], { prepare: true })
        .then(() => userMapper.update(doc, { when: { firstName: 'a' }}))
        .then(result => {
          assertNotApplied(result);
          const lwtDoc = result.first();
          assert.strictEqual(lwtDoc.firstName, doc.firstName);
        })
        .then(() => userMapper.update(doc, { when: { firstName: 'a', lastName: 'b' }}))
        .then(result => {
          assertNotApplied(result);
          const lwtDoc = result.first();
          assert.strictEqual(lwtDoc.firstName, doc.firstName);
          assert.strictEqual(lwtDoc.lastName, doc.lastName);
        })
        .then(() => userMapper.update(doc, { when: { firstName: 'a', lastName: q.notEq(doc.lastName) }}))
        .then(result => {
          assertNotApplied(result);
          const lwtDoc = result.first();
          assert.strictEqual(lwtDoc.firstName, doc.firstName);
          assert.strictEqual(lwtDoc.lastName, doc.lastName);
        });
    });

    it('should add new items to a set', () => {
      const doc = { id: Uuid.random(), name: 'hello', tags: ['a', 'b', 'c']};
      return client
        .execute('INSERT INTO videos (videoid, name, tags) VALUES (?, ?, ?)', [ doc.id, doc.name, doc.tags ],
          { prepare: true })
        .then(() => videoMapper.update({ id: doc.id, tags: q.append(['d', 'e']) }))
        .then(() => mapperTestHelper.getVideoRows(client, { id: doc.id }))
        .then(rows => {
          assert.strictEqual(rows.length, 1);
          assert.deepStrictEqual(rows[0]['tags'], doc.tags.concat('d', 'e'));
        });
    });

    it('should throw an error when the table does not exist', () => testTableNotFound(mapper, 'find'));
  });

  describe('#remove()', () => {
    it('should delete on all tables where the partition and clustering keys are specified');

    it('should delete on some of the tables when those keys are not specified');

    it('should delete a single table when it only matches one table');

    it('should consider fields filter');

    it('should remove only columns when specified');

    it('should support conditional statements on a single table', () => {
      const doc = { id: Uuid.random(), userId: Uuid.random(), name: 'video to delete' };

      return mapperTestHelper.insertVideoRows(client, doc)
        .then(() => mapperTestHelper.getVideoRows(client, doc))
        // It was inserted on 1 table
        .then(rows => {
          assert.strictEqual(rows.length, 1);
          assert.notEqual(rows[0], null);
        })
        .then(() => videoMapper.remove(doc, { when: { name: 'video to delete' }, fields: ['id']}))
        .then(() => mapperTestHelper.getVideoRows(client, doc))
        .then(rows => {
          assert.strictEqual(rows.length, 1);
          assert.strictEqual(rows[0], null);
        });
    });

    it('should fail when conditional statements affect multiple tables', () => {
      const doc = {
        id: Uuid.random(), userId: Uuid.random(), addedDate: new Date(), name: 'video to delete',
        description: 'desc', yyyymmdd: new Date().toISOString().substr(0, 10) };

      let error;

      return mapperTestHelper.insertVideoRows(client, doc)
        .then(() => videoMapper.remove(doc, { when: { name: 'video to delete'}}))
        .catch(err => error = err)
        .then(() => {
          helper.assertInstanceOf(error, Error);
          assert.strictEqual(error.message, 'Batch with when or ifExists conditions cannot span multiple tables');
        });
    });

    it('should throw an error when the table does not exist', () => testTableNotFound(mapper, 'find'));
  });
});

function testTableNotFound(mapper, methodName) {
  const modelMapper = mapper.forModel('TableDoesNotExist');
  let catchCalled = false;

  return modelMapper[methodName]({id: 1})
    .catch(err => {
      catchCalled = true;
      helper.assertInstanceOf(err, Error);
      assert.strictEqual(err.message, 'Table "TableDoesNotExist" could not be retrieved');
    })
    .then(() => assert.strictEqual(catchCalled, true));
}
