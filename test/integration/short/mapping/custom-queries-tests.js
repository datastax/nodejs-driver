'use strict';

const assert = require('assert');
const types = require('../../../../lib/types');
const Result = require('../../../../lib/mapping/result');
const Uuid = types.Uuid;
const mapperTestHelper = require('./mapper-test-helper');
const assertRowMatchesDoc = mapperTestHelper.assertRowMatchesDoc;
const helper = require('../../../test-helper');

describe('ModelMapper', function () {
  mapperTestHelper.setupOnce(this);

  describe('#mapWithQuery()', () => {
    const mapper = mapperTestHelper.getMapper();
    const client = mapper.client;
    const videoMapper = mapper.forModel('Video');

    it('should map the result for a given SELECT query', () => {
      const id = mapperTestHelper.videoIds[0];
      const executor = videoMapper.mapWithQuery('SELECT * FROM videos WHERE videoid = ?', d => [ d.id ]);
      return executor({ id })
        .then(results => mapperTestHelper.getVideoRows(client, { id }).then(rows => ({ rows, results})))
        .then(obj => {
          assert.strictEqual(obj.rows.length, 1);
          assertRowMatchesDoc(obj.rows[0], obj.results.first());
        });
    });

    it('should map the result for a given UPDATE IF query', () => {
      const doc = {
        id: Uuid.random(),
        name: 'Test Conditional Update',
        description: 'My description'
      };

      const executor = videoMapper.mapWithQuery(
        'UPDATE videos SET name = ?, description = ? WHERE videoid  = ? IF EXISTS',
        d => [ d.name, d.description, d.id ]);

      return executor(doc, { prepare: true })
        .then(result => {
          helper.assertInstanceOf(result, Result);
          assert.strictEqual(result.wasApplied(), false);
          // The empty doc should be hidden
          assert.strictEqual(result.length, 0);
          assert.deepStrictEqual(result.toArray(), []);
          return mapperTestHelper.getVideoRows(client, doc, 'videoid, name, description');
        })
        .then(rows => {
          assert.strictEqual(rows.length, 1);
          assertRowMatchesDoc(rows, doc);
        });
    });
  });
});