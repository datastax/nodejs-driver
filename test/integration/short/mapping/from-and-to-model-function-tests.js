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

const { assert } = require('chai');

const { Uuid } = require('../../../../lib/types');
const mapperTestHelper = require('./mapper-test-helper');
const helper = require('../../../test-helper');
const { UnderscoreCqlToCamelCaseMappings } = require('../../../../lib/mapping/table-mappings');

describe('Mapper', function () {
  mapperTestHelper.setupOnce(this);

  context('using fromModel and toModel mapping functions function', () => {
    const mapper = mapperTestHelper.getMapper({ models: {
      'Video': {
        tables: [ 'videos', 'user_videos' ],
        mappings: new UnderscoreCqlToCamelCaseMappings(),
        columns: {
          'videoid': 'videoId',
          'userid': { name: 'userId', fromModel: Uuid.fromString, toModel: v => v.toString() },
          'description': { fromModel: JSON.stringify, toModel: JSON.parse }
        }
      }
    }});

    const videoMapper = mapper.forModel('Video');

    it('should use the custom mapping functions for the values', async () => {
      let result;
      const videoId = Uuid.random();
      const userId = Uuid.random().toString();
      const name = 'sample';
      const addedDate = new Date();
      const description1 = { prop1: 1, prop2: 'two' };
      await videoMapper.insert({ videoId, userId, name, description: description1, addedDate });
      result = (await videoMapper.find({ videoId })).first();
      // userId should be a string, even though it's stored as a "uuid"
      assert.strictEqual(result.userId, userId);
      assert.strictEqual(result.name, name);
      assert.deepEqual(result.description, description1);

      // Find by user_id on user_videos table
      result = (await videoMapper.find({ userId })).first();
      assert.strictEqual(result.name, name);

      // Update the description
      const description2 = { prop1: 1, prop2: 'two', anotherProp: true };
      await videoMapper.update({ videoId, userId, name, description: description2, addedDate });
      result = (await videoMapper.find({ videoId })).first();
      assert.deepEqual(result.description, description2);

      // Remove
      await videoMapper.remove({ videoId, userId, addedDate });

      // Verify that it was removed
      assert.lengthOf((await videoMapper.find({ videoId })).toArray(), 0);
      assert.lengthOf((await videoMapper.find({ userId })).toArray(), 0);
    });

    it('should throw an error when mapping function throws an error', async () => {
      const mapper = mapperTestHelper.getMapper({ models: {
        'Video': {
          tables: [ 'videos', 'user_videos' ],
          mappings: new UnderscoreCqlToCamelCaseMappings(),
          columns: {
            'videoid': 'videoId',
            'added_date': 'addedDate',
            'userid': {
              name: 'userId',
              fromModel: () => {
                throw new Error('fromModel error');
              },
              toModel: () => {
                throw new Error('toModel error');
              }
            },
          }
        }
      }});

      const videoMapper = mapper.forModel('Video');
      const videoId = Uuid.random();
      const userId = Uuid.random();
      await helper.assertThrowsAsync(
        videoMapper.insert({ videoId, userId, name: 'Sample name', description: 'description', addedDate: new Date() }),
        Error,
        'fromModel error'
      );
      await helper.assertThrowsAsync(
        videoMapper.update({ videoId, userId, name: 'Sample name', description: 'description', addedDate: new Date() }),
        Error,
        'fromModel error'
      );
      await helper.assertThrowsAsync(
        videoMapper.remove({ videoId, userId, addedDate: new Date() }),
        Error,
        'fromModel error'
      );

      // Use an existing row
      const rs = await videoMapper.find({ videoId: mapperTestHelper.videoIds[0] });
      // Throws when adapting row values
      assert.throws(() => rs.toArray(), /toModel error/);
    });
  });
});