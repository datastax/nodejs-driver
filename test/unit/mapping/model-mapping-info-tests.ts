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

const ModelMappingInfo = require('../../../lib/mapping/model-mapping-info');

describe('ModelMappingInfo', function () {
  describe('parse()', function () {
    it('should throw when the model keyspace is not set', () => {
      const options = {
        models: {
          'Video': { }
        }
      };

      assert.throws(() => ModelMappingInfo.parse(options, null),
        /You should specify the keyspace of the model in the MappingOptions/);

      assert.doesNotThrow(() => ModelMappingInfo.parse(options, 'some_ks'));
    });

    it('should parse the column options', () => {
      const options = {
        models: {
          'Video': {
            tables: ['videos'],
            keyspace: 'ks1',
            columns: {
              'videoid': 'videoId',
              'userid': { name: 'userId' },
              'video_media': {
                name: 'media',
                fromModel: a => a,
                toModel: b => b
              }
            }
          }
        }
      };

      const info = ModelMappingInfo.parse(options);
      assert.ok(info);
      const modelInfo = info.get('Video');
      assert.isObject(modelInfo);
      assert.equal(modelInfo.keyspace, options.models['Video'].keyspace);
      assert.lengthOf(modelInfo.tables, 1);
      assert.equal(modelInfo.getColumnName('media'), 'video_media');
      assert.equal(modelInfo.getPropertyName('video_media'), 'media');
      assert.equal(modelInfo.getColumnName('videoId'), 'videoid');
      assert.equal(modelInfo.getPropertyName('userid'), 'userId');
    });

    it('should throw when toModel or fromModel are not functions', () => {
      assert.throws(() => ModelMappingInfo.parse({
        models: {
          'Video': {
            tables: ['videos'],
            keyspace: 'ks1',
            columns: {
              'video_media': {
                name: 'media',
                fromModel: 'a',
              }
            }
          }
        }
      }), /fromModel type for property 'media' should be a function/);

      assert.throws(() => ModelMappingInfo.parse({
        models: {
          'Video': {
            tables: ['videos'],
            keyspace: 'ks1',
            columns: {
              'video_media': {
                name: 'media',
                toModel: {},
              }
            }
          }
        }
      }), /toModel type for property 'media' should be a function/);
    });
  });
});