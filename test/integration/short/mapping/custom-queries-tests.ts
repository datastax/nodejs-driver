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
import assert from "assert";
import types from "../../../../lib/types/index";
import Result from "../../../../lib/mapping/result";
import mapperTestHelper from "./mapper-test-helper";
import helper from "../../../test-helper";

'use strict';
const Uuid = types.Uuid;
const assertRowMatchesDoc = mapperTestHelper.assertRowMatchesDoc;
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