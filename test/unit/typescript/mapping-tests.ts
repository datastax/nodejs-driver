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

import { Client, mapping, types } from "../../../index";
import Mapper = mapping.Mapper;
import ModelMapper = mapping.ModelMapper;
import Uuid = types.Uuid;
import Result = mapping.Result;

/*
 * TypeScript definitions compilation tests for types module.
 */

async function myTest(client: Client): Promise<any> {
  let o: object;
  let b: boolean;
  let result: Result;

  const mapper: Mapper = new Mapper(client, {
    models: {
      'Video': {
        tables: ['videos'],
        columns: {
          'videoid': 'videoId',
          'userid': { name: 'userId' },
          'video_media': {
            name: 'media',
            toModel: (value) => JSON.parse(value),
            fromModel: (value) => JSON.stringify(value)
          }
        }
      }
    }
  });

  const videoMapper: ModelMapper = mapper.forModel('Video');
  o = await videoMapper.get({ videoId: Uuid.random() });

  o = await videoMapper.get({ name: 'a' }, { fields: ['videoId'] }, 'ep1');
  o = await videoMapper.get({ name: 'b' }, undefined, { executionProfile: 'ep1' });
  o = await videoMapper.get({ userId: 1 }, { }, { isIdempotent: true });

  result = await videoMapper.find({ name: 'a' }, { fields: ['videoId'], limit: 10, orderBy: { 'name': 'asc' } });
  result = await videoMapper.find({ name: 'b' }, { }, 'ep1');

  let arr:any[] = result.toArray();
  o = result.first();

  result = await videoMapper.insert({ videoId: Uuid.random(), name: 'a' });
  result = await videoMapper.insert({ name: 'a' }, { ifNotExists: true }, 'ep1');

  result = await videoMapper.update({ videoId: Uuid.random(), userId: 1, name: 'a' });
  result = await videoMapper.update({ name: 'a' }, { when: { date: new Date() } }, 'ep1');
  b = result.wasApplied();
  result = await videoMapper.update({ name: 'a' }, { ttl: 123, ifExists: true }, { isIdempotent: true, executionProfile: 'ep2' });

  result = await videoMapper.remove({ videoId: Uuid.random() });
  result = await videoMapper.remove({ videoId: Uuid.random() }, { ifExists: true });
}