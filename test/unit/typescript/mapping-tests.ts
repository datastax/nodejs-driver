/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    models: { 'Video': { tables: ['videos'] } }
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