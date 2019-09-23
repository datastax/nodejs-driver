/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

import { Client, graph } from "../../../index";
import GraphResultSet = graph.GraphResultSet;


/*
 * TypeScript definitions compilation tests for metadata module.
 */

async function myTest(client: Client): Promise<any> {
  let result: GraphResultSet;
  let cb = (err: Error, r: GraphResultSet) => {};

  // Promise-based API
  result = await client.executeGraph('g.V()');
  result = await client.executeGraph('g.V(id)', { id: 1});
  result = await client.executeGraph('g.V()', undefined, { executionProfile: 'ep1' });

  // Callback-based API
  await client.executeGraph('g.V()', cb);
  await client.executeGraph('g.V(id)', { id: 1}, cb);
  await client.executeGraph('g.V()', {}, { executionProfile: 'ep1' }, cb);
}