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
import { assert } from "chai";
import Client from "../../../lib/client";
import errors from "../../../lib/errors";
import promiseUtils from "../../../lib/promise-utils";
import helper from "../../test-helper";
import simulacron from "../simulacron";

'use strict';
const { OrderedLoadBalancingPolicy } = helper;

const queryDelayedOnNode0 = 'INSERT INTO paused_on_first_node';
const queryDelayedOnAllNodes = 'INSERT INTO paused_on_all_nodes';
const queryDelayedOnPrepareOnNode0 = 'INSERT INTO paused_prepare_on_first_node';
const queryDelayedOnPrepareOnAllNodes = 'INSERT INTO paused_prepare_on_all_nodes';

let simulacronCluster = null;
const readTimeout = 100;

describe('client read timeouts', function () {
  this.timeout(10000);

  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  beforeEach(done => {
    simulacronCluster = new simulacron.SimulacronCluster();
    simulacronCluster.register([3], {}, done);
  });

  beforeEach(done => simulacronCluster.clear(done));

  beforeEach(done => simulacronCluster.node(0).prime({
    when: { request: 'batch', queries: [{ query: queryDelayedOnNode0 }] },
    then: { result: 'success', delay_in_ms: 1000 }
  }, done));

  beforeEach(done => simulacronCluster.node(0).prime({
    when: { query: queryDelayedOnNode0 },
    then: { result: 'success', delay_in_ms: 1000 }
  }, done));

  beforeEach(done => simulacronCluster.prime({
    when: { request: 'batch', queries: [{ query: queryDelayedOnAllNodes }] },
    then: { result: 'success', delay_in_ms: 1000 }
  }, done));

  beforeEach(done => simulacronCluster.prime({
    when: { query: queryDelayedOnAllNodes },
    then: { result: 'success', delay_in_ms: 1000 }
  }, done));

  beforeEach(done => simulacronCluster.prime({
    when: { query: queryDelayedOnPrepareOnAllNodes },
    then: { result: 'success', delay_in_ms: 1000, ignore_on_prepare: false }
  }, done));

  beforeEach(done => simulacronCluster.node(0).prime({
    when: { query: queryDelayedOnPrepareOnNode0 },
    then: { result: 'success', delay_in_ms: 1000, ignore_on_prepare: false }
  }, done));

  afterEach(done => simulacronCluster.unregister(done));

  it('should do nothing else than waiting when socketOptions.readTimeout is not set',
    testNodeUsedAsCoordinator(0));

  it('should move to next host by default for simple queries',
    testNodeUsedAsCoordinator(1, { readTimeout }));

  it('should move to next host for prepared queries executions',
    testNodeUsedAsCoordinator(1, { readTimeout, prepare: true }));

  it('should move to next host for prepared requests',
    testNodeUsedAsCoordinator(1, { readTimeout, prepare: true }), queryDelayedOnPrepareOnNode0);

  it('should throw error when isIdempotent is false', async () => {
    const client = newInstance();
    await client.connect();
    await helper.assertThrowsAsync(
      client.execute(queryDelayedOnNode0, [], { isIdempotent: false, readTimeout }),
      errors.OperationTimedOutError);
    await client.shutdown();
  });

  it('defunct the connection when the threshold passed', async () => {
    const defunctReadTimeoutThreshold = 10;
    const readTimeout = 400;
    const client = newInstance({ socketOptions: { defunctReadTimeoutThreshold, readTimeout }});
    await client.connect();
    let hostDown = null;

    // The driver should mark the host as down when the pool closes all connections
    client.on('hostDown', h => hostDown = h);
    const coordinators = new Set();

    await promiseUtils.times(100, 32, async () => {
      const rs = await client.execute(queryDelayedOnNode0);
      coordinators.add(rs.info.queriedHost);
    });

    // First node should not have responded
    assert.doesNotHaveAllKeys(coordinators, [ simulacronCluster.node(0).address ]);

    // Node should be marked as down
    assert.ok(hostDown);
    assert.strictEqual(hostDown.address, simulacronCluster.node(0).address);

    await client.shutdown();
  });

  describe('with prepared batches', function () {
    const getOptions = (readTimeout) => ({ socketOptions: { readTimeout } });

    it('should retry when preparing multiple queries', async () => {
      const client = newInstance(getOptions(500));

      await client.connect();
      const rs = await client.batch([ { query: queryDelayedOnNode0 } ], { prepare: true });
      // It timed out and then it was retried on the second host
      assert.strictEqual(rs.info.queriedHost, simulacronCluster.node(1).address);
      await client.shutdown();
    });

    it('should produce a NoHostAvailableError when execution timed out on all hosts', async () => {
      const client = newInstance(getOptions(readTimeout));

      await client.connect();
      const err = await helper.assertThrowsAsync(
        client.batch([ { query: queryDelayedOnAllNodes } ], { prepare: true }),
        errors.NoHostAvailableError);

      assert.lengthOf(Object.values(err.innerErrors), 3);
      Object.values(err.innerErrors).forEach(err => {
        assert.instanceOf(err, errors.OperationTimedOutError);
      });

      await client.shutdown();
    });

    it('should produce a NoHostAvailableError when prepare tried and timed out on all hosts', async () => {
      const client = newInstance(getOptions(readTimeout));

      await client.connect();
      // Throws on prepare
      await helper.assertThrowsAsync(
        client.execute(queryDelayedOnPrepareOnAllNodes, [ 'a' ], { prepare: true }),
        errors.NoHostAvailableError);


      await helper.assertThrowsAsync(
        client.batch([ { query: queryDelayedOnPrepareOnAllNodes, params: [ 'a' ]} ], { prepare: true }),
        errors.NoHostAvailableError);

      await client.shutdown();
    });
  });
});

function newInstance(options) {
  const client = new Client(Object.assign({
    localDataCenter: 'dc1',
    contactPoints: simulacronCluster.getContactPoints(),
    // Use a LBP that yields the hosts in deterministic order for test purposes
    policies: { loadBalancing: new OrderedLoadBalancingPolicy(simulacronCluster) },
    queryOptions: { isIdempotent: true }
  }, options));

  helper.shutdownAfterThisTest(client);

  return client;
}

function testNodeUsedAsCoordinator(nodeIndex, queryOptions, query) {
  const prepare = false;
  query = query || queryDelayedOnNode0;
  return async () => {
    const client = newInstance();
    await client.connect();

    const rs = await client.execute(query, [], Object.assign({ prepare }, queryOptions));
    assert.strictEqual(rs.info.queriedHost, simulacronCluster.node(nodeIndex).address);

    await client.shutdown();
  };
}