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
const Client = require('../../../lib/client');
const errors = require('../../../lib/errors');
const helper = require('../../test-helper');
const simulacron = require('../simulacron');
const { OrderedLoadBalancingPolicy } = helper;

describe('client read timeouts', function () {

  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));


  describe('with prepared batches', function () {

    let simulacronCluster = null;

    const queryDelayedOnPrepare = 'INSERT INTO paused_prepared';
    const queryDelayedOnNode0 = 'INSERT INTO a.b (c) VALUES (?)';
    const queryDelayedOnAllNodes = 'INSERT INTO d.e (f) VALUES (?)';

    beforeEach(done => {
      simulacronCluster = new simulacron.SimulacronCluster();
      simulacronCluster.register([3], {}, done);
    });

    beforeEach(done => simulacronCluster.clear(done));

    beforeEach(done => simulacronCluster.node(0).prime({
      when: { request: 'batch', queries: [{ query: queryDelayedOnNode0 }] },
      then: { result: 'success', delay_in_ms: 2000 }
    }, done));

    beforeEach(done => simulacronCluster.prime({
      when: { request: 'batch', queries: [{ query: queryDelayedOnAllNodes }] },
      then: { result: 'success', delay_in_ms: 2000 }
    }, done));

    beforeEach(done => simulacronCluster.prime({
      when: { query: queryDelayedOnPrepare },
      then: { result: 'success', delay_in_ms: 2000, ignore_on_prepare: false }
    }, done));

    afterEach(done => simulacronCluster.unregister(done));

    it('should retry when preparing multiple queries', async () => {
      const client = newInstance({
        contactPoints: simulacronCluster.getContactPoints(),
        policies: { loadBalancing: new OrderedLoadBalancingPolicy(simulacronCluster) },
        socketOptions: { readTimeout: 500 },
        queryOptions: { isIdempotent: true }
      });

      await client.connect();
      const rs = await client.batch([ { query: queryDelayedOnNode0, params: ['a'] } ], { prepare: true });
      // It timed out and then it was retried on the second host
      assert.strictEqual(rs.info.queriedHost, simulacronCluster.node(1).address);
      await client.shutdown();
    });

    it('should produce a NoHostAvailableError when execution timed out on all hosts', async () => {
      const client = newInstance({
        contactPoints: simulacronCluster.getContactPoints(),
        policies: { loadBalancing: new OrderedLoadBalancingPolicy(simulacronCluster) },
        socketOptions: { readTimeout: 50 },
        queryOptions: { isIdempotent: true }
      });

      await client.connect();
      const err = await helper.assertThrowsAsync(
        client.batch([ { query: queryDelayedOnAllNodes, params: ['a'] } ], { prepare: true }),
        errors.NoHostAvailableError);

      assert.lengthOf(Object.values(err.innerErrors), 3);
      Object.values(err.innerErrors).forEach(err => {
        assert.instanceOf(err, errors.OperationTimedOutError);
      });

      await client.shutdown();
    });

    it('should produce a NoHostAvailableError when prepare tried and timed out on all hosts', async () => {
      const client = newInstance({
        contactPoints: simulacronCluster.getContactPoints(),
        policies: { loadBalancing: new OrderedLoadBalancingPolicy(simulacronCluster) },
        socketOptions: { readTimeout: 50 },
        queryOptions: { isIdempotent: true }
      });

      await client.connect();
      // Throws on prepare
      await helper.assertThrowsAsync(
        client.execute(queryDelayedOnPrepare, [ 'a' ], { prepare: true }),
        errors.NoHostAvailableError);


      await helper.assertThrowsAsync(
        client.batch([ { query: queryDelayedOnPrepare, params: [ 'a' ]} ], { prepare: true }),
        errors.NoHostAvailableError);

      await client.shutdown();
    });
  });
});

function newInstance(options) {
  return helper.shutdownAfterThisTest(new Client(Object.assign({ localDataCenter: 'dc1' }, options)));
}