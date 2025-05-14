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
import events from "events";
import helper from "../test-helper";
import PrepareHandler from "../../lib/prepare-handler";
import types from "../../lib/types/index";
import utils from "../../lib/utils";
import { defaultOptions } from "../../lib/client-options";

describe('PrepareHandler', function () {

  describe('getPrepared()', function () {

    it('should make request when not already prepared', async () => {
      const client = getClient({ prepareOnAllHosts: false });
      const lbp = helper.getLoadBalancingPolicyFake([ { isUp: false }, { ignored: true }, {}, {} ]);
      await PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null);
      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[2].prepareCalled, 1);
      assert.strictEqual(hosts[3].prepareCalled, 0);
    });

    it('should make the same prepare request once and queue the rest', async () => {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ { } ]);
      await Promise.all(
        Array(100).fill(0).map(() => PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null)));

      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].prepareCalled, 1);
    });

    it('should callback in error if request send fails', async () => {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ {} ], function (q, h, cb) {
        cb(new Error('Test prepare error'));
      });

      let err;

      try {
        await PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null);
      } catch (e) {
        err = e;
      }

      assert.instanceOf(err, Error);
      const host = lbp.getFixedQueryPlan()[0];
      assert.strictEqual(host.prepareCalled, 1);
    });

    it('should retry on next host if request send fails due to socket error', async () => {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function (q, h, cb) {
        if (h.address === '0') {
          const err = new Error('Test prepare error');
          err.isSocketError = true;
          return cb(err);
        }
        cb(null, { id: 100, meta: {} });
      });

      await PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null);

      const hosts = lbp.getFixedQueryPlan();
      assert.strictEqual(hosts[0].prepareCalled, 1);
      assert.strictEqual(hosts[1].prepareCalled, 1);
    });

    it('should prepare on all UP hosts not ignored', async () => {
      const client = getClient({ prepareOnAllHosts: true });
      const lbp = helper.getLoadBalancingPolicyFake([ { isUp: false }, {}, { isUp: false }, { ignored: true }, {} ]);

      await PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null);

      const hosts = lbp.getFixedQueryPlan();

      const consideredHosts = [ hosts[1], hosts[4] ];
      const avoidedHosts = [ hosts[0], hosts[2], hosts[3] ];

      consideredHosts.forEach(h => {
        assert.strictEqual(h.prepareCalled, 1);
        assert.strictEqual(h.borrowConnection.callCount, 1);
      });

      avoidedHosts.forEach(h => {
        assert.strictEqual(h.prepareCalled, 0);
        assert.strictEqual(h.borrowConnection.callCount, 0);
      });
    });
  });

  describe('prepareAllQueries', function () {
    it('should switch keyspace per each keyspace and execute', async () => {
      const host = helper.getHostsMock([ {} ])[0];
      const preparedInfoArray = [
        { keyspace: 'system', query: 'query1' },
        { keyspace: 'system_schema', query: 'query2' },
        { keyspace: null, query: 'query3' },
        { keyspace: 'userks', query: 'query4' },
        { keyspace: 'system', query: 'query5' },
      ];

      await PrepareHandler.prepareAllQueries(host, preparedInfoArray);
      assert.deepStrictEqual(host.connectionKeyspace, [ 'system', 'system_schema', 'userks' ]);
      assert.strictEqual(host.prepareCalled, 5);
    });

    it('should callback when there are no queries to prepare', async () => {
      await PrepareHandler.prepareAllQueries({}, []);
    });

    it('should callback in error when there is an error borrowing a connection', async () => {
      const host = helper.getHostsMock([ {} ])[0];
      host.borrowConnection = () => Promise.reject(new Error('Test error'));

      let err;
      try {
        await PrepareHandler.prepareAllQueries(host, [{ query: 'query1' }]);
      } catch (e) {
        err = e;
      }
      helper.assertInstanceOf(err, Error);
    });

    it('should callback in error when there is an error preparing any of the queries', async () => {
      function prepareOnce(q, h, cb) {
        if (q === 'query3') {
          return cb(new Error('Test error'));
        }
        cb();
      }
      const host = helper.getHostsMock([ {} ], prepareOnce)[0];
      const preparedInfoArray = [
        { keyspace: 'system', query: 'query1' },
        { keyspace: null, query: 'query2' },
        { keyspace: 'system', query: 'query3' }
      ];

      let err;
      try {
        await PrepareHandler.prepareAllQueries(host, preparedInfoArray);
      } catch (e) {
        err = e;
      }

      helper.assertInstanceOf(err, Error);
      assert.deepStrictEqual(host.connectionKeyspace, ['system']);
      assert.strictEqual(host.prepareCalled, 2);
    });
  });
});

function getClient(options) {
  return {
    metadata: {
      _infos: {},
      getPreparedInfo: function (ks, q) {
        let info = this._infos[ks + '.' + q];
        if (!info) {
          info = this._infos[ks + '.' + q] = new events.EventEmitter().setMaxListeners(1000);
        }
        return info;
      },
      setPreparedById: utils.noop
    },
    options: utils.extend({ logEmitter: () => {}}, defaultOptions(), options),
    profileManager: {
      getDistance: function (h) {
        return h.shouldBeIgnored ? types.distance.ignored : types.distance.local;
      }
    }
  };
}