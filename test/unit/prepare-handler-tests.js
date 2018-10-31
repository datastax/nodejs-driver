'use strict';

const assert = require('assert');
const events = require('events');
const helper = require('../test-helper');
const PrepareHandler = require('../../lib/prepare-handler');
const defaultOptions = require('../../lib/client-options').defaultOptions;
const types = require('../../lib/types');
const utils = require('../../lib/utils');

describe('PrepareHandler', function () {
  describe('getPrepared()', function () {
    it('should make request when not already prepared', function (done) {
      const client = getClient({ prepareOnAllHosts: false });
      const lbp = helper.getLoadBalancingPolicyFake([ { isUp: false }, { ignored: true }, {}, {} ]);
      PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null, function (err) {
        assert.ifError(err);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[2].prepareCalled, 1);
        assert.strictEqual(hosts[3].prepareCalled, 0);
        done();
      });
    });
    it('should make the same prepare request once and queue the rest', function (done) {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ { } ]);
      utils.times(100, function (n, next) {
        PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null, next);
      }, function (err) {
        assert.ifError(err);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].prepareCalled, 1);
        done();
      });
    });
    it('should callback in error if request send fails', function (done) {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ {} ], function (q, h, cb) {
        cb(new Error('Test prepare error'));
      });
      PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null, function (err) {
        assert.ok(err);
        const host = lbp.getFixedQueryPlan()[0];
        assert.strictEqual(host.prepareCalled, 1);
        done();
      });
    });
    it('should retry on next host if request send fails due to socket error', function (done) {
      const client = getClient();
      const lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function (q, h, cb) {
        if (h.address === '0') {
          const err = new Error('Test prepare error');
          err.isSocketError = true;
          return cb(err);
        }
        cb(null, { id: 100, meta: {} });
      });
      PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null, function (err) {
        assert.ifError(err);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[0].prepareCalled, 1);
        assert.strictEqual(hosts[1].prepareCalled, 1);
        done();
      });
    });
    it('should prepare on all UP hosts not ignored', function (done) {
      const client = getClient({ prepareOnAllHosts: true });
      const lbp = helper.getLoadBalancingPolicyFake([ { isUp: false }, {}, {}, { ignored: true }, {} ]);
      PrepareHandler.getPrepared(client, lbp, 'SELECT QUERY', null, function (err) {
        assert.ifError(err);
        const hosts = lbp.getFixedQueryPlan();
        assert.strictEqual(hosts[1].prepareCalled, 1);
        assert.strictEqual(hosts[2].prepareCalled, 1);
        assert.strictEqual(hosts[4].prepareCalled, 1);
        assert.strictEqual(hosts[0].prepareCalled, 0);
        assert.strictEqual(hosts[3].prepareCalled, 0);
        done();
      });
    });
  });
  describe('prepareAllQueries', function () {
    it('should switch keyspace per each keyspace and execute', function (done) {
      const host = helper.getHostsMock([ {} ])[0];
      const preparedInfoArray = [
        { keyspace: 'system', query: 'query1' },
        { keyspace: 'system_schema', query: 'query2' },
        { keyspace: null, query: 'query3' },
        { keyspace: 'userks', query: 'query4' },
        { keyspace: 'system', query: 'query5' },
      ];
      PrepareHandler.prepareAllQueries(host, preparedInfoArray, function (err) {
        assert.ifError(err);
        assert.strictEqual(host.changeKeyspaceCalled, 3);
        assert.strictEqual(host.prepareCalled, 5);
        done();
      });
    });
    it('should callback when there are no queries to prepare', function (done) {
      PrepareHandler.prepareAllQueries({}, [], done);
    });
    it('should callback in error when there is an error borrowing a connection', function (done) {
      const host = helper.getHostsMock([ {} ])[0];
      host.borrowConnection = function (c, cb) {
        cb(new Error('Test error'));
      };
      PrepareHandler.prepareAllQueries(host, [{ query: 'query1' }], function (err) {
        helper.assertInstanceOf(err, Error);
        done();
      });
    });
    it('should callback in error when there is an error preparing any of the queries', function (done) {
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
      PrepareHandler.prepareAllQueries(host, preparedInfoArray, function (err) {
        helper.assertInstanceOf(err, Error);
        assert.strictEqual(host.changeKeyspaceCalled, 1);
        assert.strictEqual(host.prepareCalled, 2);
        done();
      });
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
          info = this._infos[ks + '.' + q] = new events.EventEmitter();
        }
        return info;
      },
      setPreparedById: utils.noop
    },
    options: utils.extend(defaultOptions(), options),
    profileManager: {
      getDistance: function (h) {
        return h.shouldBeIgnored ? types.distance.ignored : types.distance.local;
      }
    }
  };
}