'use strict';
const assert = require('assert');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');
const errors = require('../../../lib/errors');

const responseErrorCodes = require('../../../lib/types').responseErrorCodes;
const Client = require('../../../lib/client.js');
const DCAwareRoundRobinPolicy = require('../../../lib/policies').loadBalancing.DCAwareRoundRobinPolicy;
const WhiteListPolicy = require('../../../lib/policies').loadBalancing.WhiteListPolicy;

const query = "select * from data";

describe('Client', function() {
  this.timeout(30000);
  describe('#execute(query, params, {host: h})', () => {

    const setupInfo = simulacron.setup([4], { initClient: false });
    const cluster = setupInfo.cluster;
    let client;

    before(() => {
      client = new Client({
        contactPoints: [simulacron.startingIp],
        policies: {
          // define an LBP that includes all nodes except node 3
          loadBalancing: new WhiteListPolicy(new DCAwareRoundRobinPolicy(), [
            cluster.node(0).address,
            cluster.node(1).address,
            cluster.node(2).address
          ])
        }
      });
      return client.connect();
    });
  
    after(() => client.shutdown());
  
    it('should send request to host used in options', (done) => {
      utils.times(10, (n, next) => {
        const nodeIndex = n % 3;
        const node = cluster.node(nodeIndex);
        const host = client.hosts.get(node.address);
        client.execute(query, [], { host: host }, (err, result) => {
          assert.ifError(err);
          assert.strictEqual(result.info.queriedHost, node.address);
          assert.deepEqual(Object.keys(result.info.triedHosts), [node.address]);
          next();
        });
      }, done);
    });

    it('should throw an error if host used raises an error', (done) => {
      const node = cluster.node(0);
      const host = client.hosts.get(node.address);
      node.prime({
        when: {
          query: query
        },
        then: {
          result: 'unavailable',
          alive: 0,
          required: 1,
          consistency_level: 'LOCAL_ONE'
        }
      }, () => {
        client.execute(query, [], { host: host }, (err, result) => {
          assert.ok(err);
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.strictEqual(Object.keys(err.innerErrors).length, 1);
          const nodeError = err.innerErrors[node.address];
          assert.strictEqual(nodeError.code, responseErrorCodes.unavailableException);
          done();
        });
      });
    });
  
    it('should throw an error if host used in options is ignored by load balancing policy', () => {
      // since node 3 is not included in our LBP, the request should fail as we have no
      // connectivity to that node.
      const node = cluster.node(3);
      const host = client.hosts.get(node.address);
      let caughtErr = null;
      return client.execute(query, [], { host: host })
        .catch((err) => {
          caughtErr = err;
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          // no hosts should have been attempted.
          assert.strictEqual(Object.keys(err.innerErrors).length, 0);
        })
        .then(() => assert.ok(caughtErr));
    });
  });
});
