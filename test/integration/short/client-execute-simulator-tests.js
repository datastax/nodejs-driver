'use strict';
const assert = require('assert');
const simulacron = require('../simulacron');
const helper = require('../../test-helper');
const utils = require('../../../lib/utils');
const util = require('util');
const errors = require('../../../lib/errors');

const Client = require('../../../lib/client.js');
const { responseErrorCodes } = require('../../../lib/types');
const { WhiteListPolicy, DCAwareRoundRobinPolicy } = require('../../../lib/policies').loadBalancing;

const query = "select * from data";

describe('Client', () => {
  describe('#execute(query, params, {host: h})', () => {

    const setupInfo = simulacron.setup([4], { initClient: false });
    const cluster = setupInfo.cluster;
    let client;

    before(() => {
      client = new Client({
        contactPoints: [simulacron.startingIp],
        policies: {
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

    it('should throw an error if host used raises an error', () => {
      const node = cluster.node(0);
      const host = client.hosts.get(node.address);
      const prime = util.promisify(node.prime.bind(node));
      return prime({
        when: {
          query: query
        },
        then: {
          result: 'unavailable',
          alive: 0,
          required: 1,
          consistency_level: 'LOCAL_ONE'
        }})
        .then(() => client.execute(query, [], { host: host }))
        .catch((err) => {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.deepEqual(Object.keys(err.innerErrors), [node.address]);
          const nodeError = err.innerErrors[node.address];
          assert.strictEqual(nodeError.code, responseErrorCodes.unavailableException);
        });
    });
  
    it('should throw an error if host used in options is ignored by load balancing policy', () => {
      const node = cluster.node(3);
      const host = client.hosts.get(node.address);
      return client.execute(query, [], { host: host })
        .catch((err) => {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
        });
    });
  });
});
