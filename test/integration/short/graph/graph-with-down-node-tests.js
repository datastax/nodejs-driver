/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const assert = require('assert');
const Client = require('../../../../lib/dse-client');
const helper = require('../../../test-helper');
const vdescribe = helper.vdescribe;
const types = require('../../../../lib/types');
const cl = types.consistencies;
const ExecutionProfile = require('../../../../lib/execution-profile').ExecutionProfile;
const utils = require('../../../../lib/utils');
const graphTestHelper = require('./graph-test-helper');

// DSP-15333 prevents this suite to be tested against DSE 5.0
vdescribe('dse-5.1', 'Client with down node', function () {
  this.timeout(270000);
  before(function (done) {
    const client = new Client(helper.getOptions());
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(3, {workloads: ['graph']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        const replicationConfig = "{'class' : 'SimpleStrategy', 'replication_factor' : 3}";
        const query = `system.graph("name1")
          .option("graph.replication_config").set(replicationConfig)
          .option("graph.system_replication_config").set(replicationConfig)
          .ifNotExists()
          ${helper.isDseGreaterThan('6.8') ? '.classicEngine()' : ''}
          .create()`;
        client.executeGraph(query, {replicationConfig: replicationConfig}, { graphName: null}, next);
      },
      next => graphTestHelper.createModernGraph(client, next),
      function simpleQuery(next) {
        // execute a graph traversal, which triggers some schema operations.
        client.executeGraph('g.V().limit(1)', null, { graphName: "name1" }, next);
      },
      function stopNode(next) {
        helper.ccm.stopNode(2, next);
      },
      // Wait for the down node to be marked as unavailable by the other nodes
      helper.delay(30000),
      client.shutdown.bind(client)
    ], done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  describe('#executeGraph()', function () {

    const addVertexQuery = 'graph.addVertex(label, "person", "name", "joe", "age", 42);';
    const getVertexQuery = 'g.V().limit(1)';

    function expectFailAtAll(done) {
      return (function(err, result) {
        assert.ok(err);
        assert.strictEqual(
          err.message, 'Not enough replicas available for query at consistency ALL (3 required but only 2 alive)');
        assert.strictEqual(err.code, types.responseErrorCodes.unavailableException);
        assert.strictEqual(result, undefined);
        done();
      });
    }

    it('should be able to make a read query with ONE read consistency, ALL write consistency', wrapClient(function (client, done) {
      client.executeGraph(getVertexQuery, null, {readTimeout: 5000, graphReadConsistency: cl.one, graphWriteConsistency: cl.all}, function (err, result) {
        assert.ifError(err);
        assert.ok(result.first());
        done();
      });
    }));

    it('should fail to make a read query with ALL read consistency', wrapClient(function (client, done) {
      client.executeGraph(getVertexQuery, null, {graphReadConsistency: cl.all}, expectFailAtAll(done));
    }));

    it('should be able to make a write query with ALL read consistency, TWO write consistency', wrapClient(function (client, done) {
      client.executeGraph(addVertexQuery, null, {readTimeout: 5000, graphReadConsistency: cl.all, graphWriteConsistency: cl.two}, function (err, result) {
        assert.ifError(err);
        assert.ok(result.first());
        done();
      });
    }));

    it('should fail to make a write query with ALL write consistency', wrapClient(function (client, done) {
      client.executeGraph(addVertexQuery, null, {graphWriteConsistency: cl.all}, expectFailAtAll(done));
    }));

    it('should use read consistency from profile', wrapClient(function (client, done) {
      client.executeGraph(getVertexQuery, null, {executionProfile: 'readALL'}, expectFailAtAll(done));
    }, {profiles: [new ExecutionProfile('readALL', {graphOptions: {readConsistency: cl.all}})]}));

    it('should use write consistency from profile', wrapClient(function (client, done) {
      client.executeGraph(addVertexQuery, null, {executionProfile: 'writeALL'}, expectFailAtAll(done));
    }, {profiles: [new ExecutionProfile('writeALL', {graphOptions: {writeConsistency: cl.all}})]}));

    it('should use read consistency from default profile', wrapClient(function (client, done) {
      client.executeGraph(getVertexQuery, expectFailAtAll(done));
    }, {profiles: [new ExecutionProfile('default', {graphOptions: {readConsistency: cl.all}})]}));

    it('should use write consistency from default profile', wrapClient(function (client, done) {
      client.executeGraph(addVertexQuery, expectFailAtAll(done));
    }, {profiles: [new ExecutionProfile('default', {graphOptions: {writeConsistency: cl.all}})]}));
  });
});

function wrapClient(handler, options) {
  return (function wrappedTestCase(done) {
    const client = newInstance(options);
    utils.series([
      client.connect.bind(client),
      function testItem(next) {
        handler(client, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
}

function newInstance(options) {
  const opts = helper.getOptions(utils.extend(options || {}, { graphOptions : { name: 'name1' }}));
  return new Client(opts);
}
