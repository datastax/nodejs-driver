/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var errors = require('cassandra-driver').errors;
var helper = require('../helper.js');
var vdescribe = helper.vdescribe;
var utils = require('../../lib/utils.js');
var Client = require('../../lib/dse-client');
var ExecutionProfile = require('../../lib/execution-profile');

vdescribe('5.0', 'graph query client timeouts', function () {
  this.timeout(120000);
  function profiles() {
    return [
      new ExecutionProfile('123Profile', {readTimeout: 123}),
      new ExecutionProfile('default', {readTimeout: 112})
    ];
  }
  before(function (done) {
    var client = newInstance();
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {workloads: ['graph']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        var query = 'system.graph("name1").ifNotExists().create()';
        client.executeGraph(query, null, { graphName: null}, next);
      },
      client.shutdown.bind(client)
    ], done);
  });
  after(function (done) {
    helper.ccm.remove(done);
  });
  describe('when graphOptions.readTimeout is set', function () {
    it('should be used instead of socketOptions.readTimeout',
      getTimeoutTest(100, null, {graphOptions: {readTimeout: 100}}));
  });
  describe('when profile.readTimeout is set', function () {
    it('should be used instead of socketOptions.readTimeout',
      getTimeoutTest(123, {executionProfile: '123Profile'}, {profiles: profiles()}));
    it('should be used instead of graphOptions.readTimeout',
      getTimeoutTest(123, {executionProfile: '123Profile'}, {profiles: profiles(), graphOptions: {readTimeout: 20}}));
    it('should be used on default profile when no profile specified',
      getTimeoutTest(112, null, {profiles: profiles()}));
  });
  describe('when queryOptions.readTimeout is set', function () {
    it('should be used', getTimeoutTest(111, {readTimeout: 111}));
    it('should be used instead of profile',
      getTimeoutTest(92, {readTimeout: 92, executionProfile: '123Profile'}, {profiles: profiles()}));
    it('should be used instead of default profile', getTimeoutTest(93, {readTimeout: 93}, {profiles: profiles()}));
    it('should be used instead of graphOptions.readTimeout',
      getTimeoutTest(94, {readTimeout: 94}, {graphOptions: {readTimeout: 20}}));
  });
  describe('when readTimeout not set on profile or graphOptions', function() {
    it('should not encounter a client timeout and instead depend on server timeout', function (done) {
      var serverTimeoutErrRE = /^script evaluation exceeded.*?of (\d+) ms/i;
      // the read timeout on socket options should be completely ignored.
      var client = newInstance({socketOptions: {readTimeout: 1000}, graphOptions: {name: 'name1', source: '1sectimeout'}});
      utils.series([
        client.connect.bind(client),
        function setTimeoutOnSource (next) {
          client.executeGraph('graph.schema().config().option("graph.traversal_sources.1sectimeout.evaluation_timeout").set("1002 ms")', next);
        },
        function executeQuery (next) {
          client.executeGraph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(10000L);", function(err) {
            assert.ok(err);
            helper.assertInstanceOf(err, errors.ResponseError);
            // check that the error message indicates a timeout on the server side.
            var match = err.message.match(serverTimeoutErrRE);
            assert.ok(match);
            assert.ok(match[1]);
            assert.strictEqual(parseInt(match[1]), 1002);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.extend({graphOptions: {name: 'name1'}}, helper.baseOptions, options));
}

/**
 * executes a graph query with the given queryOptions using a client with the given clientOptions and expects a
 * timeout in expectedTimeoutMillis.
 *
 * @param {Number} expectedTimeoutMillis How long the timeout error reported is expected to be.
 * @param {QueryOptions} queryOptions options to use on executeGraph query.
 * @param {DseClientOptions} [clientOptions] options to use on client.
 */
function getTimeoutTest(expectedTimeoutMillis, queryOptions, clientOptions) {
  var operationTimeoutRE = /.*The host .* did not reply before timeout (\d+) ms.*/;
  return (function timeoutTest (done) {
    var client = newInstance(clientOptions);
    utils.series([
      client.connect.bind(client),
      function executeQuery (next) {
        client.executeGraph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(10000L);", {}, queryOptions, function(err, result) {
          assert.ok(err);
          assert.ifError(result);
          var match = err.message.match(operationTimeoutRE);
          assert.ok(match[1]);
          assert.strictEqual(parseInt(match[1]), expectedTimeoutMillis);
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
}
