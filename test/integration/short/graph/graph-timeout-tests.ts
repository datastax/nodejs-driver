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

import assert from "assert";
import errors from "../../../../lib/errors";
import helper from "../../../test-helper";
import utils from "../../../../lib/utils";
import Client from "../../../../lib/client";
import {ExecutionProfile} from "../../../../lib/execution-profile";
import {RetryPolicy as DefaultRetryPolicy} from "../../../../lib/policies/retry";


const vdescribe = helper.vdescribe;

vdescribe('dse-5.0', 'graph query client timeouts', function () {
  this.timeout(120000);
  function profiles() {
    return [
      new ExecutionProfile('123Profile', {readTimeout: 123}),
      new ExecutionProfile('default', {readTimeout: 112}),
      new ExecutionProfile('withRetryPolicy', {readTimeout: 114, retry: new DefaultRetryPolicy() })
    ];
  }
  before(function (done) {
    const client = newInstance();
    utils.series([
      function startCcm(next) {
        helper.ccm.startAll(1, {workloads: ['graph']}, next);
      },
      client.connect.bind(client),
      function createGraph(next) {
        const query = `system.graph("name1")
          .ifNotExists()
          ${helper.isDseGreaterThan('6.8') ? '.classicEngine()' : ''}
          .create()`;
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
      const serverTimeoutErrRE = /evaluation exceeded.*\s+(\d+) ?ms/i;
      // the read timeout on socket options should be completely ignored.
      const client = newInstance({socketOptions: {readTimeout: 1000}, graphOptions: {name: 'name1', source: '1sectimeout'}});
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
            const match = err.message.match(serverTimeoutErrRE);
            assert.ok(match);
            assert.ok(match[1]);
            assert.strictEqual(parseFloat(match[1]), 1002);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
  describe('when readTimeout elapses', function () {
    it('should not retry and callback with OperationTimedOutError', function (done) {
      const client = newInstance({ graphOptions: {name: 'name1'}});
      utils.series([
        client.connect.bind(client),
        function executeQuery (next) {
          // use a ridiculously small client readTimeout to ensure it elapses client side
          client.executeGraph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2000L);", null, { readTimeout: 1}, function(err) {
            assert.ok(err);
            // A NoHostAvailableError would indicate that retry policy is attempting to retry
            helper.assertInstanceOf(err, errors.OperationTimedOutError);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retry on timeout when using profile with DefaultRetryPolicy', function(done) {
      const client = newInstance({ graphOptions: {name: 'name1'} , profiles: profiles()});
      utils.series([
        client.connect.bind(client),
        function executeQuery (next) {
          client.executeGraph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2000L);", null,
            { executionProfile: 'withRetryPolicy', isIdempotent: true }, function(err) {
              assert.ok(err);
              helper.assertInstanceOf(err, errors.NoHostAvailableError);
              next();
            });
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should retry on timeout when default profile uses DefaultRetryPolicy', function(done) {
      const client = newInstance({ graphOptions: {name: 'name1'} , profiles: [ new ExecutionProfile('default',
        {readTimeout: 100, retry: new DefaultRetryPolicy() })]});
      utils.series([
        client.connect.bind(client),
        function executeQuery (next) {
          const query = "java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2000L);";
          client.executeGraph(query, null, { isIdempotent: true }, function(err) {
            assert.ok(err);
            helper.assertInstanceOf(err, errors.NoHostAvailableError);
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
  const operationTimeoutRE = /.*The host .* did not reply before timeout (\d+) ms.*/;
  return (function timeoutTest (done) {
    const client = newInstance(clientOptions);
    utils.series([
      client.connect.bind(client),
      function executeQuery (next) {
        client.executeGraph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(10000L);", {}, queryOptions, function(err, result) {
          assert.ok(err);
          assert.ifError(result);
          // Having a NoHostAvailableError means that the retry policy insisted on retrying (but there is just 1 node)
          // Graph statements should not be retried
          assert.ok(!(err instanceof errors.NoHostAvailableError), 'Error should be rethrown by the retry policy');
          const match = err.message.match(operationTimeoutRE);
          assert.ok(match && match[1], `Message does not match: "${err.message}"`);
          assert.strictEqual(parseFloat(match[1]), expectedTimeoutMillis);
          next();
        });
      },
      client.shutdown.bind(client)
    ], done);
  });
}
