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

import assert from "assert";
import helper from "../../test-helper";
import utils from "../../../lib/utils";
import Client from "../../../lib/client";
import { Host } from "../../../lib/host";

describe('Client', function () {
  describe('events', function () {
    this.timeout(600000);
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should emit hostUp hostDown', function (done) {
      const client = newInstance();
      const hostsWentUp = [];
      const hostsWentDown = [];
      utils.series([
        client.connect.bind(client),
        function addListeners(next) {
          client.on('hostUp', hostsWentUp.push.bind(hostsWentUp));
          client.on('hostDown', hostsWentDown.push.bind(hostsWentDown));
          next();
        },
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        helper.toTask(helper.ccmHelper.startNode, null, 2),
        function waitForUp(next) {
          // We wait slightly before checking host states
          // because the node is marked UP just before the listeners are
          // called since CCM considers the node up as soon as other nodes
          // have seen it up, at which point they would send the
          // notification on the control connection so there is a very small
          // race here (evident in both 1.2 and 2.2).
          // If the host was already marked up, we simply proceed.
          if(hostsWentUp.length >= 1) {
            next();
          } else {
            // Set a timeout of 10 seconds to call next at which point
            // things will likely fail if the hostUp event has not been
            // surfaced yet.
            const timeout = setTimeout(next, 10000);
            client.on('hostUp', function() {
              // Call next on a timeout since this callback may be invoked
              // before the one that adds to hostsWentUp.
              setTimeout(next, 1);
              clearTimeout(timeout);
            });
          }
        },
        function checkResults(next) {
          assert.strictEqual(hostsWentUp.length, 1);
          helper.assertInstanceOf(hostsWentUp[0], Host);
          assert.strictEqual(helper.lastOctetOf(hostsWentUp[0]), '2');

          // Special exception for C* 1.x, as it may send duplicate down events
          // for a single host.
          assert.strictEqual(hostsWentDown.length, 1);
          hostsWentDown.forEach(function(downHost) {
            helper.assertInstanceOf(downHost, Host);
            assert.strictEqual(helper.lastOctetOf(downHost), '2');
          });
          next();
        },
        client.shutdown.bind(client)
      ], done);
    });
    it('should emit hostAdd hostRemove', function (done) {
      const client = newInstance();
      const hostsAdded = [];
      const hostsRemoved = [];
      function trace(message) {
        return (function (next) {
          helper.trace(message);
          next();
        });
      }
      utils.series([
        client.connect.bind(client),
        function addListeners(next) {
          client.on('hostAdd', hostsAdded.push.bind(hostsAdded));
          client.on('hostRemove', hostsRemoved.push.bind(hostsRemoved));
          next();
        },
        trace('Bootstrapping node 3'),
        helper.toTask(helper.ccmHelper.bootstrapNode, null, 3),
        trace('Starting newly bootstrapped node 3'),
        helper.toTask(helper.ccmHelper.startNode, null, 3),
        trace('Decommissioning node 2'),
        helper.toTask(helper.ccmHelper.decommissionNode, null, 2),
        trace('Stopping node 2'),
        helper.toTask(helper.ccmHelper.stopNode, null, 2),
        function checkResults(next) {
          helper.trace('Checking results');
          assert.strictEqual(hostsAdded.length, 1);
          assert.strictEqual(hostsRemoved.length, 1);
          helper.assertInstanceOf(hostsAdded[0], Host);
          helper.assertInstanceOf(hostsRemoved[0], Host);
          assert.strictEqual(helper.lastOctetOf(hostsAdded[0]), '3');
          assert.strictEqual(helper.lastOctetOf(hostsRemoved[0]), '2');
          next();
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}