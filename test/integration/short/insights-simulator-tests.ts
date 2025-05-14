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
import simulacron from "../simulacron";
import helper from "../../test-helper";
import Client from "../../../lib/client";
import utils from "../../../lib/utils";
import InsightsClient from "../../../lib/insights-client";


const vdescribe = helper.vdescribe;
const insightsRpcQuery = 'CALL InsightsRpc.reportInsight(?)';

vdescribe('dse-6.7', 'InsightsClient', function () {
  this.timeout(40000);

  before(done => simulacron.start(done));
  after(done => simulacron.stop(done));

  let cluster;
  beforeEach(done => {
    cluster = new simulacron.SimulacronCluster();
    cluster.register([3], null, done);
  });
  beforeEach(done => cluster.prime({
    when: { query: insightsRpcQuery },
    then: { result: 'void', delay_in_ms: 0 }
  }, done));
  afterEach(done => cluster.unregister(done));

  it('should send startup message when a client is connected', () => {
    const firstAddress = cluster.getContactPoints()[0];
    const node = cluster.node(firstAddress);
    const client = new Client(helper.getOptions({ contactPoints: [ firstAddress ] }));

    after(() => client.shutdown());

    let rpcLog;
    let attempts = 0;

    return client.connect()
      .then(() => new Promise((resolve, reject) => {
        utils.whilst(
          () => !rpcLog && attempts++ < 10,
          next => {
            node.getLogs((err, logs) => {
              if (err) {
                return next(err);
              }

              rpcLog = logs.find(l => l.type === 'QUERY' && l.query === insightsRpcQuery);

              setTimeout(next, !rpcLog ? 100 : 0);
            });
          },
          err => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
      }))
      .then(() => {
        assert.ok(rpcLog, 'RPC query not found');
        const paramsBase64 = rpcLog.frame.message.options.positional_values;
        assert.strictEqual(paramsBase64.length, 1);

        // Validate a few message properties for sanity
        const message = JSON.parse(parseBase64Text(paramsBase64[0]));
        assert.strictEqual(message.metadata.name, 'driver.startup');
        assert.strictEqual(message.data.initialControlConnection, firstAddress);
        assert.ok(message.data.sessionId);
        assert.ok(message.data.clientId);
        assert.strictEqual(message.data.applicationNameWasGenerated, true);
        assert.strictEqual(message.data.compression, 'NONE');
        assert.deepStrictEqual(message.data.poolSizeByHostDistance, { local: 1, remote: 1 });
      })
      .then(() => client.shutdown());
  });

  it('should send status message periodically', () => {
    const firstAddress = cluster.getContactPoints()[0];
    const node = cluster.node(firstAddress);
    const statusEventDelay = 400;

    // Use a Client instance and a different InsightsClient instance (to provide different parameters)
    const client = new Client(helper.getOptions({ contactPoints: [ firstAddress ] }));
    const insightsClient = new InsightsClient(client, { statusEventDelay });

    after(() => client.shutdown());

    return client.connect()
      .then(() => new Promise(r => setImmediate(r)))
      .then(() => {
        // Insights client initializes in the background
        insightsClient.init();

        // Wait for the status event to be sent
        return new Promise(r => setTimeout(r, statusEventDelay + 100));
      })
      .then(() => new Promise((resolve, reject) => node.getLogs((err, logs) => (err ? reject(err) : resolve(logs)))))
      .then(logs => {
        const statusMessages = logs
          .filter(l => l.type === 'QUERY' && l.query === insightsRpcQuery)
          .map(l => parseBase64Text(l.frame.message.options.positional_values[0]))
          .filter(text => text.indexOf('"driver.status"') > 0);

        // Sometimes simulacron logs may contain invalid messages
        // Avoid json parsing it, see: https://github.com/datastax/simulacron/issues/74
        assert.ok(statusMessages.length > 0);
      })
      .then(() => insightsClient.shutdown())
      .then(() => client.shutdown());
  });
});

function parseBase64Text(text) {
  return utils.allocBufferFromString(text, 'base64').toString();
}