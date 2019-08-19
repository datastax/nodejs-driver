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

const assert = require('assert');
const os = require('os');
const Client = require('../../lib/dse-client');
const ClientState = require('../../lib/metadata/client-state');
const InsightsClient = require('../../lib/insights-client');
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const utils = require('../../lib/utils');
const types = require('../../lib/types');
const policies = require('../../lib/policies');
const coreConnectionsPerHostV3 = require('../../lib/client-options').coreConnectionsPerHostV3;
const packageInfo = require('../../package.json');
const helper = require('../test-helper');

const kerberosModule = helper.requireOptional('kerberos');
const kerberosDescribe = kerberosModule ? describe : xdescribe;

const startupEventName = 'driver.startup';
const statusEventName = 'driver.status';

describe('InsightsClient', function () {

  this.timeout(30000);

  describe('#init()', () => {

    it('should send the startup event rpc immediately', () =>
      getStartupMessage().then(result => {

        const message = result.message;
        const client = result.client;

        assert.ok(message.metadata);
        assert.strictEqual(message.metadata.name, startupEventName);
        assert.strictEqual(message.metadata.insightMappingId, 'v1');
        assert.strictEqual(message.metadata.insightType, 'EVENT');
        assert.strictEqual(typeof message.metadata.timestamp, 'number');
        assert.deepStrictEqual(message.metadata.tags, { language: 'nodejs' });

        assert.ok(message.data);
        assert.strictEqual(message.data.driverName, packageInfo.description);
        assert.strictEqual(message.data.driverVersion, packageInfo.version);
        assert.strictEqual(message.data.clientId, client.options.id.toString());
        // String representation of a Uuid
        assert.strictEqual(typeof message.data.sessionId, 'string');
        assert.strictEqual(message.data.sessionId.length, 36);
        assert.strictEqual(typeof message.data.contactPoints, 'object');
        assert.ok(Array.isArray(message.data.dataCenters));
        assert.strictEqual(message.data.protocolVersion, client.controlConnection.protocolVersion);
        assert.strictEqual(message.data.localAddress, client.controlConnection.getLocalAddress());
        assert.strictEqual(message.data.hostName, os.hostname());
        assert.strictEqual(typeof message.data.executionProfiles, 'object');
        assert.deepStrictEqual(message.data.poolSizeByHostDistance, { local: 1, remote: 1 });
        assert.strictEqual(typeof message.data.heartbeatInterval, 'number');
        assert.strictEqual(message.data.compression, 'NONE');
        assert.strictEqual(typeof message.data.reconnectionPolicy.type, 'string');
        assert.strictEqual(typeof message.data.reconnectionPolicy.options, 'object');
        assert.strictEqual(message.data.ssl.enabled, false);
        assert.strictEqual(message.data.ssl.certValidation, undefined);
        assert.strictEqual(message.data.authProvider.type, undefined);
        assert.strictEqual(typeof message.data.otherOptions.coalescingThreshold, 'number');
        assert.strictEqual(typeof message.data.platformInfo.os, 'object');
        assert.strictEqual(typeof message.data.platformInfo.cpus, 'object');
        assert.strictEqual(typeof message.data.platformInfo.runtime, 'object');
        assert.strictEqual(typeof message.data.configAntiPatterns, 'object');
        assert.deepEqual(Object.keys(message.data.configAntiPatterns), []);
        assert.strictEqual(message.data.periodicStatusInterval, 300);
      }));

    ['6.0.5', '5.1.13', '5.1.18', '6.7.1'].forEach(version => {
      it(`should send the startup event rpc when the server version is ${version}`, done => {
        let messageString;
        let error;

        const client = getClient({
          rpcCallback: m => messageString = m,
          hostVersions: [ '6.7.0', '6.8.0', '6.0.6', version]
        });

        const insights = new InsightsClient(client, {
          errorCallback: err => error = err
        });

        insights.init();

        setImmediate(() => {
          insights.shutdown();
          assert.ifError(error);
          assert.ok(messageString);
          done();
        });
      });
    });

    ['6.0.1', '5.1.2', '5.0.13', '4.8.11'].forEach(version => {
      it(`should not send the startup event rpc when the server version is ${version}`, done => {
        let messageString;
        let error;

        const client = getClient({
          rpcCallback: m => messageString = m,
          hostVersions: [ '6.7.0', '6.8.0', '6.0.6', version, '6.7.0']
        });

        const insights = new InsightsClient(client, {
          errorCallback: err => error = err
        });

        insights.init();

        setImmediate(() => {
          insights.shutdown();
          assert.ifError(error);
          assert.strictEqual(typeof messageString, 'undefined');
          done();
        });
      });
    });

    it('should schedule the first status event', done => {
      const messages = [];

      const client = getClient({
        rpcCallback: m => messages.push(m)
      });

      const insights = new InsightsClient(client, {
        errorCallback: err => assert.ifError(err),
        statusEventDelay: 40
      });

      insights.init();

      setTimeout(() => {
        insights.shutdown();

        // The startup and status events are expected
        assert.strictEqual(messages.length, 2);

        const startupMessage = JSON.parse(messages[0]);
        assert.strictEqual(startupMessage.metadata.name, startupEventName);
        assert.strictEqual(typeof startupMessage.data.sessionId, 'string');
        assert.strictEqual(startupMessage.data.sessionId.length, 36);

        const statusMessage = JSON.parse(messages[1]);
        assert.strictEqual(statusMessage.metadata.name, statusEventName);
        assert.deepStrictEqual(statusMessage.metadata.tags, { language: 'nodejs' });
        assert.strictEqual(statusMessage.data.sessionId, startupMessage.data.sessionId);
        assert.strictEqual(statusMessage.data.clientId, startupMessage.data.clientId);
        assert.deepEqual(statusMessage.data.connectedNodes,
          { '1.2.3.4:9042': { connections: 2, inFlightQueries: 123 } });

        done();
      }, 60);
    });

    it('should not schedule the status event when startup fails', done => {
      const error = new Error('Test error');
      let errorCallbackCalled = 0;
      let rpcCalled = 0;

      const client = getClient({
        rpcCallback: () => rpcCalled++,
        callbackParameterGetter: () => error
      });

      const insights = new InsightsClient(client, {
        errorCallback: err => {
          assert.strictEqual(err, error);
          errorCallbackCalled++;
        },
        statusEventDelay: 10
      });

      insights.init();

      setTimeout(() => {
        insights.shutdown();
        assert.strictEqual(errorCallbackCalled, 1);
        // Only the initial RPC called is expected
        assert.strictEqual(rpcCalled, 1);
        done();
      }, 20);
    });

    it('should schedule recurrent status events until shutdown() is cancelled', done => {
      const messages = [];

      const client = getClient({
        rpcCallback: m => messages.push(m)
      });

      const insights = new InsightsClient(client, {
        errorCallback: err => assert.ifError(err),
        statusEventDelay: 2
      });

      insights.init();

      setTimeout(() => {
        insights.shutdown();

        // The startup and status events are expected
        assert.ok(messages.length > 4, `Message length should be greater than 4, was ${messages.length}`);

        const startupMessage = JSON.parse(messages[0]);
        assert.strictEqual(startupMessage.metadata.name, startupEventName);
        assert.strictEqual(typeof startupMessage.data.sessionId, 'string');
        assert.strictEqual(startupMessage.data.sessionId.length, 36);

        const firstStatusMessage = JSON.parse(messages[1]);

        messages.slice(1).forEach(m => {
          const statusMessage = JSON.parse(m);
          assert.strictEqual(statusMessage.metadata.clientId, startupMessage.metadata.clientId);
          assert.strictEqual(statusMessage.metadata.sessionId, startupMessage.metadata.sessionId);
          assert.deepStrictEqual(statusMessage.data, firstStatusMessage.data);
        });

        done();
      }, 20);
    });

    it('should include all the options in execution profiles', () =>
      getStartupMessage().then(result => {
        const defaultProfile = result.message.data.executionProfiles['default'];
        const expected = result.client.profileManager.getDefault();
        const expectedProperties = Object.keys(expected)
          .filter(prop =>
            !prop.startsWith('_') && ['name', 'graphOptions'].indexOf(prop) === -1 && expected[prop] !== undefined)
          .concat(['speculativeExecution']);

        assert.deepStrictEqual(
          Object.keys(defaultProfile).sort(),
          expectedProperties.sort());
      }));

    it('should include the speculative execution policy in the execution profiles', () =>
      getStartupMessage().then(result => {
        utils.objectValues(result.message.data.executionProfiles).forEach(execProfile => {
          assert.strictEqual(typeof execProfile.speculativeExecution.type, 'string');
          assert.strictEqual(typeof execProfile.speculativeExecution.options, 'object');
        });
      }));

    it('should include the execution profile options that differ from default', () => {
      const clientOptions = { profiles: [
        new ExecutionProfile('time-series', {
          readTimeout: 15000,
          consistency: types.consistencies.localQuorum,
          serialConsistency: types.consistencies.localSerial,
          graphOptions:  { name: 'myGraph' }
        }),
        new ExecutionProfile('default', {
          consistency: types.consistencies.localOne,
          readTimeout: 15000,
          serialConsistency: types.consistencies.localSerial
        })
      ]};

      return getStartupMessage({clientOptions}).then(result => {
        // Default profile should be the first
        assert.deepStrictEqual(Object.keys(result.message.data.executionProfiles), ['default', 'time-series']);
        const timeSeriesProfile = result.message.data.executionProfiles['time-series'];
        // Should output only the ones that differ from the default profile
        assert.deepStrictEqual(Object.keys(timeSeriesProfile), ['consistency', 'graphOptions']);
      });
    });

    it('should include the different data centers', () => {
      const options = {
        hostVersions: ['6.7.1', '6.7.1', '6.7.1'],
        clientOptions: { policies: { loadBalancing: new policies.loadBalancing.RoundRobinPolicy() }}
      };

      return getStartupMessage(options).then(result => {
        assert.deepStrictEqual(result.message.data.dataCenters, [ 'dc0', 'dc1' ]);
      });
    });

    it('should include data centers where distance is local', () => {
      const lbp = new policies.loadBalancing.RoundRobinPolicy();
      lbp.getDistance = h => (h.datacenter === 'dc0' ? types.distance.local : types.distance.ignored);

      const options = {
        hostVersions: ['6.7.1', '6.7.1', '6.7.1'],
        clientOptions: { policies: { loadBalancing: lbp }}
      };

      return getStartupMessage(options).then(result => {
        assert.deepStrictEqual(result.message.data.dataCenters, [ 'dc0' ]);
      });
    });

    it('should not include data centers where distance is remote when remote connection length is zero', () => {
      const lbp = new policies.loadBalancing.RoundRobinPolicy();
      lbp.getDistance = h => (h.datacenter === 'dc0' ? types.distance.local : types.distance.remote);

      const options = {
        hostVersions: ['6.7.1', '6.7.1', '6.7.1'],
        clientOptions: {
          policies: { loadBalancing: lbp },
          pooling: { coreConnectionsPerHost: { [types.distance.local]: 2, [types.distance.remote]: 0 } }
        }
      };

      return getStartupMessage(options).then(result => {
        assert.deepStrictEqual(result.message.data.dataCenters, [ 'dc0' ]);
      });
    });

    it('should include data centers where distance is remote when remote connection length greater than zero', () => {
      const lbp = new policies.loadBalancing.RoundRobinPolicy();
      lbp.getDistance = h => (h.datacenter === 'dc0' ? types.distance.local : types.distance.remote);

      const options = {
        hostVersions: ['6.7.1', '6.7.1', '6.7.1'],
        clientOptions: {
          policies: { loadBalancing: lbp },
          pooling: { coreConnectionsPerHost: { [types.distance.local]: 2, [types.distance.remote]: 1 } }
        }
      };

      return getStartupMessage(options).then(result => {
        assert.deepStrictEqual(result.message.data.dataCenters, [ 'dc0', 'dc1' ]);
      });
    });

    it('should include the provided application name and version', () => {
      const clientOptions = {
        applicationName: 'My Test App',
        applicationVersion: '3.1.4'
      };

      return getStartupMessage({ clientOptions }).then(result => {
        assert.strictEqual(result.message.data.applicationName, clientOptions.applicationName);
        assert.strictEqual(result.message.data.applicationVersion, clientOptions.applicationVersion);
        assert.strictEqual(result.message.data.applicationNameWasGenerated, false);
      });
    });

    it('should generate the application name and set the generated flag', () =>
      getStartupMessage({ clientOptions: { applicationName: null } }).then(result => {
        // The applicationName obtained depends on the entry point
        // In the case of mocha, it's not useful so it sets it to the default application name
        assert.strictEqual(result.message.data.applicationName, 'Default Node.js Application');
        assert.strictEqual(result.message.data.applicationNameWasGenerated, true);
      }));

    kerberosDescribe('with kerberos dependency', () => {
      it('should output the library version', () =>
        getStartupMessage().then(result => {
          assert.strictEqual(typeof result.message.data.platformInfo.runtime, 'object');
          assert.strictEqual(result.message.data.platformInfo.runtime.kerberos, kerberosModule.version);
        }));
    });
  });

  describe('#shutdown()', () => {

    it('should clear timeouts when called after startup event is sent', done => {
      let messages = 0;
      const statusEventDelay = 100;

      const client = getClient({
        rpcCallback: m => messages++
      });

      const insights = new InsightsClient(client, {
        errorCallback: err => assert.ifError(err),
        statusEventDelay
      });

      insights.init();

      setTimeout(() => {
        // Call shutdown after the startup message
        insights.shutdown();

        assert.strictEqual(messages, 1);

        // Schedule a timer that should be fired after the status event delay (if any)
        setTimeout(() => {
          assert.strictEqual(messages, 1);
          done();
        }, statusEventDelay);
      }, 20);
    });

    it('should prevent scheduling of timeouts when called before the sending of the startup event', () => {
      const client = getClient();

      const insights = new InsightsClient(client, {
        errorCallback: err => assert.ifError(err),
        statusEventDelay: 100
      });

      insights.init();

      insights.shutdown();
    });

    it('should prevent scheduling of timeouts when called during the sending of the startup event', done => {
      const client = getClient();

      const insights = new InsightsClient(client, {
        errorCallback: err => assert.ifError(err),
        statusEventDelay: 100
      });

      // Schedule for next event loop tick
      setImmediate(() => insights.shutdown());

      insights.init();

      setImmediate(() => done());
    });
  });
});

/**
 * Gets a fake client instance.
 * @param {{rpcCallback, callbackParameterGetter, hostVersions, clientOptions}} [options]
 */
function getClient(options) {
  options = options || {};

  const rpcCallback = options.rpcCallback || utils.noop;
  const callbackParameterGetter = options.callbackParameterGetter || utils.noop;

  const clientOptions = Object.assign(
    { pooling: { coreConnectionsPerHost: coreConnectionsPerHostV3 }, applicationName: 'My Test Application' },
    options.clientOptions);

  const client = new Client(helper.getOptions(clientOptions));

  // Provide a fake control connection
  client.controlConnection.query = (request, w, callback) => {
    rpcCallback(request.params[0]);
    setImmediate(() => callback(callbackParameterGetter()));
  };
  client.controlConnection.protocolVersion = 4;
  client.controlConnection.getLocalAddress = () => '10.10.10.1:9042';

  client.getState = () => new ClientState([{ address: '1.2.3.4:9042'}], { '1.2.3.4:9042': 2 }, { '1.2.3.4:9042': 123 });

  const hostVersions = options.hostVersions || [ '6.7.0' ];

  hostVersions.forEach((v, i) => {
    client.hosts.set(`10.10.10.${i}`, {
      getDseVersion: () => v.split('.'),
      datacenter: 'dc' + (i % 2),
      setDistance: () => {}
    });
  });

  return client;
}

/**
 * @param {{hostVersions, clientOptions}} [options]
 * @returns {Promise}
 */
function getStartupMessage(options) {
  let messageString;
  let error;
  options = options || {};

  const client = getClient({
    rpcCallback: m => messageString = m,
    hostVersions: options.hostVersions,
    clientOptions: options.clientOptions
  });

  const insights = new InsightsClient(client, {
    errorCallback: err => error = err
  });

  insights.init();

  return new Promise((resolve, reject) => {
    setImmediate(() => {
      insights.shutdown();

      if (error) {
        return reject(error);
      }

      // Wait for activity in the background
      helper.setIntervalUntil(
        () => !!messageString,
        20,
        500,
        err => {
          if (err) {
            return reject('No RPC was invoked');
          }

          resolve({ message: JSON.parse(messageString), client });
        }
      );
    });
  });
}