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

const os = require('os');
const path = require('path');
const fs = require('fs');
const utils = require('./utils');
const promiseUtils = require('./promise-utils');
const types = require('./types');
const requests = require('./requests');
const { ExecutionOptions } = require('./execution-options');
const packageInfo = require('../package.json');
const VersionNumber = require('./types/version-number');
const { NoAuthProvider } = require('./auth');

let kerberosModule;

try {
  // eslint-disable-next-line
  kerberosModule = require('kerberos');
}
catch (err) {
  // Kerberos is an optional dependency
}

const minDse6Version = new VersionNumber(6, 0, 5);
const minDse51Version = new VersionNumber(5, 1, 13);
const dse600Version = new VersionNumber(6, 0, 0);
const rpc = "CALL InsightsRpc.reportInsight(?)";
const maxStatusErrorLogs = 5;

/**
 * Contains methods and functionality to send events to DSE Insights.
 */
class InsightsClient {

  /**
   * Creates a new instance of the {@link InsightsClient} using the driver {@link Client}.
   * @param {Client} client
   * @param {Object} [options]
   * @param {Number} [options.statusEventDelay]
   * @param {Function} [options.errorCallback]
   */
  constructor(client, options) {
    this._client = client;
    this._sessionId = types.Uuid.random().toString();
    this._enabled = false;
    this._closed = false;
    this._firstTimeout = null;
    this._recurrentTimeout = null;
    this._statusErrorLogs = 0;

    options = options || {};

    this._statusEventDelay = options.statusEventDelay || 300000;
    this._errorCallback = options.errorCallback || utils.noop;
  }

  /**
   * Initializes the insights client in the background by sending the startup event and scheduling status events at
   * regular intervals.
   * @returns {undefined}
   */
  init() {
    this._enabled = this._client.options.monitorReporting.enabled && this._dseSupportsInsights();
    if (!this._enabled) {
      return;
    }

    promiseUtils.toBackground(this._init());
  }

  async _init() {
    try {
      await this._sendStartupEvent();

      if (this._closed) {
        // The client was shutdown
        return;
      }

      // Send the status event the first time with a delay containing some random portion
      // Initial delay should be statusEventDelay - (0 to 10%)
      const firstDelay = Math.floor(this._statusEventDelay - 0.1 * this._statusEventDelay * Math.random());
      // Schedule the first timer
      this._firstTimeout = setTimeout(() => {
        // Send the first status event, the promise will never be rejected
        this._sendStatusEvent();
        // The following status events are sent at regular intervals
        this._recurrentTimeout = setInterval(() => this._sendStatusEvent(), this._statusEventDelay);
      }, firstDelay);
    } catch (err) {
      if (this._closed) {
        // Sending failed because the Client was shutdown
        return;
      }
      // We shouldn't try to recover
      this._client.log('verbose', `Insights startup message could not be sent (${err})`, err);
      this._errorCallback(err);
    }
  }

  /**
   * Sends the startup event.
   * @returns {Promise}
   * @private
   */
  async _sendStartupEvent() {
    const message = await this._getStartupMessage();
    const request = new requests.QueryRequest(rpc, [message], ExecutionOptions.empty());
    await this._client.controlConnection.query(request, false);
  }

  /**
   * Sends the status event.
   * @returns {Promise} A promise that is never rejected.
   * @private
   */
  async _sendStatusEvent() {
    const request = new requests.QueryRequest(rpc, [ this._getStatusEvent() ], ExecutionOptions.empty());

    try {
      await this._client.controlConnection.query(request, false);
    } catch (err) {
      if (this._closed) {
        // Sending failed because the Client was shutdown
        return;
      }

      if (this._statusErrorLogs < maxStatusErrorLogs) {
        this._client.log('warning', `Insights status message could not be sent (${err})`, err);
        this._statusErrorLogs++;
      }

      this._errorCallback(err);
    }
  }

  /**
   * Validates the minimum server version for all nodes in the cluster.
   * @private
   */
  _dseSupportsInsights() {
    if (this._client.hosts.length === 0) {
      return false;
    }

    return this._client.hosts.values().reduce((acc, host) => {
      if (!acc) {
        return acc;
      }

      const versionArr = host.getDseVersion();

      if (versionArr.length === 0) {
        return false;
      }

      const version = new VersionNumber(...versionArr);

      return version.compare(minDse6Version) >= 0 ||
        (version.compare(dse600Version) < 0 && version.compare(minDse51Version) >= 0);

    }, true);
  }

  /**
   * @returns {Promise<String>} Returns a json string with the startup message.
   * @private
   */
  async _getStartupMessage() {
    const cc = this._client.controlConnection;
    const options = this._client.options;


    const appInfo = await this._getAppInfo(options);
    const message = {
      metadata: {
        name: 'driver.startup',
        insightMappingId: 'v1',
        insightType: 'EVENT',
        timestamp: Date.now(),
        tags: { language: 'nodejs' }
      },
      data: {
        driverName: packageInfo.description,
        driverVersion: packageInfo.version,
        clientId: options.id,
        sessionId: this._sessionId,
        applicationName: appInfo.applicationName,
        applicationVersion: appInfo.applicationVersion,
        applicationNameWasGenerated: appInfo.applicationNameWasGenerated,
        contactPoints: mapToObject(cc.getResolvedContactPoints()),
        dataCenters: this._getDataCenters(),
        initialControlConnection: cc.host ? cc.host.address : undefined,
        protocolVersion: cc.protocolVersion,
        localAddress: cc.getLocalAddress(),
        hostName: os.hostname(),
        executionProfiles: getExecutionProfiles(this._client),
        poolSizeByHostDistance: {
          local: options.pooling.coreConnectionsPerHost[types.distance.local],
          remote: options.pooling.coreConnectionsPerHost[types.distance.remote]
        },
        heartbeatInterval: options.pooling.heartBeatInterval,
        compression: 'NONE',
        reconnectionPolicy: getPolicyInfo(options.policies.reconnection),
        ssl: {
          enabled: !!options.sslOptions,
          certValidation: options.sslOptions ? !!options.sslOptions.rejectUnauthorized : undefined
        },
        authProvider: {
          type: !(options.authProvider instanceof NoAuthProvider) ? getConstructor(options.authProvider) : undefined,
        },
        otherOptions: {
          coalescingThreshold: options.socketOptions.coalescingThreshold,
        },
        platformInfo: {
          os: {
            name: os.platform(),
            version: os.release(),
            arch: os.arch()
          },
          cpus: {
            length: os.cpus().length,
            model: os.cpus()[0].model
          },
          runtime: {
            node: process.versions['node'],
            v8: process.versions['v8'],
            uv: process.versions['uv'],
            openssl: process.versions['openssl'],
            kerberos: kerberosModule ? kerberosModule.version : undefined
          }
        },
        configAntiPatterns: this._getConfigAntiPatterns(),
        periodicStatusInterval: Math.floor(this._statusEventDelay / 1000)
      }
    };

    return JSON.stringify(message);
  }

  _getConfigAntiPatterns() {
    const options = this._client.options;
    const result = {};

    if (options.sslOptions && !options.sslOptions.rejectUnauthorized) {
      result.sslWithoutCertValidation =
        'Client-to-node encryption is enabled but server certificate validation is disabled';
    }

    return result;
  }

  /**
   * Gets an array of data centers the driver connects to.
   * Whether the driver connects to a certain host is determined by the host distance (local and remote hosts)
   * and the pooling options (whether connection length for remote hosts is greater than 0).
   * @returns {Array}
   * @private
   */
  _getDataCenters() {
    const remoteConnectionsLength = this._client.options.pooling.coreConnectionsPerHost[types.distance.remote];
    const dataCenters = new Set();

    this._client.hosts.values().forEach(h => {
      const distance = this._client.profileManager.getDistance(h);
      if (distance === types.distance.local || (distance === types.distance.remote && remoteConnectionsLength > 0)) {
        dataCenters.add(h.datacenter);
      }
    });

    return Array.from(dataCenters);
  }

  /**
   * Tries to obtain the application name and version from
   * @param {DseClientOptions} options
   * @returns {Promise}
   * @private
   */
  async _getAppInfo(options) {
    if (typeof options.applicationName === 'string') {
      return Promise.resolve({
        applicationName: options.applicationName,
        applicationVersion: options.applicationVersion,
        applicationNameWasGenerated: false
      });
    }

    let readPromise = Promise.resolve();

    if (require.main && require.main.filename) {
      const packageInfoPath = path.dirname(require.main.filename);
      readPromise = this._readPackageInfoFile(packageInfoPath);
    }

    const text = await readPromise;
    let applicationName = 'Default Node.js Application';
    let applicationVersion;

    if (text) {
      try {
        const packageInfo = JSON.parse(text);
        if (packageInfo.name) {
          applicationName = packageInfo.name;
          applicationVersion = packageInfo.version;
        }
      }
      catch (err) {
        // The package.json file could not be parsed
        // Use the default name
      }
    }

    return {
      applicationName,
      applicationVersion,
      applicationNameWasGenerated: true
    };
  }

  /**
   * @private
   * @returns {Promise<string>} A Promise that will never be rejected
   */
  _readPackageInfoFile(packageInfoPath) {
    return new Promise(resolve => {
      fs.readFile(path.join(packageInfoPath, 'package.json'), 'utf8', (err, data) => {
        // Swallow error
        resolve(data);
      });
    });
  }

  /**
   * @returns {String} Returns a json string with the startup message.
   * @private
   */
  _getStatusEvent() {
    const cc = this._client.controlConnection;
    const options = this._client.options;
    const state = this._client.getState();
    const connectedNodes = {};

    state.getConnectedHosts().forEach(h => {
      connectedNodes[h.address] = {
        connections: state.getOpenConnections(h),
        inFlightQueries: state.getInFlightQueries(h)
      };
    });

    const message = {
      metadata: {
        name: 'driver.status',
        insightMappingId: 'v1',
        insightType: 'EVENT',
        timestamp: Date.now(),
        tags: { language: 'nodejs' }
      },
      data: {
        clientId: options.id,
        sessionId: this._sessionId,
        controlConnection: cc.host ? cc.host.address : undefined,
        connectedNodes
      }
    };

    return JSON.stringify(message);
  }

  /**
   * Cleans any timer used internally and sets the client as closed.
   */
  shutdown() {
    if (!this._enabled) {
      return;
    }

    this._closed = true;

    if (this._firstTimeout !== null) {
      clearTimeout(this._firstTimeout);
    }

    if (this._recurrentTimeout !== null) {
      clearInterval(this._recurrentTimeout);
    }
  }
}

module.exports = InsightsClient;

function mapToObject(map) {
  const result = {};
  map.forEach((value, key) => result[key] = value);
  return result;
}

function getPolicyInfo(policy) {
  if (!policy) {
    return undefined;
  }

  const options = policy.getOptions && policy.getOptions();

  return {
    type: policy.constructor.name,
    options: (options instanceof Map) ? mapToObject(options) : utils.emptyObject
  };
}

function getConsistencyString(c) {
  if (typeof c !== 'number') {
    return undefined;
  }

  return types.consistencyToString[c];
}

function getConstructor(instance) {
  return instance ? instance.constructor.name : undefined;
}

function getExecutionProfiles(client) {
  const executionProfiles = {};

  const defaultProfile = client.profileManager.getDefault();
  setExecutionProfileProperties(client, executionProfiles, defaultProfile, defaultProfile);

  client.profileManager.getAll()
    .filter(p => p !== defaultProfile)
    .forEach(profile => setExecutionProfileProperties(client, executionProfiles, profile, defaultProfile));

  return executionProfiles;
}

function setExecutionProfileProperties(client, parent, profile, defaultProfile) {
  const output = parent[profile.name] = {};
  setExecutionProfileItem(output, profile, defaultProfile, 'readTimeout');
  setExecutionProfileItem(output, profile, defaultProfile, 'loadBalancing', getPolicyInfo);
  setExecutionProfileItem(output, profile, defaultProfile, 'retry', getPolicyInfo);
  setExecutionProfileItem(output, profile, defaultProfile, 'consistency', getConsistencyString);
  setExecutionProfileItem(output, profile, defaultProfile, 'serialConsistency', getConsistencyString);

  if (profile === defaultProfile) {
    // Speculative execution policy is included in the profiles as some drivers support
    // different spec exec policy per profile, in this case is fixed for all profiles
    output.speculativeExecution = getPolicyInfo(client.options.policies.speculativeExecution);
  }

  if (profile.graphOptions) {
    output.graphOptions = {};
    const defaultGraphOptions = defaultProfile.graphOptions || utils.emptyObject;
    setExecutionProfileItem(output.graphOptions, profile.graphOptions, defaultGraphOptions, 'language');
    setExecutionProfileItem(output.graphOptions, profile.graphOptions, defaultGraphOptions, 'name');
    setExecutionProfileItem(output.graphOptions, profile.graphOptions, defaultGraphOptions, 'readConsistency',
      getConsistencyString);
    setExecutionProfileItem(output.graphOptions, profile.graphOptions, defaultGraphOptions, 'source');
    setExecutionProfileItem(output.graphOptions, profile.graphOptions, defaultGraphOptions, 'writeConsistency',
      getConsistencyString);

    if (Object.keys(output.graphOptions).length === 0) {
      // Properties that are undefined will not be included in the JSON
      output.graphOptions = undefined;
    }
  }
}

function setExecutionProfileItem(output, profile, defaultProfile, prop, valueGetter) {
  const value = profile[prop];
  valueGetter = valueGetter || (x => x);

  if ((profile === defaultProfile && value !== undefined) || value !== defaultProfile[prop]) {
    output[prop] = valueGetter(value);
  }
}