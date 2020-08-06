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

const util = require('util');
const policies = require('./policies');
const types = require('./types');
const utils = require('./utils');
const tracker = require('./tracker');
const metrics = require('./metrics');
const auth = require('./auth');

/** Core connections per host for protocol versions 1 and 2 */
const coreConnectionsPerHostV2 = {
  [types.distance.local]: 2,
  [types.distance.remote]: 1,
  [types.distance.ignored]: 0
};

/** Core connections per host for protocol version 3 and above */
const coreConnectionsPerHostV3 = {
  [types.distance.local]: 1,
  [types.distance.remote]: 1,
  [types.distance.ignored]: 0
};

/** Default maxRequestsPerConnection value for protocol v1 and v2 */
const maxRequestsPerConnectionV2 = 128;

/** Default maxRequestsPerConnection value for protocol v3+ */
const maxRequestsPerConnectionV3 = 2048;

const continuousPageUnitBytes = 'bytes';
const continuousPageDefaultSize = 5000;
const continuousPageDefaultHighWaterMark = 10000;

/**
 * @returns {ClientOptions}
 */
function defaultOptions () {
  return ({
    policies: {
      addressResolution: policies.defaultAddressTranslator(),
      loadBalancing: policies.defaultLoadBalancingPolicy(),
      reconnection: policies.defaultReconnectionPolicy(),
      retry: policies.defaultRetryPolicy(),
      speculativeExecution: policies.defaultSpeculativeExecutionPolicy(),
      timestampGeneration: policies.defaultTimestampGenerator()
    },
    queryOptions: {
      fetchSize: 5000,
      prepare: false,
      captureStackTrace: false
    },
    protocolOptions: {
      port: 9042,
      maxSchemaAgreementWaitSeconds: 10,
      maxVersion: 0,
      noCompact: false
    },
    pooling: {
      heartBeatInterval: 30000,
      warmup: true
    },
    socketOptions: {
      connectTimeout: 5000,
      defunctReadTimeoutThreshold: 64,
      keepAlive: true,
      keepAliveDelay: 0,
      readTimeout: 12000,
      tcpNoDelay: true,
      coalescingThreshold: 65536
    },
    authProvider: null,
    requestTracker: null,
    metrics: new metrics.DefaultMetrics(),
    maxPrepared: 500,
    refreshSchemaDelay: 1000,
    isMetadataSyncEnabled: true,
    prepareOnAllHosts: true,
    rePrepareOnUp: true,
    encoding: {
      copyBuffer: true,
      useUndefinedAsUnset: true
    },
    monitorReporting: {
      enabled: true
    }
  });
}

/**
 * Extends and validates the user options
 * @param {Object} [baseOptions] The source object instance that will be overridden
 * @param {Object} userOptions
 * @returns {Object}
 */
function extend(baseOptions, userOptions) {
  if (arguments.length === 1) {
    userOptions = arguments[0];
    baseOptions = {};
  }
  const options = utils.deepExtend(baseOptions, defaultOptions(), userOptions);

  if (!options.cloud) {
    if (!Array.isArray(options.contactPoints) || options.contactPoints.length === 0) {
      throw new TypeError('Contacts points are not defined.');
    }

    for (let i = 0; i < options.contactPoints.length; i++) {
      const hostName = options.contactPoints[i];
      if (!hostName) {
        throw new TypeError(util.format('Contact point %s (%s) is not a valid host name, ' +
          'the following values are valid contact points: ipAddress, hostName or ipAddress:port', i, hostName));
      }
    }

    options.sni = undefined;
  } else {
    validateCloudOptions(options);
  }

  if (!options.logEmitter) {
    options.logEmitter = function () {};
  }
  if (!options.queryOptions) {
    throw new TypeError('queryOptions not defined in options');
  }

  if (options.requestTracker !== null && !(options.requestTracker instanceof tracker.RequestTracker)) {
    throw new TypeError('requestTracker must be an instance of RequestTracker');
  }

  if (!(options.metrics instanceof metrics.ClientMetrics)) {
    throw new TypeError('metrics must be an instance of ClientMetrics');
  }

  validatePoliciesOptions(options.policies);

  validateProtocolOptions(options.protocolOptions);

  validateSocketOptions(options.socketOptions);

  validateAuthenticationOptions(options);

  options.encoding = options.encoding || {};

  validateEncodingOptions(options.encoding);

  if (options.profiles && !Array.isArray(options.profiles)) {
    throw new TypeError('profiles must be an Array of ExecutionProfile instances');
  }

  validateApplicationInfo(options);

  validateMonitorReporting(options);

  return options;
}

/**
 * Validates the options to connect to a cloud instance.
 * @private
 */
function validateCloudOptions(options) {
  const bundle = options.cloud.secureConnectBundle;

  // eslint-disable-next-line no-undef
  if (!(typeof bundle === 'string' || (typeof URL !== 'undefined' && bundle instanceof URL))) {
    throw new TypeError('secureConnectBundle in cloud options must be of type string');
  }

  if (options.contactPoints) {
    throw new TypeError('Contact points can not be defined when cloud settings are provided');
  }

  if (options.sslOptions) {
    throw new TypeError('SSL options can not be defined when cloud settings are provided');
  }
}

/**
 * Validates the policies from the client options.
 * @param {ClientOptions.policies} policiesOptions
 * @private
 */
function validatePoliciesOptions(policiesOptions) {
  if (!policiesOptions) {
    throw new TypeError('policies not defined in options');
  }
  if (!(policiesOptions.loadBalancing instanceof policies.loadBalancing.LoadBalancingPolicy)) {
    throw new TypeError('Load balancing policy must be an instance of LoadBalancingPolicy');
  }
  if (!(policiesOptions.reconnection instanceof policies.reconnection.ReconnectionPolicy)) {
    throw new TypeError('Reconnection policy must be an instance of ReconnectionPolicy');
  }
  if (!(policiesOptions.retry instanceof policies.retry.RetryPolicy)) {
    throw new TypeError('Retry policy must be an instance of RetryPolicy');
  }
  if (!(policiesOptions.addressResolution instanceof policies.addressResolution.AddressTranslator)) {
    throw new TypeError('Address resolution policy must be an instance of AddressTranslator');
  }
  if (policiesOptions.timestampGeneration !== null &&
    !(policiesOptions.timestampGeneration instanceof policies.timestampGeneration.TimestampGenerator)) {
    throw new TypeError('Timestamp generation policy must be an instance of TimestampGenerator');
  }
}

/**
 * Validates the protocol options.
 * @param {ClientOptions.protocolOptions} protocolOptions
 * @private
 */
function validateProtocolOptions(protocolOptions) {
  if (!protocolOptions) {
    throw new TypeError('protocolOptions not defined in options');
  }
  const version = protocolOptions.maxVersion;
  if (version && (typeof version !== 'number' || !types.protocolVersion.isSupported(version))) {
    throw new TypeError(util.format('protocolOptions.maxVersion provided (%s) is invalid', version));
  }
}

/**
 * Validates the socket options.
 * @param {ClientOptions.socketOptions} socketOptions
 * @private
 */
function validateSocketOptions(socketOptions) {
  if (!socketOptions) {
    throw new TypeError('socketOptions not defined in options');
  }
  if (typeof socketOptions.readTimeout !== 'number') {
    throw new TypeError('socketOptions.readTimeout must be a Number');
  }
  if (typeof socketOptions.coalescingThreshold !== 'number' || socketOptions.coalescingThreshold <= 0) {
    throw new TypeError('socketOptions.coalescingThreshold must be a positive Number');
  }
}

/**
 * Validates authentication provider and credentials.
 * @param {ClientOptions} options
 * @private
 */
function validateAuthenticationOptions(options) {
  if (!options.authProvider) {
    const credentials = options.credentials;
    if (credentials) {
      if (typeof credentials.username !== 'string' || typeof credentials.password !== 'string') {
        throw new TypeError('credentials username and password must be a string');
      }

      options.authProvider = new auth.PlainTextAuthProvider(credentials.username, credentials.password);
    } else {
      options.authProvider = new auth.NoAuthProvider();
    }
  } else if (!(options.authProvider instanceof auth.AuthProvider)) {
    throw new TypeError('options.authProvider must be an instance of AuthProvider');
  }
}

/**
 * Validates the encoding options.
 * @param {ClientOptions.encoding} encodingOptions
 * @private
 */
function validateEncodingOptions(encodingOptions) {
  if (encodingOptions.map) {
    const mapConstructor = encodingOptions.map;
    if (typeof mapConstructor !== 'function' ||
      typeof mapConstructor.prototype.forEach !== 'function' ||
      typeof mapConstructor.prototype.set !== 'function') {
      throw new TypeError('Map constructor not valid');
    }
  }

  if (encodingOptions.set) {
    const setConstructor = encodingOptions.set;
    if (typeof setConstructor !== 'function' ||
      typeof setConstructor.prototype.forEach !== 'function' ||
      typeof setConstructor.prototype.add !== 'function') {
      throw new TypeError('Set constructor not valid');
    }
  }

  if ((encodingOptions.useBigIntAsLong || encodingOptions.useBigIntAsVarint) && typeof BigInt === 'undefined') {
    throw new TypeError('BigInt is not supported by the JavaScript engine');
  }
}

function validateApplicationInfo(options) {
  function validateString(key) {
    const str = options[key];

    if (str !== null && str !== undefined && typeof str !== 'string') {
      throw new TypeError(`${key} should be a String`);
    }
  }

  validateString('applicationName');
  validateString('applicationVersion');

  if (options.id !== null && options.id !== undefined && !(options.id instanceof types.Uuid)) {
    throw new TypeError('Client id must be a Uuid');
  }
}

function validateMonitorReporting(options) {
  const o = options.monitorReporting;
  if (o === null || typeof o !== 'object') {
    throw new TypeError(`Monitor reporting must be an object, obtained: ${o}`);
  }
}

/**
 * Sets the default options that depend on the protocol version and other metadata.
 * @param {Client} client
 */
function setMetadataDependent(client) {
  const version = client.controlConnection.protocolVersion;
  let coreConnectionsPerHost = coreConnectionsPerHostV3;
  let maxRequestsPerConnection = maxRequestsPerConnectionV3;

  if (!types.protocolVersion.uses2BytesStreamIds(version)) {
    coreConnectionsPerHost = coreConnectionsPerHostV2;
    maxRequestsPerConnection = maxRequestsPerConnectionV2;
  }

  if (client.options.queryOptions.consistency === undefined) {
    client.options.queryOptions.consistency =
      client.metadata.isDbaas() ? types.consistencies.localQuorum : types.consistencies.localOne;
  }

  client.options.pooling = utils.deepExtend(
    {}, { coreConnectionsPerHost, maxRequestsPerConnection }, client.options.pooling);
}

exports.extend = extend;
exports.defaultOptions = defaultOptions;
exports.coreConnectionsPerHostV2 = coreConnectionsPerHostV2;
exports.coreConnectionsPerHostV3 = coreConnectionsPerHostV3;
exports.maxRequestsPerConnectionV2 = maxRequestsPerConnectionV2;
exports.maxRequestsPerConnectionV3 = maxRequestsPerConnectionV3;
exports.setMetadataDependent = setMetadataDependent;
exports.continuousPageUnitBytes = continuousPageUnitBytes;
exports.continuousPageDefaultSize = continuousPageDefaultSize;
exports.continuousPageDefaultHighWaterMark = continuousPageDefaultHighWaterMark;
