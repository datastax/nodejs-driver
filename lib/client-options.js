"use strict";
var util = require('util');

var policies = require('./policies');
var types = require('./types');
var utils = require('./utils');
var errors = require('./errors');

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
      consistency: types.consistencies.localOne,
      fetchSize: 5000,
      prepare: false,
      retryOnTimeout: true,
      captureStackTrace: false
    },
    protocolOptions: {
      port: 9042,
      maxSchemaAgreementWaitSeconds: 10,
      maxVersion: 0
    },
    pooling: {
      heartBeatInterval: 30000
    },
    socketOptions: {
      connectTimeout: 5000,
      defunctReadTimeoutThreshold: 64,
      keepAlive: true,
      keepAliveDelay: 0,
      readTimeout: 12000,
      tcpNoDelay: true,
      coalescingThreshold: 8000
    },
    authProvider: null,
    maxPrepared: 500,
    refreshSchemaDelay: 1000,
    isMetadataSyncEnabled: true,
    prepareOnAllHosts: true,
    rePrepareOnUp: true,
    encoding: {
      copyBuffer: true,
      useUndefinedAsUnset: true
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
  var options = utils.deepExtend(baseOptions, defaultOptions(), userOptions);
  if (!util.isArray(options.contactPoints) || options.contactPoints.length === 0) {
    throw new TypeError('Contacts points are not defined.');
  }
  for (var i = 0; i < options.contactPoints.length; i++) {
    var hostName = options.contactPoints[i];
    if (!hostName) {
      throw new TypeError(util.format('Contact point %s (%s) is not a valid host name, ' +
        'the following values are valid contact points: ipAddress, hostName or ipAddress:port', i, hostName));
    }
  }
  if (!options.logEmitter) {
    options.logEmitter = function () {};
  }
  if (!options.queryOptions) {
    throw new TypeError('queryOptions not defined in options');
  }
  validatePoliciesOptions(options.policies);
  validateProtocolOptions(options.protocolOptions);
  validateSocketOptions(options.socketOptions);
  options.encoding = options.encoding || {};
  validateEncodingOptions(options.encoding);
  if (options.profiles && !util.isArray(options.profiles)) {
    throw new TypeError('profiles must be an Array of ExecutionProfile instances');
  }
  return options;
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
  var version = protocolOptions.maxVersion;
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
 * Validates the encoding options.
 * @param {ClientOptions.encoding} encodingOptions
 * @private
 */
function validateEncodingOptions(encodingOptions) {
  if (encodingOptions.map) {
    var mapConstructor = encodingOptions.map;
    if (typeof mapConstructor !== 'function' ||
      typeof mapConstructor.prototype.forEach !== 'function' ||
      typeof mapConstructor.prototype.set !== 'function') {
      throw new TypeError('Map constructor not valid');
    }
  }
  if (encodingOptions.set) {
    var setConstructor = encodingOptions.set;
    if (typeof setConstructor !== 'function' ||
      typeof setConstructor.prototype.forEach !== 'function' ||
      typeof setConstructor.prototype.add !== 'function') {
      throw new TypeError('Set constructor not valid');
    }
  }
}

/**
 * Creates a new instance of query options with the values from the user.
 * When some values are not defined, it takes the default values from
 * - {@link ExecutionProfile}.
 * - {@link QueryOptions} from the default options.
 * @param {Client} client
 * @param {QueryOptions|function} userOptions
 * @param {Function} [rowCallback]
 * @param {Boolean} [logged]
 * @returns {Object|Error} Returns a new instance of an object with the query options or returns an Error
 * instance (doesn't throw the Error).
 */
function createQueryOptions(client, userOptions, rowCallback, logged) {
  var profile =
    client.profileManager.getProfile(userOptions && userOptions.executionProfile);
  if (!profile) {
    return new errors.ArgumentError(util.format('Execution profile "%s" not found', userOptions.executionProfile));
  }
  // userOptions can be undefined and could be of type function (is an optional parameter)
  userOptions = (!userOptions || typeof userOptions === 'function') ? utils.emptyObject : userOptions;
  var defaultQueryOptions = client.options.queryOptions;

  // Using fixed property names is 2 order of magnitude faster than dynamically shallow clone objects
  var result = {
    autoPage: ifUndefined(userOptions.autoPage, defaultQueryOptions.autoPage),
    captureStackTrace: ifUndefined(userOptions.captureStackTrace, defaultQueryOptions.captureStackTrace),
    consistency: ifUndefined3(userOptions.consistency, profile.consistency, defaultQueryOptions.consistency),
    customPayload: ifUndefined(userOptions.customPayload, defaultQueryOptions.customPayload),
    executionProfile: profile,
    fetchSize: ifUndefined(userOptions.fetchSize, defaultQueryOptions.fetchSize),
    hints: userOptions.hints,
    isIdempotent: ifUndefined(userOptions.isIdempotent, defaultQueryOptions.isIdempotent),
    keyspace: userOptions.keyspace,
    logged: ifUndefined(userOptions.logged, logged),
    pageState: userOptions.pageState,
    prepare: ifUndefined(userOptions.prepare, defaultQueryOptions.prepare),
    readTimeout: ifUndefined3(userOptions.readTimeout, profile.readTimeout, client.options.socketOptions.readTimeout),
    retry: ifUndefined3(userOptions.retry, profile.retry, client.options.policies.retry),
    retryOnTimeout: ifUndefined3(
      userOptions.retryOnTimeout, profile.retryOnTimeout, defaultQueryOptions.retryOnTimeout),
    routingIndexes: userOptions.routingIndexes,
    routingKey: userOptions.routingKey,
    routingNames: userOptions.routingNames,
    serialConsistency: ifUndefined3(
      userOptions.serialConsistency, profile.serialConsistency, defaultQueryOptions.serialConsistency),
    timestamp: getTimestamp(client, userOptions, defaultQueryOptions.timestamp),
    traceQuery: ifUndefined(userOptions.traceQuery, defaultQueryOptions.traceQuery),
    // not part of query options
    rowCallback: rowCallback
  };
  if (userOptions === utils.emptyObject) {
    return result;
  }
  var userOptionsKeys = Object.keys(userOptions);
  var key, value;
  // Use the fastest iteration of array
  var i = userOptionsKeys.length;
  while (i--) {
    key = userOptionsKeys[i];
    if (key === 'executionProfile') {
      // Execution profile was the only value that could has been "replaced"
      continue;
    }
    value = userOptions[key];
    if (value === undefined) {
      continue;
    }
    result[key] = value;
  }
  return result;
}

function ifUndefined(v1, v2) {
  return v1 !== undefined ? v1 : v2;
}

function ifUndefined3(v1, v2, v3) {
  if (v1 !== undefined) {
    return v1;
  }
  return v2 !== undefined ? v2 : v3;
}

function getTimestamp(client, userOptions, defaultValue) {
  if (typeof userOptions.timestamp !== 'undefined') {
    return userOptions.timestamp;
  }
  var timestampGenerator = client.options.policies.timestampGeneration;
  if (types.protocolVersion.supportsTimestamp(client.controlConnection.protocolVersion) && timestampGenerator) {
    return timestampGenerator.next(client);
  }
  return defaultValue;
}

/**
 * Core connections per host for protocol versions 1 and 2
 */
var coreConnectionsPerHostV2 = {};
coreConnectionsPerHostV2[types.distance.local] = 2;
coreConnectionsPerHostV2[types.distance.remote] = 1;
coreConnectionsPerHostV2[types.distance.ignored] = 0;
/**
 * Core connections per host for protocol version 3 and above
 */
var coreConnectionsPerHostV3 = {};
coreConnectionsPerHostV3[types.distance.local] = 1;
coreConnectionsPerHostV3[types.distance.remote] = 1;
coreConnectionsPerHostV3[types.distance.ignored] = 0;

exports.extend = extend;
exports.defaultOptions = defaultOptions;
exports.coreConnectionsPerHostV2 = coreConnectionsPerHostV2;
exports.coreConnectionsPerHostV3 = coreConnectionsPerHostV3;
exports.createQueryOptions = createQueryOptions;