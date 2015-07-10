"use strict";
var util = require('util');

var loadBalancing = require('./policies/load-balancing');
var reconnection = require('./policies/reconnection');
var retry = require('./policies/retry');
var addressResolution = require('./policies/address-resolution');
var types = require('./types');
var utils = require('./utils');

/**
 * @returns {ClientOptions}
 */
function defaultOptions () {
  return ({
    policies: {
      loadBalancing: new loadBalancing.TokenAwarePolicy(new loadBalancing.DCAwareRoundRobinPolicy()),
      reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
      retry: new retry.RetryPolicy(),
      addressResolution: new addressResolution.AddressTranslator()
    },
    queryOptions: {
      consistency: types.consistencies.one,
      fetchSize: 5000,
      prepare: false,
      retryOnTimeout: true
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
      tcpNoDelay: true
    },
    authProvider: null,
    maxPrepared: 500,
    encoding: {
      copyBuffer: true
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
  if (!options.policies) {
    throw new TypeError('policies not defined in options');
  }
  if (!(options.policies.loadBalancing instanceof loadBalancing.LoadBalancingPolicy)) {
    throw new TypeError('Load balancing policy must be an instance of LoadBalancingPolicy');
  }
  if (!(options.policies.reconnection instanceof reconnection.ReconnectionPolicy)) {
    throw new TypeError('Reconnection policy must be an instance of ReconnectionPolicy');
  }
  if (!(options.policies.retry instanceof retry.RetryPolicy)) {
    throw new TypeError('Retry policy must be an instance of RetryPolicy');
  }
  if (!(options.policies.addressResolution instanceof addressResolution.AddressTranslator)) {
    throw new TypeError('Address resolution policy must be an instance of AddressTranslator');
  }
  if (!options.queryOptions) {
    throw new TypeError('queryOptions not defined in options');
  }
  if (!options.protocolOptions) {
    throw new TypeError('protocolOptions not defined in options');
  }
  if (!options.socketOptions) {
    throw new TypeError('socketOptions not defined in options');
  }
  if (typeof options.socketOptions.readTimeout !== 'number') {
    throw new TypeError('socketOptions.readTimeout must be a Number');
  }
  if (!options.logEmitter) {
    options.logEmitter = function () {};
  }
  if (!options.encoding) {
    options.encoding = {};
  }
  if (options.encoding.map) {
    var mapConstructor = options.encoding.map;
    if (typeof mapConstructor !== 'function' ||
      typeof mapConstructor.prototype.forEach !== 'function' ||
      typeof mapConstructor.prototype.set !== 'function') {
      throw new TypeError('Map constructor not valid');
    }
  }
  if (options.encoding.set) {
    var setConstructor = options.encoding.set;
    if (typeof setConstructor !== 'function' ||
      typeof setConstructor.prototype.forEach !== 'function' ||
      typeof setConstructor.prototype.add !== 'function') {
      throw new TypeError('Set constructor not valid');
    }
  }
  return options;
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
