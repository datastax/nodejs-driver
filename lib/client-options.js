var util = require('util');

var loadBalancing = require('./policies/load-balancing.js');
var reconnection = require('./policies/reconnection.js');
var retry = require('./policies/retry.js');
var types = require('./types');
var utils = require('./utils.js');

/**
 * @returns {ClientOptions}
 */
function defaultOptions () {
  return ({
    policies: {
      loadBalancing: new loadBalancing.TokenAwarePolicy(new loadBalancing.DCAwareRoundRobinPolicy()),
      reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
      retry: new retry.RetryPolicy()
    },
    queryOptions: {
      consistency: types.consistencies.one,
      fetchSize: 5000,
      prepare: false
    },
    protocolOptions: {
      port: 9042,
      maxSchemaAgreementWaitSeconds: 10
    },
    pooling: {
      heartBeatInterval: 30000,
      coreConnectionsPerHost: {
        '0': 2,
        '1': 1,
        '2': 0
      },
      maxConnectionsPerHost: {}
    },
    socketOptions: {
      connectTimeout: 5000,
      keepAlive: true,
      keepAliveDelay: 0
    },
    authProvider: null,
    maxPrepared: 500,
    encoding: {}
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
    if (!hostName || hostName.indexOf(':') > 0) {
      throw new TypeError(util.format('Contact point %s (%s) is not a valid host name, ' +
        'use ip address or host name without specifying the port number', i, hostName));
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
  if (!options.queryOptions) {
    throw new TypeError('queryOptions not defined in options');
  }
  if (!options.protocolOptions) {
    throw new TypeError('protocolOptions');
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

exports.extend = extend;
exports.defaultOptions = defaultOptions;
