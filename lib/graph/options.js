/**
 * Copyright (C) 2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var types = require('../types');
var utils = require('../utils');
var clientOptions = require('../client-options');
var Long = types.Long;

var consistencyNames;

/**
 * Graph options that extends {@link QueryOptions}.
 * <p>
 *   Consider using [execution profiles]{@link ExecutionProfile} if you plan to reuse options across different
 *   query executions.
 * </p>
 * @typedef {QueryOptions} GraphQueryOptions
 * @property {String} [graphLanguage] The graph language to use in graph queries.
 * @property {String} [graphName] The graph name to be used in the query. You can use <code>null</code> to clear the
 * value from the <code>DseClientOptions</code> and execute a query without a default graph.
 * @property {Number} [graphReadConsistency] Specifies the
 * [consistency level]{h@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies}
 * to be used for the graph read queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the READ part of the graph query.
 * </p>
 * @property {String} [graphSource] The graph traversal source name to use in graph queries.
 * @property {Number} [graphWriteConsistency] Specifies the
 * [consistency level]{@link http://docs.datastax.com/en/latest-nodejs-driver-api/module-types.html#~consistencies} to
 * be used for the graph write queries in this execution.
 * <p>
 *   When defined, it overrides the consistency level only for the WRITE part of the graph query.
 * </p>
 * @property {RetryPolicy} [retry] Sets the retry policy to be used for the graph query execution.
 * <p>
 *   When not specified in the {@link GraphQueryOptions} or in the {@link ExecutionProfile}, it will use by default
 *   a retry policy that does not retry graph executions.
 * </p>
 */

/**
 * @param {ProfileManager} profileManager
 * @param baseOptions
 * @param {RetryPolicy|null} defaultRetryPolicy
 * @param {GraphQueryOptions} options
 * @return {GraphQueryOptions}
 * @ignore
 */
function createQueryOptions(profileManager, baseOptions, defaultRetryPolicy, options) {
  var profile = profileManager.getProfile(options && options.executionProfile);
  if (!profile) {
    // Let the core driver deal with specified profile not been found
    return options;
  }
  var defaultGraphOptions = getDefaultGraphOptions(profileManager, baseOptions, defaultRetryPolicy, profile);
  if (!options || typeof options === 'function') {
    return defaultGraphOptions;
  }

  // Check if the user is using a parameter that would make the custom payload different from the
  // payload for the profile (ie: the user specified only the profile / or nothing at all)
  var noGraphPayloadOptions =
    !options.customPayload &&
    !options.graphLanguage &&
    !options.graphSource &&
    !options.executeAs &&
    options.graphName === undefined &&
    options.graphReadConsistency === undefined &&
    options.graphWriteConsistency === undefined &&
    options.readTimeout === undefined;

  options = utils.extend({
    graphLanguage: defaultGraphOptions.graphLanguage,
    graphSource: defaultGraphOptions.graphSource,
    readTimeout: defaultGraphOptions.readTimeout,
    // The default retry policy for graph should not retry
    retry: defaultGraphOptions.retry
  }, options);

  // If there are no changes to custom payload, return.
  if (noGraphPayloadOptions) {
    // Reuse the same customPayload instance and avoid reconstruct it
    options.customPayload = defaultGraphOptions.customPayload;
    return options;
  }
  if (!options.customPayload) {
    options.customPayload = {};
  }
  else {
    // Shallow clone the custom payload
    options.customPayload = utils.extend({}, options.customPayload);
  }
  // Set the proxy execute payload to avoid clientOptions from cloning this payload dictionary
  setPayloadKey(options, defaultGraphOptions, clientOptions.proxyExecuteKey, options.executeAs);
  // Set the payload for DSE graph exclusive options
  setPayloadKey(options, defaultGraphOptions, 'graph-language', options.graphLanguage);
  setPayloadKey(options, defaultGraphOptions, 'graph-source', options.graphSource);
  setPayloadKey(options, defaultGraphOptions, 'graph-name', options.graphName);
  setPayloadKey(options, defaultGraphOptions, 'graph-read-consistency',
    getConsistencyName(options.graphReadConsistency));
  setPayloadKey(options, defaultGraphOptions, 'graph-write-consistency',
    getConsistencyName(options.graphWriteConsistency));
  setPayloadKey(options, defaultGraphOptions, 'request-timeout',
    options.readTimeout > 0 ? options.readTimeout : null, longBuffer);
  return options;
}

/**
 * Gets the default options with the custom payload for a given profile.
 * @param {ProfileManager} profileManager
 * @param baseOptions
 * @param {RetryPolicy|null} defaultRetryPolicy
 * @param {ExecutionProfile} profile
 * @returns {DseClientOptions}
 * @private
 */
function getDefaultGraphOptions(profileManager, baseOptions, defaultRetryPolicy, profile) {
  return profileManager.getOrCreateGraphOptions(profile, function createDefaultOptions() {
    var profileOptions = profile.graphOptions || utils.emptyObject;
    var defaultProfile = profileManager.getDefault();
    var options = {
      customPayload: {
        'graph-language': utils.allocBufferFromString(profileOptions.language || baseOptions.language),
        'graph-source': utils.allocBufferFromString(profileOptions.source || baseOptions.source)
      },
      graphLanguage: profileOptions.language || baseOptions.language,
      graphSource: profileOptions.source || baseOptions.source
    };
    if (profile !== defaultProfile) {
      options.retry = profile.retry || baseOptions.retry;
    }
    else {
      // Based on an implementation detail of the execution profiles, the retry policy for the default profile is
      // always loaded (required), but that doesn't mean that it was specified by the user.
      // If it wasn't specified by the user, use the default retry policy for graph statements.
      options.retry = defaultRetryPolicy || baseOptions.retry;
    }
    if (baseOptions.executeAs) {
      options.customPayload[clientOptions.proxyExecuteKey] = utils.allocBufferFromString(baseOptions.executeAs);
    }
    var name = utils.ifUndefined(profileOptions.name, baseOptions.name);
    if (name) {
      options.customPayload['graph-name'] = utils.allocBufferFromString(name);
    }
    var readConsistency = utils.ifUndefined(profileOptions.readConsistency, baseOptions.readConsistency);
    if (readConsistency !== undefined) {
      options.customPayload['graph-read-consistency'] =
        utils.allocBufferFromString(getConsistencyName(readConsistency));
    }
    var writeConsistency = utils.ifUndefined(profileOptions.writeConsistency, baseOptions.writeConsistency);
    if (writeConsistency !== undefined) {
      options.customPayload['graph-write-consistency'] =
        utils.allocBufferFromString(getConsistencyName(writeConsistency));
    }
    options.readTimeout = utils.ifUndefined3(profile.readTimeout, defaultProfile.readTimeout, baseOptions.readTimeout);
    if (options.readTimeout > 0) {
      // Write the graph read timeout payload
      options.customPayload['request-timeout'] = longBuffer(options.readTimeout);
    }
    return options;
  });
}

/**
 * Sets the payload key. If the value is not provided, it uses the value from the default profile options.
 * @param {QueryOptions} options
 * @param {QueryOptions} profileOptions
 * @param {String} key
 * @param {String|Number|null} value
 * @param {Function} [converter]
 * @private
 */
function setPayloadKey(options, profileOptions, key, value, converter) {
  converter = converter || utils.allocBufferFromString;
  if (value === null) {
    // Use null to avoid set payload for a key
    return;
  }
  if (value !== undefined) {
    options.customPayload[key] = converter(value);
    return;
  }
  if (profileOptions.customPayload[key]) {
    options.customPayload[key] = profileOptions.customPayload[key];
  }
}

function longBuffer(value) {
  value = Long.fromNumber(value);
  return Long.toBuffer(value);
}

/**
 * Gets the name in upper case of the consistency level.
 * @param {Number} consistency
 * @private
 */
function getConsistencyName(consistency) {
  // eslint-disable-next-line
  if (consistency == undefined) {
    //null or undefined => undefined
    return undefined;
  }
  loadConsistencyNames();
  var name = consistencyNames[consistency];
  if (!name) {
    throw new Error(util.format(
      'Consistency %s not found, use values defined as properties in types.consistencies object', consistency
    ));
  }
  return name;
}

function loadConsistencyNames() {
  if (consistencyNames) {
    return;
  }
  consistencyNames = {};
  var propertyNames = Object.keys(types.consistencies);
  for (var i = 0; i < propertyNames.length; i++) {
    var name = propertyNames[i];
    consistencyNames[types.consistencies[name]] = name.toUpperCase();
  }
  //Using java constants naming conventions
  consistencyNames[types.consistencies.localQuorum] = 'LOCAL_QUORUM';
  consistencyNames[types.consistencies.eachQuorum] = 'EACH_QUORUM';
  consistencyNames[types.consistencies.localSerial] = 'LOCAL_SERIAL';
  consistencyNames[types.consistencies.localOne] = 'LOCAL_ONE';
}

exports.createQueryOptions = createQueryOptions;