/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var clientOptions = require('./lib/client-options');
exports.Client = require('./lib/dse-client');
exports.ExecutionProfile = require('./lib/execution-profile').ExecutionProfile;
exports.types = require('./lib/types');
exports.errors = require('./lib/errors');
exports.policies = require('./lib/policies');
exports.auth = require('./lib/auth');
var Metadata = require('./lib/metadata');
exports.metadata = { Metadata: Metadata };
exports.Encoder = require('./lib/encoder');
exports.geometry = require('./lib/geometry');
exports.graph = require('./lib/graph');
/**
 * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
 */
exports.defaultOptions = function () {
  return clientOptions.defaultOptions();
};
exports.version = require('./package.json').version;