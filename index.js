/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const clientOptions = require('./lib/client-options');
exports.Client = require('./lib/dse-client');
exports.ExecutionProfile = require('./lib/execution-profile').ExecutionProfile;
exports.ExecutionOptions = require('./lib/execution-options').ExecutionOptions;
exports.types = require('./lib/types');
exports.errors = require('./lib/errors');
exports.policies = require('./lib/policies');
exports.auth = require('./lib/auth');
exports.mapping = require('./lib/mapping');
exports.tracker = require('./lib/tracker');
exports.metrics = require('./lib/metrics');
exports.concurrent = require('./lib/concurrent');

const token = require('./lib/token');
exports.token = {
  Token: token.Token,
  TokenRange: token.TokenRange
};
const Metadata = require('./lib/metadata');
exports.metadata = {
  Metadata: Metadata
};
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