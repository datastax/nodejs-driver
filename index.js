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
const clientOptions = require('./lib/client-options');
exports.Client = require('./lib/client');
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
exports.datastax = require('./lib/datastax');
/**
 * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
 */
exports.defaultOptions = function () {
  return clientOptions.defaultOptions();
};
exports.version = require('./package.json').version;