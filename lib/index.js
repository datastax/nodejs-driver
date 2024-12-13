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
const clientOptions = require('./client-options');
exports.Client = require('./client');
exports.ExecutionProfile = require('./execution-profile').ExecutionProfile;
exports.ExecutionOptions = require('./execution-options').ExecutionOptions;
exports.types = require('./types');
exports.errors = require('./errors');
exports.policies = require('./policies');
exports.auth = require('./auth');
exports.mapping = require('./mapping');
exports.tracker = require('./tracker');
exports.metrics = require('./metrics');
exports.concurrent = require('./concurrent');

const token = require('./token');
exports.token = {
  Token: token.Token,
  TokenRange: token.TokenRange
};
const Metadata = require('./metadata');
exports.metadata = {
  Metadata: Metadata
};
exports.Encoder = require('./encoder');
exports.geometry = require('./geometry');
exports.datastax = require('./datastax');
/**
 * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
 */
exports.defaultOptions = function () {
  return clientOptions.defaultOptions();
};
exports.version = require('../package.json').version;