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
const Client = require('./lib/client');
const ExecutionProfile = require('./lib/execution-profile').ExecutionProfile;
const ExecutionOptions = require('./lib/execution-options').ExecutionOptions;
const types = require('./lib/types');
const errors = require('./lib/errors');
const policies = require('./lib/policies');
import * as auth from './lib/auth';
const mapping = require('./lib/mapping');
const tracker = require('./lib/tracker');
const metrics = require('./lib/metrics');
const concurrent = require('./lib/concurrent');
const token = require('./lib/token');
const Metadata = require('./lib/metadata');
const Encoder = require('./lib/encoder');
const geometry = require('./lib/geometry');
const datastax = require('./lib/datastax');

export default {
  Client,
  ExecutionProfile,
  ExecutionOptions,
  types,
  errors,
  policies,
  auth,
  mapping,
  tracker,
  metrics,
  concurrent,
  token: {
    Token: token.Token,
    TokenRange: token.TokenRange
  },
  metadata: {
    Metadata: Metadata
  },
  Encoder,
  geometry,
  datastax,
  /**
   * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
   */
  defaultOptions: function () {
    return clientOptions.defaultOptions();
  },
  version: require('./package.json').version
};