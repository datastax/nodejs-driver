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
const ExecutionProfile = require('./lib/execution-profile').ExecutionProfile;
const ExecutionOptions = require('./lib/execution-options').ExecutionOptions;
import * as auth from './lib/auth/index.js';
import clientOptions from "./lib/client-options.js";
import Client from "./lib/client.js";
import types from "./lib/types/index.js";
import errors from "./lib/errors.js";
import policies from "./lib/policies/index.js";
import mapping from "./lib/mapping/index.js";
import tracker from "./lib/tracker/index.js";
import metrics from "./lib/metrics/index.js";
import concurrent from "./lib/concurrent/index.js";
import token from "./lib/token.js";
import Metadata from "./lib/metadata/index.js";
import Encoder from "./lib/encoder.js";
import geometry from "./lib/geometry/index.js";
import datastax from "./lib/datastax/index.js";
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