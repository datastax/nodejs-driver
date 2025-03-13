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
import auth from './lib/auth/index';
import clientOptions from "./lib/client-options";
import Client from "./lib/client";
import types from "./lib/types/index";
import errors from "./lib/errors";
import policies from "./lib/policies/index";
import mapping from "./lib/mapping/index";
import tracker from "./lib/tracker/index";
import metrics from "./lib/metrics/index";
import concurrent from "./lib/concurrent/index";
import Token from "./lib/token";
import Metadata from "./lib/metadata/index";
import Encoder from "./lib/encoder";
import geometry from "./lib/geometry/index";
import datastax from "./lib/datastax/index";
import packageJson from './package.json';

import { ExecutionProfile } from './lib/execution-profile';
import { ExecutionOptions } from './lib/execution-options';

const token = {
  Token: Token.Token,
  TokenRange: Token.TokenRange
}
const metadata = {Metadata: Metadata};
const defaultOptions = function () {
  return clientOptions.defaultOptions();
}
const version = packageJson.version;


export {
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
  token,
  metadata,
  Encoder,
  geometry,
  datastax,
  /**
   * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
   */
  defaultOptions,
  version
};

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
  token,
  metadata,
  Encoder,
  geometry,
  datastax,
  /**
   * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
   */
  defaultOptions,
  version
};