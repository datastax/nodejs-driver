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
import Client, { type ClientOptions, type QueryOptions } from "./lib/client";
import clientOptions from "./lib/client-options";
import concurrent from "./lib/concurrent/index";
import datastax from "./lib/datastax/index";
import Encoder from "./lib/encoder";
import errors from "./lib/errors";
import geometry from "./lib/geometry/index";
import mapping from "./lib/mapping/index";
import Metadata from "./lib/metadata/index";
import metrics from "./lib/metrics/index";
import policies from "./lib/policies/index";
import Token from "./lib/token";
import tracker from "./lib/tracker/index";
import types from "./lib/types/index";
import packageJson from './package.json';

import { ExecutionOptions } from './lib/execution-options';
import { ExecutionProfile } from './lib/execution-profile';

const token = {
  Token: Token.Token,
  TokenRange: Token.TokenRange
};
const metadata = {Metadata: Metadata};
const defaultOptions = function () {
  return clientOptions.defaultOptions();
};
const version = packageJson.version;

const cassandra = {
  Client,
  ExecutionProfile,
  ExecutionOptions,
  types: {
    ...types,
    /** @internal */
    getDataTypeNameByCode: types.getDataTypeNameByCode,
    /** @internal */
    FrameHeader: types.FrameHeader,
    /** @internal */
    generateTimestamp: types.generateTimestamp,
  },
  errors,
  policies,
  auth: {
    ...auth,
    /** @internal */
    NoAuthProvider: auth.NoAuthProvider
  },
  mapping,
  tracker,
  metrics,
  concurrent,
  token,
  metadata,
  Encoder,
  geometry,
  datastax: {
    ...datastax,
    graph: {
      ...datastax.graph,
      /** @internal */
      getCustomTypeSerializers: datastax.graph.getCustomTypeSerializers,
      /** @internal */
      GraphTypeWrapper: datastax.graph.GraphTypeWrapper,
      /** @internal */
      UdtGraphWrapper: datastax.graph.UdtGraphWrapper,
    }
  },
  /**
   * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
   */
  defaultOptions,
  version,
};

export default cassandra; 



export {
  auth, Client, concurrent, datastax,
  /**
   * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
   */
  defaultOptions, Encoder, errors, ExecutionOptions, ExecutionProfile, geometry, mapping, metadata, metrics, policies, token, tracker, types, version, type ClientOptions, type QueryOptions
};

// We need those for something like this to work: client.execute(query, (err: DriverError, result: ResultSet) => {}))
export * from './lib/auth/index';
export * from './lib/concurrent/index';
export * from './lib/datastax/index';
export * from './lib/errors';
export * from './lib/geometry/index';
export * from './lib/mapping/index';
export * from './lib/metadata/index';
export * from './lib/metrics/index';
export * from './lib/policies/index';
export * from './lib/tracker/index';
export * from './lib/types/index';

