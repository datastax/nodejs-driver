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


/**
 * DSE Authentication module.
 * <p>
 *   Contains the classes used for connecting to a DSE cluster secured with DseAuthenticator.
 * </p>
 * @module auth
 */

import DseGssapiAuthProvider from './dse-gssapi-auth-provider';
import DsePlainTextAuthProvider from './dse-plain-text-auth-provider';
import NoAuthProvider from './no-auth-provider';
import { PlainTextAuthProvider } from './plain-text-auth-provider';
import { Authenticator, AuthProvider } from './provider';

export {
  Authenticator,
  AuthProvider,
  DseGssapiAuthProvider,
  DsePlainTextAuthProvider,
  /** @internal */
  NoAuthProvider,
  PlainTextAuthProvider
};

export default {
  Authenticator,
  AuthProvider,
  DseGssapiAuthProvider,
  DsePlainTextAuthProvider,
  /** @internal */
  NoAuthProvider,
  PlainTextAuthProvider
};