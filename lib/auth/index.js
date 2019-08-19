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
/**
 * DSE Authentication module.
 * <p>
 *   Contains the classes used for connecting to a DSE cluster secured with DseAuthenticator.
 * </p>
 * @module auth
 */
const baseProvider = require('./provider.js');
exports.AuthProvider = baseProvider.AuthProvider;
exports.Authenticator = baseProvider.Authenticator;
exports.PlainTextAuthProvider = require('./plain-text-auth-provider.js');
exports.DseGssapiAuthProvider = require('./dse-gssapi-auth-provider');
exports.DsePlainTextAuthProvider = require('./dse-plain-text-auth-provider');