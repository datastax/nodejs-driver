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

import { Authenticator } from '../../lib/auth/provider';
import DsePlainTextAuthProvider from '../../lib/auth/dse-plain-text-auth-provider';
import assert from "assert";

describe('DsePlainTextAuthProvider', function () {
  describe('#newAuthenticator()', function () {
    it('should return an Authenticator instance', function () {
      const authProvider = new DsePlainTextAuthProvider('u', 'p');
      const authenticator = authProvider.newAuthenticator('a:1', 'PassAuth');
      assert.ok(authenticator);
      assert.ok(authenticator instanceof Authenticator);
    });
  });
});