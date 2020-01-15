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

const assert = require('assert');

const clientOptions = require('../../lib/client-options');
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const ProfileManager = require('../../lib/execution-profile').ProfileManager;
const types = require('../../lib/types');

describe('ProfileManager', function () {
  describe('constructor', function () {
    it('should set the default profile based on the client options', function () {
      const options = clientOptions.defaultOptions();
      const manager = new ProfileManager(options);
      const profile = manager.getDefault();
      assert.ok(profile);
      assert.strictEqual(profile.loadBalancing, options.policies.loadBalancing);
      assert.strictEqual(profile.retry, options.policies.retry);
    });
    it('should set the default profile required options', function () {
      const options = clientOptions.defaultOptions();
      options.profiles = [
        new ExecutionProfile('default')
      ];
      const manager = new ProfileManager(options);
      const profile = manager.getDefault();
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles[0]);
      assert.strictEqual(profile.loadBalancing, options.policies.loadBalancing);
      assert.strictEqual(profile.retry, options.policies.retry);
    });
  });
  describe('#getProfile()', function () {
    it('should get the profile by name', function () {
      const options = clientOptions.defaultOptions();
      options.profiles = [
        new ExecutionProfile('metrics', { consistency: types.consistencies.localQuorum })
      ];
      const manager = new ProfileManager(options);
      const profile = manager.getProfile('metrics');
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles[0]);
      assert.strictEqual(profile.consistency, types.consistencies.localQuorum);
      assert.ok(manager.getDefault());
      assert.notStrictEqual(manager.getDefault(), profile);
      assert.strictEqual(manager.getProfile('metrics'), profile);
    });
    it('should get the default profile when name is undefined', function () {
      const options = clientOptions.defaultOptions();
      const manager = new ProfileManager(options);
      const profile = manager.getProfile(undefined);
      assert.ok(profile);
      assert.strictEqual(manager.getDefault(), profile);
    });
    it('should return same the execution profile if provided', function () {
      const options = clientOptions.defaultOptions();
      const manager = new ProfileManager(options);
      const metricsProfile = new ExecutionProfile('metrics');
      options.profiles = [ metricsProfile ];
      const profile = manager.getProfile(metricsProfile);
      assert.ok(profile);
      assert.strictEqual(profile, metricsProfile);
    });
  });
});