"use strict";

var assert = require('assert');

var clientOptions = require('../../lib/client-options');
var ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
var ProfileManager = require('../../lib/execution-profile').ProfileManager;
var types = require('../../lib/types');

describe('ProfileManager', function () {
  describe('constructor', function () {
    it('should set the default profile based on the client options', function () {
      var options = clientOptions.defaultOptions();
      var manager = new ProfileManager(options);
      var profile = manager.getDefault();
      assert.ok(profile);
      assert.strictEqual(profile.loadBalancing, options.policies.loadBalancing);
      assert.strictEqual(profile.retry, options.policies.retry);
    });
    it('should set the default profile required options', function () {
      var options = clientOptions.defaultOptions();
      options.profiles = [
        new ExecutionProfile('default')
      ];
      var manager = new ProfileManager(options);
      var profile = manager.getDefault();
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles[0]);
      assert.strictEqual(profile.loadBalancing, options.policies.loadBalancing);
      assert.strictEqual(profile.retry, options.policies.retry);
    });
  });
  describe('#getProfile()', function () {
    it('should get the profile by name', function () {
      var options = clientOptions.defaultOptions();
      options.profiles = [
        new ExecutionProfile('metrics', { consistency: types.consistencies.localQuorum })
      ];
      var manager = new ProfileManager(options);
      var profile = manager.getProfile('metrics');
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles[0]);
      assert.strictEqual(profile.consistency, types.consistencies.localQuorum);
      assert.ok(manager.getDefault());
      assert.notStrictEqual(manager.getDefault(), profile);
      assert.strictEqual(manager.getProfile('metrics'), profile);
    });
    it('should get the default profile when name is undefined', function () {
      var options = clientOptions.defaultOptions();
      var manager = new ProfileManager(options);
      var profile = manager.getProfile(undefined);
      assert.ok(profile);
      assert.strictEqual(manager.getDefault(), profile);
    });
    it('should return same the execution profile if provided', function () {
      var options = clientOptions.defaultOptions();
      var manager = new ProfileManager(options);
      var metricsProfile = new ExecutionProfile('metrics');
      options.profiles = [ metricsProfile ];
      var profile = manager.getProfile(metricsProfile);
      assert.ok(profile);
      assert.strictEqual(profile, metricsProfile);
    });
  });
});