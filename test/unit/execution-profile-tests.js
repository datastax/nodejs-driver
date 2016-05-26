"use strict";

var assert = require('assert');

var clientOptions = require('../../lib/client-options');
var ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
var helper = require('../test-helper');
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
      options.profiles = {
        'default': new ExecutionProfile()
      };
      var manager = new ProfileManager(options);
      var profile = manager.getDefault();
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles['default']);
      assert.strictEqual(profile.loadBalancing, options.policies.loadBalancing);
      assert.strictEqual(profile.retry, options.policies.retry);
    });
  });
  describe('#getProfile()', function () {
    it('should get the profile by name', function () {
      var options = clientOptions.defaultOptions();
      options.profiles = {
        'metrics': new ExecutionProfile({ consistency: types.consistencies.localQuorum })
      };
      var manager = new ProfileManager(options);
      var profile = manager.getProfile('metrics');
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles['metrics']);
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
      options.profiles = {
        'metrics': new ExecutionProfile()
      };
      var profile = manager.getProfile(options.profiles['metrics']);
      assert.ok(profile);
      assert.strictEqual(profile, options.profiles['metrics']);
    });
  });
});