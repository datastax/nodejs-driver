var clientOptions = require('./lib/client-options');
exports.Client = require('./lib/client');
exports.ExecutionProfile = require('./lib/execution-profile').ExecutionProfile;
exports.types = require('./lib/types');
exports.errors = require('./lib/errors');
exports.policies = require('./lib/policies');
exports.auth = require('./lib/auth');
exports.metadata = {
  Metadata: require('./lib/metadata')
};
exports.Encoder = require('./lib/encoder');
/**
 * Returns a new instance of the default [options]{@link ClientOptions} used by the driver.
 */
exports.defaultOptions = function () {
  return clientOptions.defaultOptions();
};
exports.version = require('./package.json').version;