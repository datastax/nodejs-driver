'use strict';

var assert = require('assert');
var path = require('path');
var errors = require('../../lib/errors');
var helper = require('../test-helper');
var fileName = path.basename(__filename);

describe('DriverError', function () {
  it('should inherit from Error and have properties defined', function () {
    var error = new errors.DriverError('My message');
    assertError(error, errors.DriverError);
  });
});
describe('NoHostAvailableError', function () {
  it('should inherit from DriverError and have properties defined', function () {
    var error = new errors.NoHostAvailableError({});
    assert.strictEqual(error.message, 'All host(s) tried for query failed.');
    assertError(error, errors.NoHostAvailableError);
  });
});
describe('ResponseError', function () {
  it('should inherit from DriverError and have properties defined', function () {
    var error = new errors.ResponseError(1, 'My message');
    assertError(error, errors.ResponseError);
    assert.strictEqual(error.message, 'My message');
  });
});

[
  errors.ArgumentError,
  errors.AuthenticationError,
  errors.DriverInternalError,
  errors.NotSupportedError,
  errors.OperationTimedOutError
].forEach(function (errorConstructor) {
  describe(errorConstructor.name, function () {
    it('should inherit from DriverError and have properties defined', function () {
      var error = new errorConstructor('My message');
      assertError(error, errorConstructor);
      assert.strictEqual(error.message, 'My message');
    });
  });
});

function assertError(err, errorConstructor) {
  assert.strictEqual(err.name, errorConstructor.name);
  assert.ok(err.message);
  assert.ok(err.info);
  assert.ok(err.stack);
  helper.assertInstanceOf(err, errors.DriverError);
  assert.strictEqual(err.stack.indexOf('new ' + errorConstructor.name), -1,
    'It should exclude the error instance creation from stack');
  helper.assertContains(err.stack, fileName);
}