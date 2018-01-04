/**
 * Copyright (C) 2018 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const assert = require('assert');
const path = require('path');
const errors = require('../../lib/errors');
const helper = require('../test-helper');
const fileName = path.basename(__filename);

describe('DriverError', function () {
  it('should inherit from Error and have properties defined', function () {
    const error = new errors.DriverError('My message');
    assertError(error, errors.DriverError);
  });
});
describe('NoHostAvailableError', function () {
  it('should inherit from DriverError and have properties defined', function () {
    const error = new errors.NoHostAvailableError({});
    assert.strictEqual(error.message, 'All host(s) tried for query failed.');
    assertError(error, errors.NoHostAvailableError);
  });
});
describe('ResponseError', function () {
  it('should inherit from DriverError and have properties defined', function () {
    const error = new errors.ResponseError(1, 'My message');
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
      const error = new errorConstructor('My message');
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