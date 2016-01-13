var assert = require('assert');
var api = require('../../index.js');
var cassandra = require('cassandra-driver');

describe('API', function () {
  it('should expose auth.DsePlainTextAuthProvider', function () {
    assert.ok(api.auth);
    assert.strictEqual(typeof api.auth.DsePlainTextAuthProvider, 'function');
    assert.ok(new api.auth.DsePlainTextAuthProvider('u', 'pass') instanceof cassandra.auth.AuthProvider);
  });
});