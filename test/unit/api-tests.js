/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require('assert');
var api = require('../../index.js');
var cassandra = require('cassandra-driver');

describe('API', function () {
  it('should expose auth module', function () {
    assert.ok(api.auth);
    assert.strictEqual(typeof api.auth.DsePlainTextAuthProvider, 'function');
    assert.ok(new api.auth.DsePlainTextAuthProvider('u', 'pass') instanceof cassandra.auth.AuthProvider);
    assert.ok(new api.auth.DseGssapiAuthProvider() instanceof cassandra.auth.AuthProvider);
  });
  it('should expose geometry module', function () {
    assert.ok(api.geometry);
    checkConstructor(api.geometry, 'LineString');
    checkConstructor(api.geometry, 'Point');
    checkConstructor(api.geometry, 'Polygon');
  });
  it('should expose DseClient constructor', function () {
    checkConstructor(api, 'DseClient');
  });
  it('should expose GraphResultSet constructor', function () {
    checkConstructor(api.graph, 'GraphResultSet');
  });
  it('should expose cassandra driver modules', function () {
    assert.ok(api.errors);
    assert.ok(api.policies);
    assert.ok(api.policies.loadBalancing);
    assert.ok(api.policies.retry);
    assert.ok(api.policies.reconnection);
    assert.ok(api.metadata);
    assert.ok(api.types);
    checkConstructor(api.types, 'BigDecimal');
    checkConstructor(api.types, 'Integer');
    checkConstructor(api.types, 'InetAddress');
    checkConstructor(api.types, 'Uuid');
    checkConstructor(api.types, 'TimeUuid');
  });
});

function checkConstructor(module, constructorName) {
  assert.strictEqual(typeof module[constructorName], 'function');
  // use Function.name
  assert.strictEqual(module[constructorName].name, constructorName);
}