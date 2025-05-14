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
import { assert } from "chai";
import api from "../../index";
import auth from "../../lib/auth/index";
import helper from "../test-helper";


describe('API', function () {
  it('should expose auth module', function () {
    assert.ok(api.auth);
    assert.strictEqual(typeof api.auth.DsePlainTextAuthProvider, 'function');
    assert.ok(new api.auth.DsePlainTextAuthProvider('u', 'pass') instanceof auth.AuthProvider);
    if (helper.requireOptional('kerberos')) {
      assert.ok(new api.auth.DseGssapiAuthProvider() instanceof auth.AuthProvider);
    }
  });

  it('should expose geometry module', function () {
    assert.ok(api.geometry);
    checkConstructor(api.geometry, 'LineString');
    checkConstructor(api.geometry, 'Point');
    checkConstructor(api.geometry, 'Polygon');
  });

  it('should expose Client constructor', function () {
    checkConstructor(api, 'Client');
  });

  it('should expose GraphResultSet constructor', function () {
    checkConstructor(api.datastax.graph, 'GraphResultSet');
  });

  it('should expose graph types constructor', function () {
    checkConstructor(api.datastax.graph, 'Edge');
    checkConstructor(api.datastax.graph, 'Element');
    checkConstructor(api.datastax.graph, 'Path');
    checkConstructor(api.datastax.graph, 'Property');
    checkConstructor(api.datastax.graph, 'Vertex');
    checkConstructor(api.datastax.graph, 'VertexProperty');
  });

  it('should expose graph wrappers', () => {
    [
      'asUdt',
      'asInt',
      'asFloat',
      'asDouble',
      'asTimestamp'
    ].forEach(functionName => {
      assert.isFunction(api.datastax.graph[functionName]);
    });

    checkConstructor(api.datastax.graph, 'UdtGraphWrapper');
    checkConstructor(api.datastax.graph, 'GraphTypeWrapper');
  });

  it('should expose the custom type serializers', () => {
    assert.isObject(api.datastax.graph.getCustomTypeSerializers());
    assert.isObject(api.datastax.graph.getCustomTypeSerializers()['dse:UDT']);
    assert.isObject(api.datastax.graph.getCustomTypeSerializers()['dse:Tuple']);
  });

  it('should expose graph tokens', () => {
    [
      'id',
      'key',
      'label',
      'value'
    ].forEach(name => {
      assert.isObject(api.datastax.graph.t[name]);
      assert.equal(api.datastax.graph.t[name].toString(), name);
    });

    [
      'in',
      'out',
      'both'
    ].forEach(name => {
      assert.isObject(api.datastax.graph.direction[name]);
      assert.equal(api.datastax.graph.direction[name].toString().toLowerCase(), name);
    });

    assert.equal(api.datastax.graph.direction['in_'].toString(), 'IN');

    checkConstructor(api.datastax.graph, 'UdtGraphWrapper');
    checkConstructor(api.datastax.graph, 'GraphTypeWrapper');
  });

  it('should expose cassandra driver modules', function () {
    assert.ok(api.errors);
    assert.ok(api.policies);
    assert.ok(api.policies.loadBalancing);
    checkConstructor(api.policies.loadBalancing, 'AllowListPolicy');
    // For backward compatibility only
    checkConstructor(api.policies.loadBalancing, 'WhiteListPolicy');
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