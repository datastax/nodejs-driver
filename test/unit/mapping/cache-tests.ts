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
import assert from "assert";
import Cache from "../../../lib/mapping/cache";
import {q} from "../../../lib/mapping/q";

describe('Cache', function() {
  this.timeout(5000);
  describe('getSelectKey()', () => {

    // Use arrays instead of iterators to compare
    const getKey = (docKeys, doc, docInfo) => Array.from(Cache.getSelectKey(docKeys, doc, docInfo));

    it('should consider docKeys', () => {
      const docKeys1 = ['abc', 'def'];
      const docKeys2 = ['abc', 'd', 'e', 'f'];
      const docKeys3 = ['abc'];

      const key1 = getKey(docKeys1, {});
      const key2 = getKey(docKeys2, {});
      const key3 = getKey(docKeys3, {});

      assert.notDeepEqual(key1, key2);
      assert.notDeepEqual(key1, key3);
      assert.notDeepEqual(key2, key3);

      // Same values => same key
      assert.deepEqual(key1, getKey(docKeys1.slice(0), {}));
      assert.deepEqual(key2, getKey(docKeys2.slice(0), {}));
      assert.deepEqual(key3, getKey(docKeys3.slice(0), {}));
    });

    it('should consider limit, orderBy, fields', () => {
      const docKeys1 = ['abc', 'def'];

      // Any defined limit
      const key1WithLimitA = getKey(docKeys1, {}, { limit: 123 });
      const key1WithLimitB = getKey(docKeys1, {}, { limit: 100 });
      const key2WithLimit = getKey(['abc', 'd', 'e', 'f'], {}, { limit: 100 });

      assert.deepEqual(key1WithLimitA, key1WithLimitB);
      assert.notDeepEqual(key1WithLimitA, key2WithLimit);

      // orderBy
      const key1WithOrderByA = getKey(docKeys1, {}, { orderBy: {'abc': 'asc'} });
      const key1WithOrderByB = getKey(docKeys1, {}, { orderBy: {'def': 'desc'} });
      const key1WithOrderByC = getKey(docKeys1, {}, { orderBy: {'abc': 'asc'} });
      const key1WithOrderByD = getKey(docKeys1, {}, { orderBy: {'abc': 'asc', 'def': 'desc'} });

      assert.notDeepEqual(key1WithOrderByA, key1WithOrderByB);
      assert.notDeepEqual(key1WithOrderByA, key1WithOrderByD);
      assert.deepEqual(key1WithOrderByA, key1WithOrderByC);

      // fields
      const key1WithFieldsA = getKey(docKeys1, {}, { fields: ['abc'] });
      const key1WithFieldsB = getKey(docKeys1, {}, { fields: ['def'] });
      const key1WithFieldsC = getKey(docKeys1, {}, { fields: ['abc'] });
      const key1WithFieldsD = getKey(docKeys1, {}, { fields: ['abc', 'def'] });

      assert.notDeepEqual(key1WithFieldsA, key1WithFieldsB);
      assert.notDeepEqual(key1WithFieldsA, key1WithFieldsD);
      assert.deepEqual(key1WithFieldsA, key1WithFieldsC);

      // Compare fields and orderBy
      assert.notDeepEqual(key1WithFieldsA, key1WithOrderByA);
      assert.notDeepEqual(key1WithFieldsD, key1WithOrderByD);
    });

    testQueryOperators(getKey);

    testFields(getKey);
  });

  describe('getUpdateKey()', () => {
    const getKey = (docKeys, doc, docInfo) => Array.from(Cache.getUpdateKey(docKeys, doc, docInfo));

    testQueryOperators(getKey);

    testWhenOperators(getKey);

    testFields(getKey);

    testDocInfoProperties(getKey, [
      { ifExists: true },
      { ttl: 1000 }
    ]);
  });

  describe('getRemoveKey', () => {
    const getKey = (docKeys, doc, docInfo) => Array.from(Cache.getRemoveKey(docKeys, doc, docInfo));

    testQueryOperators(getKey);

    testWhenOperators(getKey);

    testFields(getKey);

    testDocInfoProperties(getKey, [
      { ifExists: true },
      { deleteOnlyColumns: true }
    ]);
  });

  describe('getInsertKey', () => {
    const getKey = (docKeys, doc, docInfo) => Array.from(Cache.getInsertKey(docKeys, docInfo));

    testFields(getKey);

    testDocInfoProperties(getKey, [
      { ifNotExists: true },
      { ttl: 20 }
    ]);
  });
});

function testQueryOperators(getKey) {
  it('should consider query operators', () => {
    const key1 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: q.lt(10) });
    const key2 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: 10 });
    assert.notDeepEqual(key1, key2);
  });

  it('should consider nested query operators', () => {
    const key1 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: q.and(1, q.lt(10)) });
    const key2 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: q.and(1, 10) });
    assert.notDeepEqual(key1, key2);
  });
}

function testWhenOperators(getKey) {
  it('should consider when condition', () => {
    const key1 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 3 }, { when: { prop1: 1, prop2: q.lt(10) }});
    const key2 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 3 }, { when: { prop1: 1, prop2: 10 }});
    assert.notDeepEqual(key1, key2);
  });

  it('should consider nested query operators on when conditions', () => {
    const key1 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: q.and(1, q.lt(10)) });
    const key2 = getKey(['prop1', 'prop2', 'prop3'], { prop1: 1, prop2: q.and(1, 10) });
    assert.notDeepEqual(key1, key2);
  });
}

function testFields(getKey) {
  it('should not collide when including fields', () => {
    const key1 = getKey(['prop1', 'prop2'], {});
    const key2 = getKey(['prop1'], {}, { fields: ['prop2'] });
    const key3 = getKey(['prop1', 'prop2'], {}, { fields: ['prop2'] });
    assert.notDeepEqual(key1, key2);
    assert.notDeepEqual(key1, key3);
    assert.notDeepEqual(key2, key3);
  });
}

function testDocInfoProperties(getKey, items) {
  it('should consider docInfo properties', () => {
    items.forEach(docInfo => {
      const key = getKey(['prop1'], { prop1: 1}, docInfo);
      // Compare to a key without docInfo
      assert.notDeepEqual(key, getKey(['prop1'], { prop1: 1}, undefined));

      // Compare to each other
      items.forEach(compareDocInfo => {
        if (compareDocInfo === docInfo) {
          return;
        }
        assert.notDeepEqual(key, getKey(['prop1'], { prop1: 1}, compareDocInfo));
      });
    });
  });
}