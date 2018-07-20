'use strict';

const assert = require('assert');
const Cache = require('../../../lib/mapper/cache');

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
      const key1WithOrderByA = getKey(docKeys1, {}, { orderBy: ['abc'] });
      const key1WithOrderByB = getKey(docKeys1, {}, { orderBy: ['def'] });
      const key1WithOrderByC = getKey(docKeys1, {}, { orderBy: ['abc'] });
      const key1WithOrderByD = getKey(docKeys1, {}, { orderBy: ['abc', 'def'] });

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

    it('should consider doc values when those provide a hashKey');

    it('should not collide when including fields', () => {
      const key1 = getKey(['prop1', 'prop2'], {});
      const key2 = getKey(['prop1'], {}, { fields: ['prop2'] });
      assert.notDeepEqual(key1, key2);
    });

    it('should consider query operators');
  });
});