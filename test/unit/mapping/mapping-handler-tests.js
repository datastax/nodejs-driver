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

const assert = require('assert');
const mapperTestHelper = require('./mapper-unit-test-helper');
const MappingHandler = require('../../../lib/mapping/mapping-handler');
const ModelMappingInfo = require('../../../lib/mapping/model-mapping-info');
const DefaultTableMappings = require('../../../lib/mapping/table-mappings').DefaultTableMappings;
const q = require('../../../lib/mapping/q').q;

describe('MappingHandler', () => {
  describe('#getSelectExecutor()', () => {
    const clientInfo = mapperTestHelper.getClient(['id', 'name', 'description'], [ 1, 2]);
    const handler = getMappingHandler(clientInfo);
    const getExecutor = (doc, docInfo) => handler.getSelectExecutor(doc, docInfo, false);

    testCacheGet(getExecutor, { id: 2 }, { id: 100 }, { id: 9999 });

    testCacheDifferentDocumentProperties(getExecutor, { id: 2 }, { id: 100, name: 'a'}, { id: q.in_([2, 3])});

    testCacheDifferentDocInfo(getExecutor, { id: 2 }, { limit: 1 }, { fields: [ 'name' ]});
  });

  describe('#getUpdateExecutor()', () => {
    const clientInfo = mapperTestHelper.getClient(['id', 'name', 'description', 'location'], [ 1 ]);
    const handler = getMappingHandler(clientInfo);
    const getExecutor = (doc, docInfo) => handler.getUpdateExecutor(doc, docInfo);

    testCacheGet(getExecutor, { id: 1, name: 'a' }, { id: 100, name: 'b' }, { id: 2, name: 'c' });

    testCacheDifferentDocumentProperties(
      getExecutor, { id: 2, name: 'a'}, { id: 100, description: 'a'}, { id: 2, location: 'b'});

    testCacheDifferentDocInfo(getExecutor, { id: 1, name: 'a' }, { ttl: 1 }, { fields: [ 'id', 'name' ]});
  });

  describe('#getInsertExecutor()', () => {
    const clientInfo = mapperTestHelper.getClient(['id', 'name', 'description', 'location'], [ 1 ]);
    const handler = getMappingHandler(clientInfo);
    const getExecutor = (doc, docInfo) => handler.getInsertExecutor(doc, docInfo);

    testCacheGet(getExecutor, { id: 2, name: 'a' }, { id: 100, name: 'b' }, { id: 2, name: 'c' });

    testCacheDifferentDocumentProperties(
      getExecutor,{ id: 2, name: 'a'}, { id: 100, description: 'a'}, { id: 2, location: 'b'});

    testCacheDifferentDocInfo(getExecutor, { id: 1, name: 'a' }, { ifNotExists: true }, { fields: [ 'id', 'name' ]});
  });

  describe('#getDeleteExecutor()', () => {
    const clientInfo = mapperTestHelper.getClient(['id', 'name', 'description', 'location'], [ 1, 1 ]);
    const handler = getMappingHandler(clientInfo);
    const getExecutor = (doc, docInfo) => handler.getDeleteExecutor(doc, docInfo);

    testCacheGet(getExecutor, { id: 2, name: 'a' }, { id: 100, name: 'b' }, { id: 2, name: 'c' });

    testCacheDifferentDocInfo(getExecutor, { id: 1, name: 'a' }, { ifExists: true }, { deleteOnlyColumns: true });
  });
});

function getMappingHandler(clientInfo) {
  return new MappingHandler(clientInfo.client,
    new ModelMappingInfo('ks1', [{ name: 't1' }], new DefaultTableMappings(), new Map()));
}

function testCacheGet(getExecutor, doc1, doc2, doc3) {
  it('should generate executor once and cache it', () => {
    let executor;

    return getExecutor(doc1)
      .then(result => executor = result)
      .then(() => Promise.all([ getExecutor(doc2), getExecutor(doc3) ]))
      .then(executors => {
        // After the first was cached, it should be the same
        assert.strictEqual(executor, executors[0]);
        assert.strictEqual(executor, executors[1]);
      });
  });
}

function testCacheDifferentDocumentProperties(getExecutor, doc1, doc2, doc3) {
  it('should generate a executor depending on the document properties', () => {
    let executor;

    return getExecutor(doc1)
      .then(result => executor = result)
      .then(() => Promise.all([ getExecutor(doc2), getExecutor(doc3) ]))
      .then(executors => {
        assert.notEqual(executor, executors[0]);
        assert.notEqual(executor, executors[1]);
        assert.notEqual(executors[0], executors[1]);
      });
  });
}

function testCacheDifferentDocInfo(getExecutor, doc, docInfo1, docInfo2) {
  it('should generate a executor depending on the doc info', () => {
    let executor;

    return getExecutor(doc)
      .then(result => executor = result)
      .then(() => Promise.all([ getExecutor(doc, docInfo1), getExecutor(doc, docInfo2) ]))
      .then(executors => {
        assert.notEqual(executor, executors[0]);
        assert.notEqual(executor, executors[1]);
        assert.notEqual(executors[0], executors[1]);
      });
  });
}