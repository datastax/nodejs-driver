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
const Tree = require('../../../lib/mapping/tree');

describe('Tree', function () {
  this.timeout(20000);

  describe('#getOrCreate()', () => {
    it('should reuse existing branches', () => {
      const tree = new Tree();
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'c'], () => true), true);
      assert.strictEqual(tree.edges.length, 1);
      assertNode(tree.edges[0], ['a', 'b', 'c'], true, 0);
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'c'], () => false), true);
      assert.strictEqual(tree.edges.length, 1);
    });

    it('should create new branches when not all keys match', () => {
      const tree = new Tree();
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'c'], () => 1), 1);
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'd'], () => 2), 2);
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'd', 'e', 'f'], () => 3), 3);
      assert.strictEqual(tree.getOrCreate(['r', 's', 't', 'u'], () => 4), 4);
      assert.strictEqual(tree.getOrCreate(['r', 's', 'x'], () => 5), 5);
      assert.strictEqual(tree.getOrCreate(['j', 'k', 'l'], () => 6), 6);
      assert.strictEqual(tree.getOrCreate(['j'], () => 7), 7);
      assert.strictEqual(tree.getOrCreate(['j', 'k'], () => 8), 8);


      // Check those are not created again
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'c'], () => false), 1);
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'd'], () => false), 2);
      assert.strictEqual(tree.getOrCreate(['a', 'b', 'd', 'e', 'f'], () => false), 3);
      assert.strictEqual(tree.getOrCreate(['j'], () => false), 7);
      assert.strictEqual(tree.getOrCreate(['j', 'k'], () => false), 8);
      assert.strictEqual(tree.getOrCreate(['j', 'k', 'l'], () => false), 6);

      assert.strictEqual(tree.edges.length, 3);
      assert.strictEqual(tree.length, 8);

      const ab = assertNode(tree.edges[0], ['a', 'b'], null, 2);
      assertNode(ab.edges[0], ['c'], 1, 0);
      const d = assertNode(ab.edges[1], ['d'], 2, 1);
      assertNode(d.edges[0], ['e', 'f'], 3, 0);

      const rs = assertNode(tree.edges[1], ['r', 's'], null, 2);
      assertNode(rs.edges[0], ['t', 'u'], 4, 0);

      const j = assertNode(tree.edges[2], ['j'], 7, 1);
      const k = assertNode(j.edges[0], [ 'k' ], 8, 1);
      assertNode(k.edges[0], [ 'l' ], 6, 0);
    });

    it('should set value if a node not a leaf and it is a match', () => {
      const tree = new Tree();
      tree.getOrCreate(['a', 'b', 'c'], () => true);
      tree.getOrCreate(['a', 'b', 'd'], () => true);
      assert.strictEqual(tree.getOrCreate(['a', 'b'], () => true), true);
    });

  });
});

function assertNode(node, key, value, edgesLength) {
  assert.deepEqual(node.key, key);
  assert.strictEqual(node.value, value);
  assert.strictEqual(node.edges.length, edgesLength);
  return node;
}