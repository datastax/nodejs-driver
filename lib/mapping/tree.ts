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
import EventEmitter from "events";



/**
 * Represents a tree node where the key is composed by 1 or more strings.
 * @ignore @internal
 */
class Node extends EventEmitter {
  key: string[];
  value: object;
  edges: any[];
  /**
   * Creates a new instance of {@link Node}.
   * @param {Array<String>} key
   * @param {Object} value
   * @param {Array} [edges]
   */
  constructor(key: Array<string>, value: object, edges?: Array<any>) {
    super();
    this.key = key;
    this.value = value;
    this.edges = edges || [];
  }
}

/**
 * A radix tree where each node contains a key, a value and edges.
 * @ignore @internal
 */
class Tree extends Node {
  length: number;
  constructor() {
    super([], null);
    this.length = 0;
  }

  /**
   * Gets the existing item in the tree or creates a new one with the value provided by valueHandler
   * @param {Iterator} keyIterator
   * @param {Function} valueHandler
   * @return {Object}
   */
  getOrCreate<T extends object>(keyIterator: Iterator<string>, valueHandler: () => T ): T {
    if (typeof keyIterator.next !== 'function') {
      keyIterator = keyIterator[Symbol.iterator]();
    }
    let node : Tree = this;
    let isMatch = false;
    let item = keyIterator.next();
    while (true) {
      let newBranch;
      // Check node keys at position 1 and above
      for (let i = 1; i < node.key.length; i++) {
        if (item.done || node.key[i] !== item.value) {
          // We should branch out
          newBranch = this._createBranch(node, i, item.done, valueHandler);
          break;
        }
        item = keyIterator.next();
      }

      if (item.done) {
        isMatch = true;
        break;
      }

      if (newBranch !== undefined) {
        break;
      }

      const edges = node.edges;
      let nextNode;
      for (let i = 0; i < edges.length; i++) {
        const e = edges[i];
        if (e.key[0] === item.value) {
          // its a match
          nextNode = e;
          item = keyIterator.next();
          break;
        }
      }

      if (nextNode === undefined) {
        // Current node is the root for a new leaf
        break;
      }
      else {
        node = nextNode;
      }
    }

    if (!isMatch) {
      // Create using "node" as the root
      const value = valueHandler();
      node.edges.push(new Node(iteratorToArray(item.value, keyIterator), value));
      this._onItemAdded();
      return value;
    }
    if (node.value === null && node.edges.length > 0) {
      node.value = valueHandler();
    }
    return node.value as T;
  }

  private _createBranch(node, index, useNewValue, valueHandler) {
    const newBranch = new Node(node.key.slice(index), node.value, node.edges);
    node.key = node.key.slice(0, index);
    node.edges = [ newBranch ];
    if (useNewValue) {
      // The previous node value has moved to a leaf
      // The node containing the new leaf should use the new value
      node.value = valueHandler();
      this._onItemAdded();
    }
    else {
      // Clear the value as it was copied in the branch
      node.value = null;
    }
    return newBranch;
  }

  _onItemAdded() {
    this.length++;
    this.emit('add', this.length);
  }
}

function iteratorToArray(value, iterator) {
  const values = [ value ];
  let item = iterator.next();
  while (!item.done) {
    values.push(item.value);
    item = iterator.next();
  }
  return values;
}

export default Tree;