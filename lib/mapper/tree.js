'use strict';

/**
 * Represents a tree node where the key is composed by 1 or more strings.
 */
class Node {
  /**
   * @param {Array<String>} key
   * @param {Object} value
   * @param {Array} [edges]
   */
  constructor(key, value, edges) {
    this.key = key;
    this.value = value;
    this.edges = edges || [];
  }
}

/**
 * A radix tree where each node contains a key, a value and possible edges.
 */
class Tree extends Node {
  constructor() {
    super([], null);
    this.length = 0;
  }

  getOrCreate(keyIterator, valueHandler) {
    if (typeof keyIterator.next !== 'function') {
      keyIterator = keyIterator[Symbol.iterator]();
    }
    let node = this;
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
      this.length++;
      node.edges.push(new Node(iteratorToArray(item.value, keyIterator), value));
      return value;
    }
    return node.value;
  }

  _createBranch(node, index, useNewValue, valueHandler) {
    const newBranch = new Node(node.key.slice(index), node.value, node.edges);
    node.key = node.key.slice(0, index);
    node.edges = [ newBranch ];
    if (useNewValue) {
      // The previous node value has moved to a leaf
      // The node containing the new leaf should use the new value
      node.value = valueHandler();
      this.length++;
    }
    else {
      // Clear the value as it was copied in the branch
      node.value = null;
    }
    return newBranch;
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

module.exports = Tree;