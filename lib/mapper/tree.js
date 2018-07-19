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

  getOrCreate(key, valueHandler) {
    let node = this;
    let keyIndex = 0;
    let isMatch = false;
    while (node) {
      let newBranch;
      // Check node keys at position 1 and above
      for (let i = 1; i < node.key.length; i++) {
        if (node.key[i] !== key[keyIndex] || keyIndex === key.length) {
          // We should branch out
          newBranch = this._createBranch(node, i, keyIndex === key.length, valueHandler);
          break;
        }
        keyIndex++;
      }

      if (keyIndex === key.length) {
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
        if (e.key[0] === key[keyIndex]) {
          // its a match
          nextNode = e;
          keyIndex++;
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
      node.edges.push(new Node(key.slice(keyIndex), value));
      return value;
    }
    return node.value;
  }

  _createBranch(node, keyIndex, useNewValue, valueHandler) {
    const newBranch = new Node(node.key.slice(keyIndex), node.value, node.edges);
    node.key = node.key.slice(0, keyIndex);
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

//TODO: consider using key iterables

module.exports = Tree;