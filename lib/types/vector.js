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
"use strict";
/** @module types */
/**
 * Creates a new instance of Cql Vector, also compatible with Float32Array.
 * @class
 */
const util = require('node:util');
class Vector {
  /**
     *
     * @param {Float32Array | Array<any>} elements
     * @param {string} [subtype]
     */
  constructor (elements, subtype) {
    if (elements instanceof Float32Array) {
      this.elements = Array.from(elements);
    }
    else if (Array.isArray(elements)) {
      this.elements = elements;
    }
    else {
      throw new TypeError('Vector must be constructed with a Float32Array or an Array');
    }
    if (this.elements.length === 0) {
      throw new TypeError('Vector must contain at least one value');
    }
    /**
         * Returns the number of the elements.
         * @type Number
         */
    this.length = this.elements.length;
    this.subtype = subtype;
    return new Proxy(this, {
      get: function (obj, key) {
        if (key === 'IDENTITY'){
          return 'Vector';
        } else if (typeof (key) === 'string' && (Number.isInteger(Number(key)))) // key is an index
        {return obj.elements[key];}
        return obj[key];
      },
      set: function (obj, key, value) {
        if (typeof (key) === 'string' && (Number.isInteger(Number(key)))) // key is an index
        {return obj.elements[key] = value;}
        return obj[key] = value;
      },
      ownKeys: function (obj) {
        return Reflect.ownKeys(elements);
      },
      getOwnPropertyDescriptor(target, key) {
        if (typeof (key) === 'string' && (Number.isInteger(Number(key)))){
          // array index
          return { enumerable: true, configurable: true};
        }
        return Reflect.getOwnPropertyDescriptor(target, key);
                
      }
    });
  }
  /**
     * Returns the string representation of the vector.
     * @returns {string}
     */
  toString() {
    return "[".concat(this.elements.toString(), "]");
  }
  /**
     *
     * @param {number} index
     */
  at(index) {
    return this.elements[index];
  }

  /**
   * 
   * @returns {IterableIterator<any>} an iterator over the elements of the vector
   */
  [Symbol.iterator]() {
    return this.elements[Symbol.iterator]();
  }

  static get [Symbol.species]() {
    return Vector;
  }

  /**
     * 
     * @param {(value: any, index: number, array: any[]) => void} callback
     */
  forEach(callback) {
    return this.elements.forEach(callback);
  }

  /**
   * @returns {string | undefined} get the subtype string, e.g., "float", but it's optional so it can return null
   */
  getSubtype(){
    return this.subtype;
  }
}

Object.defineProperty(Vector, Symbol.hasInstance, {
  value: function (i) { 
    return (util.types.isProxy(i) && i.IDENTITY === 'Vector') || i instanceof Float32Array; }
});

module.exports = Vector;
