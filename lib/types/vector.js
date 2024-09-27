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
var Vector = /** @class */ (function () {
    /**
     *
     * @param {Float32Array | Array<any>} elements
     */
    function Vector(elements) {
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
        return new Proxy(this, {
            get: function (obj, key) {
                if (key === 'IDENTITY'){
                    return 'Vector';
                } else if (typeof (key) === 'string' && (Number.isInteger(Number(key)))) // key is an index
                    return obj.elements[key];
                else
                    return obj[key];
            },
            set: function (obj, key, value) {
                if (typeof (key) === 'string' && (Number.isInteger(Number(key)))) // key is an index
                    return obj.elements[key] = value;
                else
                    return obj[key] = value;
            }
        });
    }
    /**
     * Returns the string representation of the vector.
     * @returns {string}
     */
    Vector.prototype.toString = function () {
        return "[".concat(this.elements.toString(), "]");
    };
    /**
     *
     * @param {number} index
     */
    Vector.prototype.at = function (index) {
        return this.elements[index];
    };
    /**
     *
     * @param  {...any} elements
     * @returns
     */
    Vector.of = function () {
        var elements = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            elements[_i] = arguments[_i];
        }
        return new Vector(elements);
    };
    /**
     * `
     * @returns {ArrayIterator}
     */
    Vector.prototype[Symbol.iterator] = function () {
        return this.elements[Symbol.iterator]();
    };

    Vector.prototype[Symbol.species] =  {
        /**
         * instanceof
         */
        get: function () {
            return Vector;
        },
        enumerable: false,
        configurable: true
    };

    return Vector;
}());

Object.defineProperty(Vector, Symbol.hasInstance, {
    value: function (i) { 
        return (util.types.isProxy(i) && i.IDENTITY === 'Vector') || i instanceof Float32Array; }
});

module.exports = Vector;
