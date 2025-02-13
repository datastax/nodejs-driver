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

const Long = require('long');
const util = require('util');
const net = require('net');
const { EventEmitter } = require('events');

const errors = require('./errors');
const promiseUtils = require('./promise-utils');

/**
 * Max int that can be accurately represented with 64-bit Number (2^53)
 * @type {number}
 * @const
 */
const maxInt = 9007199254740992;

const maxInt32 = 0x7fffffff;

const emptyObject = Object.freeze({});

const emptyArray = Object.freeze([]);

function noop() {}

/**
 * Forward-compatible allocation of buffer, filled with zeros.
 * @type {Function}
 */
const allocBuffer = Buffer.alloc || allocBufferFillDeprecated;

/**
 * Forward-compatible unsafe allocation of buffer.
 * @type {Function}
 */
const allocBufferUnsafe = Buffer.allocUnsafe || allocBufferDeprecated;

/**
 * Forward-compatible allocation of buffer to contain a string.
 * @type {Function}
 */
const allocBufferFromString = (Int8Array.from !== Buffer.from && Buffer.from) || allocBufferFromStringDeprecated;

/**
 * Forward-compatible allocation of buffer from an array of bytes
 * @type {Function}
 */
const allocBufferFromArray = (Int8Array.from !== Buffer.from && Buffer.from) || allocBufferFromArrayDeprecated;

function allocBufferDeprecated(size) {
  // eslint-disable-next-line
  return new Buffer(size);
}

function allocBufferFillDeprecated(size) {
  const b = allocBufferDeprecated(size);
  b.fill(0);
  return b;
}

function allocBufferFromStringDeprecated(text, encoding) {
  if (typeof text !== 'string') {
    throw new TypeError('Expected string, obtained ' + util.inspect(text));
  }
  // eslint-disable-next-line
  return new Buffer(text, encoding);
}

function allocBufferFromArrayDeprecated(arr) {
  if (!Array.isArray(arr)) {
    throw new TypeError('Expected Array, obtained ' + util.inspect(arr));
  }
  // eslint-disable-next-line
  return new Buffer(arr);
}

/**
 * @returns {Function} Returns a wrapper function that invokes the underlying callback only once.
 * @param {Function} callback
 */
function callbackOnce(callback) {
  let cb = callback;

  return (function wrapperCallback(err, result) {
    cb(err, result);
    cb = noop;
  });
}

/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  const targetBuffer = allocBufferUnsafe(buf.length);
  buf.copy(targetBuffer);
  return targetBuffer;
}

/**
 * Appends the original stack trace to the error after a tick of the event loop
 */
function fixStack(stackTrace, error) {
  if (stackTrace) {
    error.stack += '\n  (event loop)\n' + stackTrace.substr(stackTrace.indexOf("\n") + 1);
  }
  return error;
}

/**
 * Uses the logEmitter to emit log events
 * @param {String} type
 * @param {String} info
 * @param [furtherInfo]
 */
function log(type, info, furtherInfo, options) {
  if (!this.logEmitter) {
    const effectiveOptions = options || this.options;
    if (!effectiveOptions || !effectiveOptions.logEmitter) {
      throw new Error('Log emitter not defined');
    }
    this.logEmitter = effectiveOptions.logEmitter;
  }
  this.logEmitter('log', type, this.constructor.name, info, furtherInfo || '');
}

/**
 * Gets the sum of the length of the items of an array
 */
function totalLength (arr) {
  if (arr.length === 1) {
    return arr[0].length;
  }
  let total = 0;
  arr.forEach(function (item) {
    let length = item.length;
    length = length ? length : 0;
    total += length;
  });
  return total;
}

/**
 * Merge the contents of two or more objects together into the first object. Similar to jQuery.extend / Object.assign.
 * The main difference between this method is that declared properties with an <code>undefined</code> value are not set
 * to the target.
 */
function extend(target) {
  const sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    if (!source) {
      return;
    }
    const keys = Object.keys(source);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const value = source[key];
      if (value === undefined) {
        continue;
      }
      target[key] = value;
    }
  });
  return target;
}

/**
 * Returns a new object with the property names set to lowercase.
 */
function toLowerCaseProperties(obj) {
  const keys = Object.keys(obj);
  const result = {};
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    result[k.toLowerCase()] = obj[k];
  }
  return result;
}

/**
 * Extends the target by the most inner props of sources
 * @param {Object} target
 * @returns {Object}
 */
function deepExtend(target) {
  const sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (const prop in source) {
      // eslint-disable-next-line no-prototype-builtins
      if (!source.hasOwnProperty(prop)) {
        continue;
      }
      const targetProp = target[prop];
      const targetType = (typeof targetProp);
      //target prop is
      // a native single type
      // or not existent
      // or is not an anonymous object (not class instance)
      if (!targetProp ||
        targetType === 'number' ||
        targetType === 'string' ||
        Array.isArray(targetProp) ||
        targetProp instanceof Date ||
        targetProp.constructor.name !== 'Object') {
        target[prop] = source[prop];
      }
      else {
        //inner extend
        target[prop] = deepExtend({}, targetProp, source[prop]);
      }
    }
  });
  return target;
}

function propCompare(propName) {
  return function (a, b) {
    if (a[propName] > b[propName]) {
      return 1;
    }
    if (a[propName] < b[propName]) {
      return -1;
    }
    return 0;
  };
}

function funcCompare(name, argArray) {
  return (function (a, b) {
    if (typeof a[name] === 'undefined') {
      return 0;
    }
    const valA = a[name].apply(a, argArray);
    const valB = b[name].apply(b, argArray);
    if (valA > valB) {
      return 1;
    }
    if (valA < valB) {
      return -1;
    }
    return 0;
  });
}
/**
 * Uses the iterator protocol to go through the items of the Array
 * @param {Array} arr
 * @returns {Iterator}
 */
function arrayIterator (arr) {
  return arr[Symbol.iterator]();
}

/**
 * Convert the iterator values into an array
 * @param iterator
 * @returns {Array}
 */
function iteratorToArray(iterator) {
  const values = [];
  let item = iterator.next();
  while (!item.done) {
    values.push(item.value);
    item = iterator.next();
  }
  return values;
}

/**
 * Searches the specified Array for the provided key using the binary
 * search algorithm.  The Array must be sorted.
 * @param {Array} arr
 * @param key
 * @param {function} compareFunc
 * @returns {number} The position of the key in the Array, if it is found.
 * If it is not found, it returns a negative number which is the bitwise complement of the index of the first element that is larger than key.
 */
function binarySearch(arr, key, compareFunc) {
  let low = 0;
  let high = arr.length-1;

  while (low <= high) {
    const mid = (low + high) >>> 1;
    const midVal = arr[mid];
    const cmp = compareFunc(midVal, key);
    if (cmp < 0) {
      low = mid + 1;
    }
    else if (cmp > 0) {
      high = mid - 1;
    }
    else
    {
      //The key was found in the Array
      return mid;
    }
  }
  // key not found
  return ~low;
}

/**
 * Inserts the value in the position determined by its natural order determined by the compare func
 * @param {Array} arr
 * @param item
 * @param {function} compareFunc
 */
function insertSorted(arr, item, compareFunc) {
  if (arr.length === 0) {
    return arr.push(item);
  }
  let position = binarySearch(arr, item, compareFunc);
  if (position < 0) {
    position = ~position;
  }
  arr.splice(position, 0, item);
}

/**
 * Validates the provided parameter is of type function.
 * @param {Function} fn The instance to validate.
 * @param {String} [name] Name of the function to use in the error message. Defaults to 'callback'.
 * @returns {Function}
 */
function validateFn(fn, name) {
  if (typeof fn !== 'function') {
    throw new errors.ArgumentError(util.format('%s is not a function', name || 'callback'));
  }
  return fn;
}

/**
 * Adapts the parameters based on the prepared metadata.
 * If the params are passed as an associative array (Object),
 * it adapts the object into an array with the same order as columns
 * @param {Array|Object} params
 * @param {Array} columns
 * @returns {Array} Returns an array of parameters.
 * @throws {Error} In case a parameter with a specific name is not defined
 */
function adaptNamedParamsPrepared(params, columns) {
  if (!params || Array.isArray(params) || !columns || columns.length === 0) {
    // params is an array or there aren't parameters
    return params;
  }
  const paramsArray = new Array(columns.length);
  params = toLowerCaseProperties(params);
  const keys = {};
  for (let i = 0; i < columns.length; i++) {
    const name = columns[i].name;
    // eslint-disable-next-line no-prototype-builtins
    if (!params.hasOwnProperty(name)) {
      throw new errors.ArgumentError(util.format('Parameter "%s" not defined', name));
    }
    paramsArray[i] = params[name];
    keys[name] = i;
  }
  return paramsArray;
}

/**
 * Adapts the associative-array of parameters and hints for simple statements
 * into Arrays based on the (arbitrary) position of the keys.
 * @param {Array|Object} params
 * @param {ExecutionOptions} execOptions
 * @returns {{ params: Array<{name, value}>, namedParameters: boolean, keyIndexes: object }} Returns an array of
 * parameters and the keys as an associative array.
 */
function adaptNamedParamsWithHints(params, execOptions) {
  if (!params || Array.isArray(params)) {
    //The parameters is an Array or there isn't parameter
    return { params: params, namedParameters: false, keyIndexes: null };
  }

  const keys = Object.keys(params);
  const paramsArray = new Array(keys.length);
  const hints = new Array(keys.length);
  const userHints = execOptions.getHints() || emptyObject;
  const keyIndexes = {};

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    // As lower cased identifiers
    paramsArray[i] = { name: key.toLowerCase(), value: params[key]};
    hints[i] = userHints[key];
    keyIndexes[key] = i;
  }

  execOptions.setHints(hints);

  return { params: paramsArray, namedParameters: true, keyIndexes };
}

/**
 * Returns a string with a value repeated n times
 * @param {String} val
 * @param {Number} times
 * @returns {String}
 */
function stringRepeat(val, times) {
  if (!times || times < 0) {
    return null;
  }
  if (times === 1) {
    return val;
  }
  return new Array(times + 1).join(val);
}

/**
 * Returns an array containing the values of the Object, similar to Object.values().
 * If obj is null or undefined, it will return an empty array.
 * @param {Object} obj
 * @returns {Array}
 */
function objectValues(obj) {
  if (!obj) {
    return emptyArray;
  }
  const keys = Object.keys(obj);
  const values = new Array(keys.length);
  for (let i = 0; i < keys.length; i++) {
    values[i] = obj[keys[i]];
  }
  return values;
}

/**
 * Wraps the callback-based method. When no originalCallback is not defined, it returns a Promise.
 * @param {ClientOptions} options
 * @param {Function} originalCallback
 * @param {Function} handler
 * @returns {Promise|undefined}
 */
function promiseWrapper(options, originalCallback, handler) {
  if (typeof originalCallback === 'function') {
    // Callback-based invocation
    handler.call(this, originalCallback);
    return undefined;
  }
  const factory = options.promiseFactory || defaultPromiseFactory;
  const self = this;
  return factory(function handlerWrapper(callback) {
    handler.call(self, callback);
  });
}

/**
 * @param {Function} handler
 * @returns {Promise}
 */
function defaultPromiseFactory(handler) {
  return new Promise(function executor(resolve, reject) {
    handler(function handlerCallback(err, result) {
      if (err) {
        return reject(err);
      }
      resolve(result);
    });
  });
}

/**
 * Returns the first not undefined param
 */
function ifUndefined(v1, v2) {
  return v1 !== undefined ? v1 : v2;
}

/**
 * Returns the first not undefined param
 */
function ifUndefined3(v1, v2, v3) {
  if (v1 !== undefined) {
    return v1;
  }
  return v2 !== undefined ? v2 : v3;
}

/**
 * Shuffles an Array in-place.
 * @param {Array} arr
 * @returns {Array}
 * @private
 */
function shuffleArray(arr) {
  // Fisher–Yates algorithm
  for (let i = arr.length - 1; i > 0; i--) {
    // Math.random() has an extremely short permutation cycle length but we don't care about collisions
    const j = Math.floor(Math.random() * (i + 1));
    const temp = arr[i];
    arr[i] = arr[j];
    arr[j] = temp;
  }

  return arr;
}

// Classes

/**
 * Represents a unique set of values.
 * @constructor
 */
function HashSet() {
  this.length = 0;
  this.items = {};
}

/**
 * Adds a new item to the set.
 * @param {Object} key
 * @returns {boolean} Returns true if it was added to the set; false if the key is already present.
 */
HashSet.prototype.add = function (key) {
  if (this.contains(key)) {
    return false;
  }
  this.items[key] = true;
  this.length++;
  return true;
};

/**
 * @returns {boolean} Returns true if the key is present in the set.
 */
HashSet.prototype.contains = function (key) {
  return this.length > 0 && this.items[key] === true;
};

/**
 * Removes the item from set.
 * @param key
 * @return {boolean} Returns true if the key existed and was removed, otherwise it returns false.
 */
HashSet.prototype.remove = function (key) {
  if (!this.contains(key)) {
    return false;
  }
  delete this.items[key];
  this.length--;
};

/**
 * Returns an array containing the set items.
 * @returns {Array}
 */
HashSet.prototype.toArray = function () {
  return Object.keys(this.items);
};

/**
 * Utility class that resolves host names into addresses.
 */
class AddressResolver {

  /**
   * Creates a new instance of the resolver.
   * @param {Object} options
   * @param {String} options.nameOrIp
   * @param {Object} [options.dns]
   */
  constructor(options) {
    if (!options || !options.nameOrIp || !options.dns) {
      throw new Error('nameOrIp and dns lib must be provided as part of the options');
    }

    this._resolve4 = util.promisify(options.dns.resolve4);
    this._nameOrIp = options.nameOrIp;
    this._isIp = net.isIP(options.nameOrIp);
    this._index = 0;
    this._addresses = null;
    this._refreshing = null;
  }

  /**
   * Resolves the addresses for the host name.
   */
  async init() {
    if (this._isIp) {
      return;
    }

    await this._resolve();
  }

  /**
   * Tries to resolve the addresses for the host name.
   */
  async refresh() {
    if (this._isIp) {
      return;
    }

    if (this._refreshing) {
      return await promiseUtils.fromEvent(this._refreshing, 'finished');
    }

    this._refreshing = new EventEmitter().setMaxListeners(0);

    try {
      await this._resolve();
    } catch (err) {
      // Ignore the possible resolution error
    }

    this._refreshing.emit('finished');
    this._refreshing = null;
  }

  async _resolve() {
    const arr = await this._resolve4(this._nameOrIp);

    if (!arr || arr.length === 0) {
      throw new Error(`${this._nameOrIp} could not be resolved`);
    }

    this._addresses = arr;
  }

  /**
   * Returns resolved ips in a round-robin fashion.
   */
  getIp() {
    if (this._isIp) {
      return this._nameOrIp;
    }

    const item = this._addresses[this._index % this._addresses.length];
    this._index = (this._index !== maxInt32) ? (this._index + 1) : 0;

    return item;
  }
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function each(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  if (length === 0) {
    return callback();
  }
  let completed = 0;
  for (let i = 0; i < length; i++) {
    fn(arr[i], next);
  }
  function next(err) {
    if (err) {
      const cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed !== length) {
      return;
    }
    callback();
  }
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function eachSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  if (length === 0) {
    return callback();
  }
  let sync;
  let index = 1;
  fn(arr[0], next);
  if (sync === undefined) {
    sync = false;
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index >= length) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    if (sync) {
      return process.nextTick(function () {
        fn(arr[index++], next);
      });
    }
    fn(arr[index++], next);
  }
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function forEachOf(arr, fn, callback) {
  return mapEach(arr, fn, true, callback);
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function map(arr, fn, callback) {
  return mapEach(arr, fn, false, callback);
}

function mapEach(arr, fn, useIndex, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  const result = new Array(length);
  let completed = 0;
  const invoke = useIndex ? invokeWithIndex : invokeWithoutIndex;
  for (let i = 0; i < length; i++) {
    invoke(i);
  }

  function invokeWithoutIndex(i) {
    fn(arr[i], function mapItemCallback(err, transformed) {
      result[i] = transformed;
      next(err);
    });
  }

  function invokeWithIndex(i) {
    fn(arr[i], i, function mapItemCallback(err, transformed) {
      result[i] = transformed;
      next(err);
    });
  }

  function next(err) {
    if (err) {
      const cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed !== length) {
      return;
    }
    callback(null, result);
  }
}

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
function mapSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  const result = new Array(length);
  let index = 0;
  let sync;
  invoke(0);
  if (sync === undefined) {
    sync = false;
  }

  function invoke(i) {
    fn(arr[i], function mapItemCallback(err, transformed) {
      result[i] = transformed;
      next(err);
    });
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (++index === length) {
      return callback(null, result);
    }
    if (sync === undefined) {
      sync = true;
    }
    const i = index;
    if (sync) {
      return process.nextTick(function () {
        invoke(i);
      });
    }
    invoke(index);
  }
}

/**
 * @param {Array.<Function>} arr
 * @param {Function} [callback]
 */
function parallel(arr, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  const length = arr.length;
  let completed = 0;
  for (let i = 0; i < length; i++) {
    arr[i](next);
  }
  function next(err) {
    if (err) {
      const cb = callback;
      callback = noop;
      return cb(err);
    }
    if (++completed !== length) {
      return;
    }
    callback();
  }
}

/**
 * Similar to async.series(), but instead accumulating the result in an Array, it callbacks with the result of the last
 * function in the array.
 * @param {Array.<Function>} arr
 * @param {Function} [callback]
 */
function series(arr, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  let index = 0;
  let sync;
  next();
  function next(err, result) {
    if (err) {
      return callback(err);
    }
    if (index === arr.length) {
      return callback(null, result);
    }
    if (sync) {
      return process.nextTick(function () {
        sync = true;
        arr[index++](next);
        sync = false;
      });
    }
    sync = true;
    arr[index++](next);
    sync = false;
  }
}

/**
 * @param {Number} count
 * @param {Function} iteratorFunc
 * @param {Function} [callback]
 */
function times(count, iteratorFunc, callback) {
  callback = callback || noop;
  count = +count;
  if (isNaN(count) || count === 0) {
    return callback();
  }
  let completed = 0;
  for (let i = 0; i < count; i++) {
    iteratorFunc(i, next);
  }
  function next(err) {
    if (err) {
      const cb = callback;
      callback = noop;
      return cb(err);
    }
    if (++completed !== count) {
      return;
    }
    callback();
  }
}

/**
 * @param {Number} count
 * @param {Number} limit
 * @param {Function} iteratorFunc
 * @param {Function} [callback]
 */
function timesLimit(count, limit, iteratorFunc, callback) {
  let sync = undefined;
  callback = callback || noop;
  limit = Math.min(limit, count);
  let index = limit - 1;
  let i;
  let completed = 0;
  for (i = 0; i < limit; i++) {
    iteratorFunc(i, next);
  }
  i = -1;
  function next(err) {
    if (err) {
      const cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed === count) {
      return callback();
    }
    index++;
    if (index >= count) {
      return;
    }
    if (sync === undefined) {
      sync = (i >= 0);
    }
    if (sync) {
      const captureIndex = index;
      return process.nextTick(function () {
        iteratorFunc(captureIndex, next);
      });
    }
    iteratorFunc(index, next);
  }
}

/**
 * @param {Number} count
 * @param {Function} iteratorFunction
 * @param {Function} callback
 */
function timesSeries(count, iteratorFunction, callback) {
  count = +count;
  if (isNaN(count) || count < 1) {
    return callback();
  }
  let index = 1;
  let sync;
  iteratorFunction(0, next);
  if (sync === undefined) {
    sync = false;
  }
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index === count) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    const i = index++;
    if (sync) {
      //Prevent "Maximum call stack size exceeded"
      return process.nextTick(function () {
        iteratorFunction(i, next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    iteratorFunction(i, next);
  }
}

/**
 * @param {Function} condition
 * @param {Function} fn
 * @param {Function} callback
 */
function whilst(condition, fn, callback) {
  let sync = 0;
  next();
  function next(err) {
    if (err) {
      return callback(err);
    }
    if (!condition()) {
      return callback();
    }
    if (sync === 0) {
      sync = 1;
      fn(function (err) {
        if (sync === 1) {
          //sync function
          sync = 4;
        }
        next(err);
      });
      if (sync === 1) {
        //async function
        sync = 2;
      }
      return;
    }
    if (sync === 4) {
      //Prevent "Maximum call stack size exceeded"
      return process.nextTick(function () {
        fn(next);
      });
    }
    //do a sync call as the callback is going to call on a future tick
    fn(next);
  }
}


/**
 * Contains the methods for reading and writing vints into binary format.
 * Exposes only 2 internal methods, the rest are hidden.
 */
const VIntCoding = (function () {
  /** @param {Long} n */
  function encodeZigZag64(n) {
    //     (n << 1) ^ (n >> 63);
    return n.toUnsigned().shiftLeft(1).xor(n.shiftRight(63));
  }

  /** @param {Long} n */
  function decodeZigZag64(n) {
    //     (n >>> 1) ^ -(n & 1);
    return n.shiftRightUnsigned(1).xor(n.and(Long.ONE).negate());
  }

  /**
   * @param {Long} value
   * @param {Buffer} buffer
   * @returns {Number}
   */
  function writeVInt(value, buffer) {
    return writeUnsignedVInt(encodeZigZag64(value), buffer);
  }

  /**
   * @param {Long} value
   * @param {Buffer} buffer
   * @returns {number}
   */
  function writeUnsignedVInt(value, buffer) {
    const size = computeUnsignedVIntSize(value);
    if (size === 1) {
      buffer[0] = value.getLowBits();
      return 1;
    }
    encodeVInt(value, size, buffer);
    return size;
  }

  /**
   * @param {Long} value
   * @returns {number}
   */
  function computeUnsignedVIntSize(value) {
    const magnitude = numberOfLeadingZeros(value.or(Long.ONE));
    return (639 - magnitude * 9) >> 6;
  }

  /**
   * @param {Long} value
   * @param {Number} size
   * @param {Buffer} buffer
   */
  function encodeVInt(value, size, buffer) {
    const extraBytes = size - 1;
    let intValue = value.getLowBits();
    let i;
    let intBytes = 4;
    for (i = extraBytes; i >= 0 && (intBytes--) > 0; i--) {
      buffer[i] = 0xFF & intValue;
      intValue >>= 8;
    }
    intValue = value.getHighBits();
    for (; i >= 0; i--) {
      buffer[i] = 0xFF & intValue;
      intValue >>= 8;
    }
    buffer[0] |= encodeExtraBytesToRead(extraBytes);
  }
  /**
   * Returns the number of zero bits preceding the highest-order one-bit in the binary representation of the value.
   * @param {Long} value
   * @returns {Number}
   */
  function numberOfLeadingZeros(value) {
    if (value.equals(Long.ZERO)) {
      return 64;
    }
    let n = 1;
    let x = value.getHighBits();
    if (x === 0) {
      n += 32;
      x = value.getLowBits();
    }
    if (x >>> 16 === 0) {
      n += 16;
      x <<= 16;
    }
    if (x >>> 24 === 0) {
      n += 8;
      x <<= 8;
    }
    if (x >>> 28 === 0) {
      n += 4;
      x <<= 4;
    }
    if (x >>> 30 === 0) {
      n += 2;
      x <<= 2;
    }
    n -= x >>> 31;
    return n;
  }


  function encodeExtraBytesToRead(extraBytesToRead) {
    return ~(0xff >> extraBytesToRead);
  }

  /**
   * @param {Buffer} buffer
   * @param {{value: number}} offset
   * @returns {Long}
   */
  function readVInt(buffer, offset) {
    return decodeZigZag64(readUnsignedVInt(buffer, offset));
  }

  /**
   * uvint_unpack
   * @param {Buffer} input
   * @param {{ value: number}} offset
   * @returns {Long}
   */
  function readUnsignedVInt(input, offset) {
    const firstByte = input[offset.value++];
    if ((firstByte & 0x80) === 0) {
      return Long.fromInt(firstByte);
    }
    const sByteInt = fromSignedByteToInt(firstByte);
    const size = numberOfExtraBytesToRead(sByteInt);
    let result = Long.fromInt(sByteInt & firstByteValueMask(size));
    for (let ii = 0; ii < size; ii++) {
      const b = Long.fromInt(input[offset.value++]);
      //       (result << 8) | b
      result = result.shiftLeft(8).or(b);
    }
    return result;
  }

  function fromSignedByteToInt(value) {
    if (value > 0x7f) {
      return value - 0x0100;
    }
    return value;
  }

  function numberOfLeadingZerosInt32(i) {
    if (i === 0) {
      return 32;
    }
    let n = 1;
    if (i >>> 16 === 0) {
      n += 16;
      i <<= 16;
    }
    if (i >>> 24 === 0) {
      n += 8;
      i <<= 8;
    }
    if (i >>> 28 === 0) {
      n += 4;
      i <<= 4;
    }
    if (i >>> 30 === 0) {
      n += 2;
      i <<= 2;
    }
    n -= i >>> 31;
    return n;
  }

  /**
   * @param {Number} firstByte
   * @returns {Number}
   */
  function numberOfExtraBytesToRead(firstByte) {
    // Instead of counting 1s of the byte, we negate and count 0 of the byte
    return numberOfLeadingZerosInt32(~firstByte) - 24;
  }

  /**
   * @param {Number} extraBytesToRead
   * @returns {Number}
   */
  function firstByteValueMask(extraBytesToRead) {
    return 0xff >> extraBytesToRead;
  }

  /**
   * @param {Number} value
   * @param {Buffer} output
   * @returns {void}
   */
  // eslint-disable-next-line no-unused-vars
  function writeUnsignedVInt32(value, output) {
    writeUnsignedVInt(Long.fromNumber(value), output);
  }

  /**
   * Read up to a 32-bit integer back, using the unsigned (no zigzag) encoding.
   *
   * <p>Note this method is the same as {@link #readUnsignedVInt(DataInput)}, except that we do
   * *not* block if there are not enough bytes in the buffer to reconstruct the value.
   *
   * @param {Buffer} input
   * @param {Number} readerIndex
   * @returns {Number}
   * @throws VIntOutOfRangeException If the vint doesn't fit into a 32-bit integer
   */
  // eslint-disable-next-line no-unused-vars
  function getUnsignedVInt32(input, readerIndex) {
    return checkedCast(getUnsignedVInt(input, readerIndex, input.length));
  }

  /**
   * 
   * @param {Buffer} input 
   * @param {Number} readerIndex 
   * @param {Number} readerLimit 
   * @returns {Long}
   */
  function getUnsignedVInt(input, readerIndex, readerLimit) {
    if (readerIndex < 0)
    {throw new errors.ArgumentError(
      "Reader index should be non-negative, but was " + readerIndex);}

    if (readerIndex >= readerLimit) {return Long.fromNumber(-1);}

    const firstByte = /** @type {Number} */ (input.at(readerIndex++));

    // Bail out early if this is one byte, necessary or it fails later
    if (firstByte >= 0) {return Long.fromNumber(firstByte);}

    const size = numberOfExtraBytesToRead(firstByte);
    if (readerIndex + size > readerLimit) {return Long.fromNumber(-1);}

    const retval = Long.fromNumber(firstByte & firstByteValueMask(size));
    for (let ii = 0; ii < size; ii++) {
      const b = /** @type {Number} */ (input.at(readerIndex++));
      retval.shiftLeft(8);
      retval.or(b & 0xff);
    }

    return retval;
  }

  /**
   * 
   * @param {Long} value 
   * @returns {Number} 
   */
  function checkedCast(value) {
    const result = value.toInt();
    if (value.notEquals(result)) {throw new errors.VIntOutOfRangeException(value);}
    return result;
  }

  /**
   * 
   * @param {Buffer} bytes 
   * @returns {[number, number]} [size, bytes read]
   */
  function uvintUnpack(bytes) {
    const firstByte = bytes[0];

    if ((firstByte & 128) === 0) {
      return [firstByte, 1];
    }

    const numExtraBytes = 8 - (~firstByte & 0xff).toString(2).length;
    let rv = firstByte & (0xff >> numExtraBytes);
    
    for (let idx = 1; idx <= numExtraBytes; idx++) {
      const newByte = bytes[idx];
      rv <<= 8;
      rv |= newByte & 0xff;
    }

    return [rv, numExtraBytes + 1];
  }

  /**
 * 
 * @param {Number} val 
 * @returns {Buffer}
 */
  function uvintPack(val) {
    const rv = [];
    if (val < 128) {
      rv.push(val);
    } else {
      let v = val;
      let numExtraBytes = 0;
      let numBits = v.toString(2).length;
      let reservedBits = numExtraBytes + 1;

      while (numBits > (8 - reservedBits)) {
        numExtraBytes += 1;
        numBits -= 8;
        reservedBits = Math.min(numExtraBytes + 1, 8);
        rv.push(v & 0xff);
        v >>= 8;
      }

      if (numExtraBytes > 8) {
        throw new Error(`Value ${val} is too big and cannot be encoded as vint`);
      }

      const n = 8 - numExtraBytes;
      v |= (0xff >> n) << n;
      rv.push(Math.abs(v));
    }

    rv.reverse();
    return Buffer.from(rv);
  }

  return {
    readVInt: readVInt,
    writeVInt: writeVInt,
    uvintPack: uvintPack,
    uvintUnpack: uvintUnpack
  };
})();

exports.adaptNamedParamsPrepared = adaptNamedParamsPrepared;
exports.adaptNamedParamsWithHints = adaptNamedParamsWithHints;
exports.AddressResolver = AddressResolver;
exports.allocBuffer = allocBuffer;
exports.allocBufferUnsafe = allocBufferUnsafe;
exports.allocBufferFromArray = allocBufferFromArray;
exports.allocBufferFromString = allocBufferFromString;
exports.arrayIterator = arrayIterator;
exports.binarySearch = binarySearch;
exports.callbackOnce = callbackOnce;
exports.copyBuffer = copyBuffer;
exports.deepExtend = deepExtend;
exports.each = each;
exports.eachSeries = eachSeries;
/** @const */
exports.emptyArray = Object.freeze([]);
/** @const */
exports.emptyObject = emptyObject;
exports.extend = extend;
exports.fixStack = fixStack;
exports.forEachOf = forEachOf;
exports.funcCompare = funcCompare;
exports.ifUndefined = ifUndefined;
exports.ifUndefined3 = ifUndefined3;
exports.insertSorted = insertSorted;
exports.iteratorToArray = iteratorToArray;
exports.log = log;
exports.map = map;
exports.mapSeries = mapSeries;
exports.maxInt = maxInt;
exports.noop = noop;
exports.objectValues = objectValues;
exports.parallel = parallel;
exports.promiseWrapper = promiseWrapper;
exports.propCompare = propCompare;
exports.series = series;
exports.shuffleArray = shuffleArray;
exports.stringRepeat = stringRepeat;
exports.times = times;
exports.timesLimit = timesLimit;
exports.timesSeries = timesSeries;
exports.totalLength = totalLength;
exports.validateFn = validateFn;
exports.whilst = whilst;
exports.HashSet = HashSet;
exports.VIntCoding = VIntCoding;