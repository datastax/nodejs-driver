"use strict";
const util = require('util');
const errors = require('./errors');

/**
 * Max int that can be accurately represented with 64-bit Number (2^53)
 * @type {number}
 * @const
 */
const maxInt = 9007199254740992;

const emptyObject = Object.freeze({});

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
function log(type, info, furtherInfo) {
  if (!this.logEmitter) {
    if (!this.options || !this.options.logEmitter) {
      throw new Error('Log emitter not defined');
    }
    this.logEmitter = this.options.logEmitter;
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
        util.isArray(targetProp) ||
        util.isDate(targetProp) ||
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
 * @param arr
 * @returns {{next: function}}
 */
function arrayIterator (arr) {
  let index = 0;
  return { next : function () {
    if (index >= arr.length) {
      return {done: true};
    }
    return {value: arr[index++], done: false };
  }};
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
  if (!params || util.isArray(params) || !columns || columns.length === 0) {
    // params is an array or there aren't parameters
    return params;
  }
  const paramsArray = new Array(columns.length);
  params = toLowerCaseProperties(params);
  const keys = {};
  for (let i = 0; i < columns.length; i++) {
    const name = columns[i].name;
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
  if (!params || util.isArray(params)) {
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
    return exports.emptyArray;
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
 * Shuffles an Array in-place.
 * @param {Array} arr
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
  if (this.items[key]) {
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
  return this.items[key] === true;
};

/**
 * Returns an array containing the set items.
 * @returns {Array}
 */
HashSet.prototype.toArray = function () {
  return Object.keys(this.items);
};

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

exports.adaptNamedParamsPrepared = adaptNamedParamsPrepared;
exports.adaptNamedParamsWithHints = adaptNamedParamsWithHints;
exports.allocBuffer = allocBuffer;
exports.allocBufferUnsafe = allocBufferUnsafe;
exports.allocBufferFromArray = allocBufferFromArray;
exports.allocBufferFromString = allocBufferFromString;
exports.arrayIterator = arrayIterator;
exports.binarySearch = binarySearch;
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
exports.stringRepeat = stringRepeat;
exports.shuffleArray = shuffleArray;
exports.times = times;
exports.timesLimit = timesLimit;
exports.timesSeries = timesSeries;
exports.totalLength = totalLength;
exports.validateFn = validateFn;
exports.whilst = whilst;
exports.HashSet = HashSet;