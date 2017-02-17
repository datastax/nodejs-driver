"use strict";
var util = require('util');
var errors = require('./errors');
var utils = require('./utils');

/**
 * Max int that can be accurately represented with 64-bit Number (2^53)
 * @type {number}
 * @const
 */
var maxInt = 9007199254740992;

function noop() {}

/**
 * Forward-compatible allocation of buffer, filled with zeros.
 * @type {Function}
 */
var allocBuffer = Buffer.alloc || allocBufferFillDeprecated;

/**
 * Forward-compatible unsafe allocation of buffer.
 * @type {Function}
 */
var allocBufferUnsafe = Buffer.allocUnsafe || allocBufferDeprecated;

/**
 * Forward-compatible allocation of buffer to contain a string.
 * @type {Function}
 */
var allocBufferFromString = Buffer.from || allocBufferFromStringDeprecated;

/**
 * Forward-compatible allocation of buffer from an array of bytes
 * @type {Function}
 */
var allocBufferFromArray = Buffer.from || allocBufferFromArrayDeprecated;

function allocBufferDeprecated(size) {
  return new Buffer(size);
}

function allocBufferFillDeprecated(size) {
  var b = allocBufferDeprecated(size);
  b.fill(0);
  return b;
}

function allocBufferFromStringDeprecated(text, encoding) {
  if (typeof text !== 'string') {
    throw new TypeError('Expected string, obtained ' + util.inspect(text));
  }
  return new Buffer(text, encoding);
}

function allocBufferFromArrayDeprecated(arr) {
  if (!Array.isArray(arr)) {
    throw new TypeError('Expected Array, obtained ' + util.inspect(arr));
  }
  return new Buffer(arr);
}

/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  var targetBuffer = new Buffer(buf.length);
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
    //noinspection JSUnresolvedVariable
    if (!this.options || !this.options.logEmitter) {
      throw new Error('Log emitter not defined');
    }
    //noinspection JSUnresolvedVariable
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
  var total = 0;
  arr.forEach(function (item) {
    var length = item.length;
    length = length ? length : 0;
    total += length;
  });
  return total;
}

/**
 * Merge the contents of two or more objects together into the first object. Similar to jQuery.extend
 */
function extend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    if (!source) {
      return;
    }
    var keys = Object.keys(source);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var value = source[key];
      if (value === undefined) {
        continue;
      }
      target[key] = value;
    }
  });
  return target;
}

function lowerCaseExtend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop.toLowerCase()] = source[prop];
      }
    }
  });
  return target;
}

/**
 * Extends the target by the most inner props of sources
 * @param {Object} target
 * @returns {Object}
 */
function deepExtend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (!source.hasOwnProperty(prop)) {
        continue;
      }
      var targetProp = target[prop];
      var targetType = (typeof targetProp);
      //target prop is
      // a native single type
      // or not existent
      // or is not an anonymous object (not class instance)
      //noinspection JSUnresolvedVariable
      if (!targetProp ||
        targetType === 'number' ||
        targetType === 'string' ||
        util.isArray(targetProp) ||
        util.isDate(targetProp) ||
        targetProp.constructor.name !== 'Object'
        ) {
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
    var valA = a[name].apply(a, argArray);
    var valB = b[name].apply(b, argArray);
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
  var index = 0;
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
  var values = [];
  var item = iterator.next();
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
  var low = 0;
  var high = arr.length-1;

  while (low <= high) {
    var mid = (low + high) >>> 1;
    var midVal = arr[mid];
    var cmp = compareFunc(midVal, key);
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
  return ~low;  // key not found
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
  var position = binarySearch(arr, item, compareFunc);
  if (position < 0) {
    position = ~position;
  }
  arr.splice(position, 0, item);
}

/**
 * Binds the domain (if any) to the callback
 * @param {Function} callback
 * @param {String} [name]
 * @returns {Function}
 */
function bindDomain(callback, name) {
  if (typeof callback !== 'function') {
    throw new errors.ArgumentError(util.format('%s is not a function', name || 'callback'));
  }
  if (process.domain) {
    callback = process.domain.bind(callback);
  }
  return callback;
}

/**
 * Adapts the parameters based on the prepared metadata.
 * If the params are passed as an associative array (Object),
 * it adapts the object into an array with the same order as columns
 * @param {Array|Object} params
 * @param {Array} columns
 * @returns {{ params: Array, keys: Object}} Returns an array of parameters and the keys as an associative array.
 * @throws {Error} In case a parameter with a specific name is not defined
 */
function adaptNamedParamsPrepared(params, columns) {
  if (!params || util.isArray(params) || !columns || columns.length === 0) {
    //The parameters is an Array or there isn't parameter
    return { params: params, keys: null};
  }
  var paramsArray = new Array(columns.length);
  params = lowerCaseExtend({}, params);
  var keys = {};
  for (var i = 0; i < columns.length; i++) {
    var name = columns[i].name;
    if (!params.hasOwnProperty(name)) {
      throw new errors.ArgumentError(util.format('Parameter "%s" not defined', name));
    }
    paramsArray[i] = params[name];
    keys[name] = i;
  }
  return { params: paramsArray, keys: keys};
}

/**
 * Adapts the associative-array of parameters and hints for simple statements
 * into Arrays based on the (arbitrary) position of the keys.
 * @param {Array|Object} params
 * @param {QueryOptions} options
 * @returns {{ params: Array.<{name, value}>, keys: Object}} Returns an array of parameters and the keys as an associative array.
 */
function adaptNamedParamsWithHints(params, options) {
  if (!params || util.isArray(params)) {
    //The parameters is an Array or there isn't parameter
    return { params: params, keys: null};
  }
  options.namedParameters = true;
  var keys = Object.keys(params);
  var paramsArray = new Array(keys.length);
  var hints = new Array(keys.length);
  var userHints = options.hints || utils.emptyObject;
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    //As lower cased identifiers
    paramsArray[i] = { name: key.toLowerCase(), value: params[key]};
    hints[i] = userHints[key];
  }
  options.hints = hints;
  return { params: paramsArray, keys: keys};
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
  var keys = Object.keys(obj);
  var values = new Array(keys.length);
  for (var i = 0; i < keys.length; i++) {
    values[i] = obj[keys[i]];
  }
  return values;
}

/**
 * Wraps the callback-based method. When no originalCallback is not defined, it returns a Promise.
 * @param {ClientOptions} options
 * @param {Function} originalCallback
 * @param {Boolean} allowNoPromiseSupport
 * @param {Function} handler
 * @returns {Promise|undefined}
 */
function promiseWrapper(options, originalCallback, allowNoPromiseSupport, handler) {
  if (allowNoPromiseSupport && !originalCallback && !options.promiseFactory && typeof Promise === 'undefined') {
    // Optional callback on some methods is supported, even for js engines without Promise support
    originalCallback = noop;
  }
  if (typeof originalCallback === 'function') {
    // Callback-based invocation
    handler.call(this, bindDomain(originalCallback));
    return undefined;
  }
  var factory = options.promiseFactory;
  if (!factory) {
    if (typeof Promise === 'undefined') {
      throw new errors.ArgumentError(
        'Callback was not provided and Promise is undefined. See ' +
        'ClientOptions.promiseFactory documentation.');
    }
    factory = defaultPromiseFactory;
  }
  var self = this;
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
  var length = arr.length;
  if (length === 0) {
    return callback();
  }
  var completed = 0;
  for (var i = 0; i < length; i++) {
    fn(arr[i], next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
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
  var length = arr.length;
  if (length === 0) {
    return callback();
  }
  var sync;
  var index = 1;
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
  var length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  var result = new Array(length);
  var completed = 0;
  var invoke = useIndex ? invokeWithIndex : invokeWithoutIndex;
  for (var i = 0; i < length; i++) {
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
      var cb = callback;
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
  var length = arr.length;
  if (length === 0) {
    return callback(null, []);
  }
  var result = new Array(length);
  var index = 0;
  var sync;
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
    var i = index;
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
  var length = arr.length;
  var completed = 0;
  for (var i = 0; i < length; i++) {
    arr[i](next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
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
  var index = 0;
  var sync;
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
        //noinspection JSUnusedAssignment
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
  var completed = 0;
  for (var i = 0; i < count; i++) {
    iteratorFunc(i, next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
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
  callback = callback || noop;
  limit = Math.min(limit, count);
  var index = limit - 1;
  var i;
  var completed = 0;
  for (i = 0; i < limit; i++) {
    iteratorFunc(i, next);
  }
  i = -1;
  var sync = undefined;
  function next(err) {
    if (err) {
      var cb = callback;
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
      var captureIndex = index;
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
  var index = 1;
  var sync;
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
    var i = index++;
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
  var sync = 0;
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
exports.bindDomain = bindDomain;
exports.copyBuffer = copyBuffer;
exports.deepExtend = deepExtend;
exports.each = each;
exports.eachSeries = eachSeries;
/** @const */
exports.emptyArray = Object.freeze([]);
/** @const */
exports.emptyObject = Object.freeze({});
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
exports.times = times;
exports.timesLimit = timesLimit;
exports.timesSeries = timesSeries;
exports.totalLength = totalLength;
exports.whilst = whilst;
exports.HashSet = HashSet;