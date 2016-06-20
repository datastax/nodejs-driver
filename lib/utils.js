/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var types = require('cassandra-driver').types;
var consistencyNames;

exports.emptyObject = Object.freeze({});

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
exports.each = function each(arr, fn, callback) {
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
};

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
exports.eachSeries = function eachSeries(arr, fn, callback) {
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
};

exports.extend = function extend(target) {
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
};

/**
 * Gets the name in upper case of the consistency level.
 * @param {Number} consistency
 */
exports.getConsistencyName = function getConsistencyName(consistency) {
  if (consistency == undefined) {
    //null or undefined => undefined
    return undefined;
  }
  loadConsistencyNames();
  var name = consistencyNames[consistency];
  if (!name) {
    throw new Error(util.format(
      'Consistency %s not found, use values defined as properties in types.consistencies object', consistency
    ));
  }
  return name;
};

/**
 * Returns the first not undefined param
 */
exports.ifUndefined = function (v1, v2) {
  return v1 !== undefined ? v1 : v2;
};

/**
 * Returns the first not undefined param
 */
exports.ifUndefined3 = function (v1, v2, v3) {
  if (v1 !== undefined) {
    return v1;
  }
  return v2 !== undefined ? v2 : v3;
};

function loadConsistencyNames() {
  if (consistencyNames) {
    return;
  }
  consistencyNames = {};
  var propertyNames = Object.keys(types.consistencies);
  for (var i = 0; i < propertyNames.length; i++) {
    var name = propertyNames[i];
    consistencyNames[types.consistencies[name]] = name.toUpperCase();
  }
  //Using java constants naming conventions
  consistencyNames[types.consistencies.localQuorum]  = 'LOCAL_QUORUM';
  consistencyNames[types.consistencies.eachQuorum]   = 'EACH_QUORUM';
  consistencyNames[types.consistencies.localSerial]  = 'LOCAL_SERIAL';
  consistencyNames[types.consistencies.localOne]     = 'LOCAL_ONE';
}

/**
 * Similar to async.series(), but instead accumulating the result in an Array, it callbacks with the result of the last
 * function in the array.
 * @param {Array.<Function>} arr
 * @param {Function} [callback]
 */
exports.series = function series(arr, callback) {
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
};

/**
 * @param {Number} count
 * @param {Function} iteratorFunc
 * @param {Function} [callback]
 */
exports.times = function times(count, iteratorFunc, callback) {
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
};

/**
 * @param {Number} count
 * @param {Function} iteratorFunction
 * @param {Function} callback
 */
exports.timesSeries = function timesSeries(count, iteratorFunction, callback) {
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
};

/**
 * @param {Function} condition
 * @param {Function} fn
 * @param {Function} callback
 */
exports.whilst = function whilst(condition, fn, callback) {
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
};