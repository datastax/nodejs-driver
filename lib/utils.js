var util = require('util');
var errors = require('./errors');

/**
 * @const
 * @type {string}
 * @private
 */
var _hex = '0123456789abcdef';
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
  if (!arr) {
    return 0;
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
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
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
  var sources = [].slice.call(arguments, 1);
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

/**
 * Sync events: executes the callback when the event (with the same parameters) have been called in all emitters.
 */
function syncEvent(emitters, eventName, context, callback) {
  var thisKey = '';
  var eventListener = getListener(eventName, context);
  emitters.forEach(function (item) {
    thisKey += '_' + item.constructor.name;
    item.on(eventName, eventListener);
  });
  context[thisKey] = {emittersLength: emitters.length};
  
  function getListener(eventName, context) {
    return function listener () {
      var argsKey = '_' + eventName + Array.prototype.slice.call(arguments).join('_');
      var elements = context[thisKey];
      if (typeof elements[argsKey] === 'undefined') {
        elements[argsKey] = 0;
        return;
      }
      else if (elements[argsKey] < elements.emittersLength-2){
        elements[argsKey] = elements[argsKey] + 1;
        return;
      }
      delete elements[argsKey];
      callback.apply(context, Array.prototype.slice.call(arguments));
    };
  }
}

/**
 * Parses the arguments used by exec methods.
 * @returns {{query, params, options, length: Number, callback: Function}} Arguments as properties (query, params, options, callback)
 */
function parseCommonArgs (query, params, options, callback) {
  var args = Array.prototype.slice.call(arguments);

  if (args.length < 2 || typeof args[args.length-1] !== 'function') {
    throw new Error('It should contain at least 2 arguments, with the callback as the last argument.');
  }

  if (args.length < 4) {
    options = null;
    callback = args[args.length-1];
    if (typeof params === 'number') {
      params = null;
    }
  }
  if (args.length === 2) {
    params = null;
  }
  return ({
    query: query,
    params: params,
    options: options,
    callback: callback,
    length: args.length
  });
}

function propCompare(propName) {
  return function (a, b) {
    if (a[propName] > b[propName])
      return 1;
    if (a[propName] < b[propName])
      return -1;
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
    if (valA > valB)
      return 1;
    if (valA < valB)
      return -1;
    return 0;
  });
}

/**
 * Parses an array of column metadata into column type info
 * @param {Array} columns
 * @returns {Array} An array of typeInfo
 */
function parseColumnDefinitions(columns) {
  var hint = [];
  for (var i = 0; i < columns.length; i++) {
    //type is an Array that contains:
    // in the first position the main type
    // in the second position the subtypes
    var c = columns[i];
    var typeOption = c;
    if (!util.isArray(typeOption)) {
      typeOption = c.type;
    }
    var subtypes = typeOption[1];
    if (util.isArray(subtypes) && util.isArray(subtypes[0])) {
      subtypes = parseColumnDefinitions(subtypes);
    }
    hint[i] = { name: c.name, type: typeOption[0], subtypes: subtypes};
  }
  return hint;
}

/**
 * Converts a buffer representation of an IP address to a string in IP format
 * @param {Buffer} value
 */
function toIpString(value) {
  if (!value) {
    return null;
  }
  if (value.length == 4) {
    return value[0] + '.' + value[1] + '.' + value[2] + '.' + value[3];
  }
  return value.toString('hex');
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
    return {value: arr[index++], done: false}
  }};
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
 * @returns {Function}
 */
function bindDomain(callback) {
  if (process.domain) {
    callback = process.domain.bind(callback);
  }
  return callback;
}

/**
 * If the params are passed as an associative array (Object),
 *  it adapts the object into an array with the same order as columns
 * @param {Array|Object} params
 * @param {Array} columns
 * @returns {{ params: Array, keys: Object}} Returns an array of parameters and the keys as an associative array.
 * @throws {Error} In case a parameter with a specific name is not defined
 */
function adaptNamedParams(params, columns) {
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

exports.adaptNamedParams = adaptNamedParams;
exports.arrayIterator = arrayIterator;
exports.binarySearch = binarySearch;
exports.bindDomain = bindDomain;
exports.copyBuffer = copyBuffer;
exports.deepExtend = deepExtend;
/** @const */
exports.emptyArray = Object.freeze([]);
/** @const */
exports.emptyObject = Object.freeze({});
exports.extend = extend;
exports.fixStack = fixStack;
exports.funcCompare = funcCompare;
exports.insertSorted = insertSorted;
exports.log = log;
exports.maxInt = 9007199254740992;
exports.parseCommonArgs = parseCommonArgs;
exports.propCompare = propCompare;
exports.stringRepeat = stringRepeat;
exports.syncEvent = syncEvent;
exports.totalLength = totalLength;
exports.parseColumnDefinitions = parseColumnDefinitions;
exports.toIpString = toIpString;