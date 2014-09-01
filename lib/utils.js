var util = require('util');
/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  var targetBuffer = new Buffer(buf.length);
  buf.copy(targetBuffer);
  return targetBuffer;
}

/**
 * Appends the original stacktrace to the error after a tick of the event loop
 */
function fixStack(stackTrace, error) {
  error.stack += '\n  (event loop)\n' + stackTrace.substr(stackTrace.indexOf("\n") + 1);
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
    throw new Error('Log emitter not defined');
  }
  this.logEmitter('log', type, info, this.constructor.name, furtherInfo || '');
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
  var sources = [].slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
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
 * Returns an array of parameters, containing also arguments as properties (query, params, options)
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
  args.query = query;
  args.params = params;
  args.options = options;
  args.callback = callback;
  return args;
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
 * Converts a buffer representation of an IP address to a string in IP format
 * @param {Buffer} value
 */
function toIpString(value) {

  if (value.length == 4) {
    return value[0] + '.' + value[1] + '.' + value[2] + '.' + value[3];
  }
  else {
    return value.toString('hex');
  }
}


exports.copyBuffer = copyBuffer;
exports.deepExtend = deepExtend;
exports.extend = extend;
exports.fixStack = fixStack;
exports.funcCompare = funcCompare;
exports.log = log;
exports.maxInt = 9007199254740992;
exports.parseCommonArgs = parseCommonArgs;
exports.propCompare = propCompare;
exports.syncEvent = syncEvent;
exports.totalLength = totalLength;
exports.toIpString = toIpString;