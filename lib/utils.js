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
 * Returns an array of parameters, containing also arguments as properties (query, params, consistency, options)
 */
function parseCommonArgs (query, params, consistency, options, callback) {
  var args = Array.prototype.slice.call(arguments);

  if (args.length < 2 || typeof args[args.length-1] !== 'function') {
    throw new Error('It should contain at least 2 arguments, with the callback as the last argument.');
  }

  if(args.length < 5) {
    options = null;
    callback = args[args.length-1];
    if (args.length < 4) {
      consistency = null;
      if (typeof params === 'number') {
        consistency = params;
        params = null;
      }
    }
    if (args.length < 3) {
      params = null;
    }
  }
  args.query = query;
  args.options = options;
  args.params = params;
  args.consistency = consistency;
  args.callback = callback;
  return args;
}


exports.copyBuffer = copyBuffer;
exports.extend = extend;
exports.totalLength = totalLength;
exports.syncEvent = syncEvent;
exports.parseCommonArgs = parseCommonArgs;
exports.fixStack = fixStack;
