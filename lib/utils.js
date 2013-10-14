var async = require('async');
var util = require('util');
var Int64 = require('node-int64');
var types = require('./types.js');
/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  var targetBuffer = new Buffer(buf.length);
  buf.copy(targetBuffer);
  return targetBuffer;
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
 * Merge the contents of two or more objects together into the first object. Similar to jQuert.extend
 */
function extend(target) {
  var sources = [].slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      target[prop] = source[prop];
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

var queryParser = (function() {
  function parse(query, args) {
    if (args == null || args.length === 0) {
      return query;
    }

    var q = 0;
    var a = 0;
    var str = '';

    try {
      while (q >= 0) {
        var oldq = q;
        q = query.indexOf('?', q);
        if (q >= 0) {
          str += query.substr(oldq, q-oldq);
          if (args[a] === undefined) {
            throw new QueryParserError('Query parameter number ' + (a+1) + ' is not defined. Placeholder for not provided argument.');
          }
          str += types.typeEncoder.stringifyValue(args[a++]);
          q += 1;
        } else {
          str += query.substr(oldq);
        }
      }

      return str;
    }
    catch (e) {
      throw new QueryParserError(e);
    }
  }
  
  return {
    parse: parse
  }
})();

function QueryParserError(e) {
  QueryParserError.super_.call(this, e.message, this.constructor);
  this.internalError = e;
}

util.inherits(QueryParserError, types.DriverError);

exports.copyBuffer = copyBuffer;
exports.extend = extend;
exports.queryParser = queryParser;
exports.totalLength = totalLength;
exports.syncEvent = syncEvent;
exports.parseCommonArgs = parseCommonArgs;