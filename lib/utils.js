var util = require('util');
var Int64 = require('node-int64');
/**
 * Creates a copy of a buffer
 */
function copyBuffer(buf) {
  var targetBuffer = new Buffer(buf.length);
  buf.copy(targetBuffer);
  return targetBuffer;
}

/**
 * Merge the contents of two or more objects together into the first object. Similar to jQuert.extend
 */
function extend(target) {
  var sources = [].slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (!target.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
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
          str += encodeParam(args[a++]);
          q += 1;
        } else {
          str += query.substr(oldq);
        }
      }

      return str;
    }
    catch (e) {
      throw new QueryParserError(e.message);
    }
  }

  function quote(x) {
    return '\'' + x + '\'';
  }

  function encodeParam(x) {
    if(x === null) {
      return 'null';
    }
    if(x instanceof Date) {
      return x.getTime().toString();
    }
    if(x instanceof Array) {
      return stringifyArray(x);
    }
    if (typeof x === 'string') {
      //quotestring
      return quote(x);
    }
    if (x instanceof Int64) {
      return stringifyInt64(x);
    }
    if (x.hint) {
      if (x.hint === 'set') {
        return stringifyArray(x.value, '{', '}');
      }
      else if (x.hint === 'map') {
        return stringifyMap(x.value);
      }
    }
    if (x instanceof Buffer) {
      return '0x' + x.toString('hex');
    }
    if (x.toString) {
      return x.toString();
    }
    return x;
  }
  
  function stringifyArray(x, openChar, closeChar) {
    if (!openChar) {
      openChar = '[';
      closeChar = ']';
    }
    var stringValues = new Array();
    for (var i=0;i<x.length;i++) {
      stringValues.push(encodeParam(x[i]));
    }
    return openChar + stringValues.join() + closeChar;
  }
  
  function stringifyInt64(x) {
    return 'blobAsBigint(0x' + x.toOctetString() + ')';
  }
  
  function stringifyMap(x) {
    var stringValues = new Array();
    for (var key in x) {
      stringValues.push(encodeParam(key) + ':' + encodeParam(x[key]));
    }
    return '{' + stringValues.join() + '}';
  }
  
  function QueryParserError(message) {
    this.message = message;
    Error.call(this, message);
  }

  util.inherits(QueryParserError, Error);
  
  return {
    parse: parse,
    encodeParam: encodeParam
  }
})();

exports.copyBuffer = copyBuffer;
exports.extend = extend;
exports.queryParser = queryParser;