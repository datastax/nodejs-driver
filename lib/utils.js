var util = require('util');
var Int64 = require('node-int64');

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
    else if(x instanceof Date) {
      return x.getTime().toString();
    }
    else if(x instanceof Array) {
      return stringifyArray(x);
    }
    else if (typeof x === 'string') {
      //quotestring
      return quote(stringify(x));
    }
    else if (x instanceof Int64) {
      return stringifyInt64(x);
    }
    else if (x.hint) {
      if (x.hint === 'set') {
        return stringifyArray(x.value, '{', '}');
      }
      else if (x.hint === 'map') {
        return stringifyMap(x.value);
      }
    }
    return stringify(x);
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

  function stringify(x) {
    // node buffers should be hex encoded
    if (x instanceof Buffer) {
      return x.toString('hex');
    }
    if (x.toString) {
      return x.toString();
    }
    return x;
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


exports.extend = extend;
exports.queryParser = queryParser;