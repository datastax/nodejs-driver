var assert = require('assert');
var util = require('util');
var events = require('events');
var Int64 = require('node-int64');
var uuid = require('node-uuid');
var async = require('async');
var utils = require('../lib/utils.js');
var types = require('../lib/types.js');
var config = require('./config.js');
var dataTypes = types.dataTypes;
var Connection = require('../index.js').Connection;

before(function (done) {
  this.timeout(5000);
  var con = new Connection(utils.extend({}, config));
  async.series([
    con.open.bind(con), 
    function (next) {
      con.execute('select cql_version, native_protocol_version, release_version from system.local', function (err, result) {
        if (!err && result && result.rows) {
          console.log('Cassandra version', result.rows[0].get('release_version'));
          console.log('Cassandra higher protocol version', result.rows[0].get('native_protocol_version'), '\n');
        }
        next();
      });
    },
    con.close.bind(con)], done);
});

describe('types', function () {
  describe('typeEncoder', function () {
    describe('#stringifyValue()', function () {
      it('should be valid for query', function () {
        function testStringify(value, expected, dataType) {
          var stringValue = types.typeEncoder.stringifyValue({value: value, hint: dataType});
          if (typeof stringValue === 'string') {
            stringValue = stringValue.toLowerCase();
          }
          assert.strictEqual(stringValue, expected);
        }
        testStringify(1, '1', dataTypes.int);
        testStringify(1.1, '1.1', dataTypes.double);
        testStringify("text", "'text'", dataTypes.text);
        testStringify("It's a quote", "'it''s a quote'", 'text');
        testStringify("some 'quoted text'", "'some ''quoted text'''", dataTypes.text);
        testStringify(null, 'null', dataTypes.text);
        testStringify([1,2,3], '[1,2,3]', dataTypes.list);
        testStringify([], '[]', dataTypes.list);
        testStringify(['one', 'two'], '[\'one\',\'two\']', dataTypes.list);
        testStringify(['one', 'two'], '{\'one\',\'two\'}', 'set');
        testStringify({key1:'value1', key2:'value2'}, '{\'key1\':\'value1\',\'key2\':\'value2\'}', 'map');
        testStringify(new Int64('56789abcdef0123'), 'blobasbigint(0x056789abcdef0123)', dataTypes.bigint);
        var date = new Date('Tue, 13 Aug 2013 09:10:32 GMT');
        testStringify(date, date.getTime().toString(), dataTypes.timestamp);
        var uuidValue = uuid.v4();
        testStringify(uuidValue, uuidValue.toString(), dataTypes.uuid);
      });
    });

    describe('#guessDataType()', function () {
      it('should guess the native types', function () {
        var guessDataType = types.typeEncoder.guessDataType;
        assert.strictEqual(guessDataType(1), dataTypes.int, 'Guess type for an integer number failed');
        assert.strictEqual(guessDataType(1.01), dataTypes.double, 'Guess type for a double number failed');
        assert.strictEqual(guessDataType(true), dataTypes.boolean, 'Guess type for a boolean value failed');
        assert.strictEqual(guessDataType([1,2,3]), dataTypes.list, 'Guess type for an Array value failed');
        assert.strictEqual(guessDataType('a string'), dataTypes.text, 'Guess type for an string value failed');
        assert.strictEqual(guessDataType(new Buffer('bip bop')), dataTypes.blob, 'Guess type for a buffer value failed');
        assert.strictEqual(guessDataType(new Date()), dataTypes.timestamp, 'Guess type for a Date value failed');
        assert.strictEqual(guessDataType(new Int64(10, 10)), dataTypes.bigint, 'Guess type for a Int64 value failed');
        assert.strictEqual(guessDataType(uuid.v4()), dataTypes.uuid, 'Guess type for a UUID value failed');
      });
    });

    describe('#encode() and #decode', function () {
      var typeEncoder = types.typeEncoder;
      it('should encode and decode maps', function () {
        var value = {value1: 'Surprise', value2: 'Mothafucka'};
        var encoded = typeEncoder.encode({hint: dataTypes.map, value: value});
        var decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.text]]]);
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });

      it('should encode and decode list<int>', function () {
        var value = [1, 2, 3, 4];
        var encoded = typeEncoder.encode({hint: 'list<int>', value: value});
        var decoded = typeEncoder.decode(encoded, [dataTypes.list, [dataTypes.int]]);
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });

      it('should encode and decode set<text>', function () {
        var value = ['1', '2', '3', '4'];
        var encoded = typeEncoder.encode({hint: 'set<text>', value: value});
        var decoded = typeEncoder.decode(encoded, [dataTypes.set, [dataTypes.text]]);
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
    })
  });

  describe('ResultStream', function() {
    it('should be readable as soon as it has data', function (done) {
      var buf = [];
      var stream = new types.ResultStream();
      
      stream.on('end', function streamEnd() {
        assert.equal(Buffer.concat(buf).toString(), 'Jimmy McNulty');
        done();
      });
      stream.on('readable', function streamReadable() {
        var item = stream.read();
        while (item) {
          buf.push(item);
          item = stream.read();
        }
      });
      
      stream.add(new Buffer('Jimmy'));
      stream.add(new Buffer(' '));
      stream.add(new Buffer('McNulty'));
      stream.add(null);
    });

    it('should buffer until is read', function (done) {
      var buf = [];
      var stream = new types.ResultStream({highWaterMark: 1});
      stream.add(new Buffer('Stringer'));
      stream.add(new Buffer(' '));
      stream.add(new Buffer('Bell'));
      stream.add(null);
      
      stream.on('end', function streamEnd() {
        assert.equal(Buffer.concat(buf).toString(), 'Stringer Bell');
        done();
      });
      assert.ok(stream.buffer.length > 0, 'It must buffer when there is no listener for the "readable" event');
      stream.on('readable', function streamReadable() {
        var item = stream.read();
        while (item) {
          buf.push(item);
          item = stream.read();
        }
      });
    });
  });
});

describe('utils', function () {
  describe('#syncEvent()', function () {
    it('should execute callback once for all emitters', function () {
      var emitter1 = new events.EventEmitter();
      var emitter2 = new events.EventEmitter(); 
      var emitter3 = new events.EventEmitter(); 
      var callbackCounter = 0;
      utils.syncEvent([emitter1, emitter2, emitter3], 'dummy', this, function (text){
        assert.strictEqual(text, 'bop');
        callbackCounter = callbackCounter + 1;
      });
      assert.ok(emitter1.emit('dummy', 'bip'));
      emitter1.emit('dummy', 'bop');
      emitter2.emit('dummy', 'bip');
      emitter2.emit('dummy', 'bop');
      emitter3.emit('dummy', 'bop');
      assert.strictEqual(callbackCounter, 1);
    });
  });

  describe('#parseCommonArgs()', function () {
    it('parses args and can be retrieved by name', function () {
      function testArgs(args, expectedLength) {
        assert.strictEqual(args.length, expectedLength, 'The arguments length do not match');
        assert.ok(args.query, 'Query must be defined');
        assert.strictEqual(typeof args.callback, 'function', 'Callback must be a function ');
        if (args && args.length > 2) {
          assert.ok(util.isArray(args.params) || args.params === null, 'params must be an array or null');
          assert.ok(typeof args.consistency === 'number' || args.consistency === null, 'Consistency must be an int or null');
        }
      }
      var args = utils.parseCommonArgs('A QUERY 1', function (){});
      assert.ok(args && args.length == 2 && args.query && args.callback);
      assert.throws(utils.parseCommonArgs, Error, 'It must contain at least 2 arguments.');
      args = utils.parseCommonArgs('A QUERY 2', [1, 2, 3], function (){});
      testArgs(args, 3);
      assert.ok(util.isArray(args.params) && args.params.length === 3);
      args = utils.parseCommonArgs('A QUERY 3', types.consistencies.quorum, function (){});
      testArgs(args, 3);
      assert.ok(args.params === null && args.consistency === types.consistencies.quorum, 'Consistency does not match');
      args = utils.parseCommonArgs('A QUERY', [1, 2, 3], types.consistencies.quorum, function (){});
      testArgs(args, 4);
      assert.ok(args.params && args.consistency, 'Params and consistency must not be null');
      args = utils.parseCommonArgs('A QUERY', [1, 2, 3], types.consistencies.quorum, {}, function (){});
      testArgs(args, 5);
      assert.ok(args.params && args.consistency && args.options, 'Params, consistency and options must not be null');
    });

    it('parses args and can be retrieved as an array', function () {
      var args = utils.parseCommonArgs('A QUERY', function (){});
      assert.ok(util.isArray(args), 'The returned object must be an Array');
      assert.strictEqual(args[0], 'A QUERY', 'The first element must be the query');
      assert.strictEqual(args.length, 2, 'There must be 2 arguments in array');
    });
  });

  describe('#extend()', function () {
    it('should allow null sources', function () {
      var originalObject = {};
      var extended = utils.extend(originalObject, null);
      assert.strictEqual(originalObject, extended);
    });
  });
});