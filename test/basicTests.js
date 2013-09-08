var utils = require('../lib/utils.js');
var queryParser = utils.queryParser;
var types = require('../lib/types.js');
var util = require('util');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;
var uuid = require('node-uuid');
var dataTypes = types.dataTypes;
var events = require('events');

module.exports = {
  encodeParamsTest: function (test) {
    function testStringify(value, expected, dataType) {
      var stringValue = types.typeEncoder.stringifyValue({value: value, hint: dataType});
      if (typeof stringValue === 'string') {
        stringValue = stringValue.toLowerCase();
      }
      test.strictEqual(stringValue, expected);
    }
    var stringifyValue = types.typeEncoder.stringifyValue;
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
    test.done();
  },
  'guess data type': function (test) {
    var guessDataType = types.typeEncoder.guessDataType;
    test.ok(guessDataType(1) === dataTypes.int, 'Guess type for an integer number failed');
    test.ok(guessDataType(1.01) === dataTypes.double, 'Guess type for a double number failed');
    test.ok(guessDataType(true) === dataTypes.boolean, 'Guess type for a boolean value failed');
    test.ok(guessDataType([1,2,3]) === dataTypes.list, 'Guess type for an Array value failed');
    test.ok(guessDataType('a string') === dataTypes.text, 'Guess type for an string value failed');
    test.ok(guessDataType(new Buffer('bip bop')) === dataTypes.blob, 'Guess type for a buffer value failed');
    test.ok(guessDataType(new Date()) === dataTypes.timestamp, 'Guess type for a Date value failed');
    test.ok(guessDataType(new Int64(10, 10)) === dataTypes.bigint, 'Guess type for a Int64 value failed');
    test.ok(guessDataType(uuid.v4()) === dataTypes.uuid, 'Guess type for a UUID value failed');
    test.done();
  },
  'event synchronization': function (test) {
    var emitter1 = new events.EventEmitter();
    var emitter2 = new events.EventEmitter(); 
    var emitter3 = new events.EventEmitter(); 
    var executedCallback = false;
    utils.syncEvent([emitter1, emitter2, emitter3], 'dummy', this, function (text){
      test.ok(text === 'bop');
      executedCallback = true;
    });
    test.ok(emitter1.emit('dummy', 'bip'));
    emitter1.emit('dummy', 'bop');
    emitter2.emit('dummy', 'bip');
    emitter2.emit('dummy', 'bop');
    emitter3.emit('dummy', 'bop');
    test.ok(executedCallback);
    test.done();
  },
  'field stream - can stream': function (test) {
    var buf = [];
    var stream = new types.FieldStream();
    
    stream.on('end', function streamEnd() {
      test.equal(Buffer.concat(buf).toString(), 'Jimmy McNulty');
      test.done();
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
  },
  'field stream - can buffer': function (test) {
    var buf = [];
    var stream = new types.FieldStream({highWaterMark: 1});
    stream.add(new Buffer('Stringer'));
    stream.add(new Buffer(' '));
    stream.add(new Buffer('Bell'));
    stream.add(null);
    
    stream.on('end', function streamEnd() {
      test.equal(Buffer.concat(buf).toString(), 'Stringer Bell');
      test.done();
    });
    test.ok(stream.buffer.length > 0, 'It must buffer when there is no listener for the "readable" event');
    stream.on('readable', function streamReadable() {
      var item = stream.read();
      while (item) {
        buf.push(item);
        item = stream.read();
      }
    });
  }
};