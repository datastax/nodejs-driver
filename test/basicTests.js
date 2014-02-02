var assert = require('assert');
var util = require('util');
var events = require('events');
var uuid = require('node-uuid');
var async = require('async');
var utils = require('../lib/utils.js');
var types = require('../lib/types.js');
var encoder = require('../lib/encoder.js');
var config = require('./config.js');
var dataTypes = types.dataTypes;
var Connection = require('../index.js').Connection;

before(function (done) {
  this.timeout(5000);
  var con = new Connection(utils.extend({}, config));
  async.series([
    function (next) {
      con.open(next);
    },
    function (next) {
      con.execute('select cql_version, native_protocol_version, release_version from system.local', function (err, result) {
        if (!err && result && result.rows) {
          console.log();
          console.log('Cassandra version', result.rows[0].get('release_version'));
          console.log('Cassandra higher protocol version', result.rows[0].get('native_protocol_version'), '\n');
        }
        next();
      });
    },
    con.close.bind(con)], done);
});

describe('encoder', function () {
  describe('#guessDataType()', function () {
    it('should guess the native types', function () {
      var guessDataType = encoder.guessDataType;
      assert.strictEqual(guessDataType(1), dataTypes.int, 'Guess type for an integer number failed');
      assert.strictEqual(guessDataType(1.01), dataTypes.double, 'Guess type for a double number failed');
      assert.strictEqual(guessDataType(true), dataTypes.boolean, 'Guess type for a boolean value failed');
      assert.strictEqual(guessDataType([1,2,3]), dataTypes.list, 'Guess type for an Array value failed');
      assert.strictEqual(guessDataType('a string'), dataTypes.text, 'Guess type for an string value failed');
      assert.strictEqual(guessDataType(new Buffer('bip bop')), dataTypes.blob, 'Guess type for a buffer value failed');
      assert.strictEqual(guessDataType(new Date()), dataTypes.timestamp, 'Guess type for a Date value failed');
      assert.strictEqual(guessDataType(new types.Long(10)), dataTypes.bigint, 'Guess type for a Int 64 value failed');
      assert.strictEqual(guessDataType(uuid.v4()), dataTypes.uuid, 'Guess type for a UUID value failed');
      assert.strictEqual(guessDataType(types.uuid()), dataTypes.uuid, 'Guess type for a UUID value failed');
      assert.strictEqual(guessDataType(types.timeuuid()), dataTypes.uuid, 'Guess type for a Timeuuid value failed');
    });
  });

  describe('#encode() and #decode', function () {
    var typeEncoder = encoder;
    it('should encode and decode maps', function () {
      var value = {value1: 'Surprise', value2: 'Madafaka'};
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

describe('types', function () {
  describe('queryParser', function () {
    it('should replace placeholders', function () {
      var parse = types.queryParser.parse;
      assert.strictEqual(parse("SELECT ?", ['123']), "SELECT 123");
      assert.strictEqual(parse("A = 'SCIENCE?' AND KEY = ?", ['2']), "A = 'SCIENCE?' AND KEY = 2");
      assert.strictEqual(parse("key0=? key1 = 'SCIENCE?' AND KEY=?", ['1', '2']), "key0=1 key1 = 'SCIENCE?' AND KEY=2");
      assert.strictEqual(parse("keyA=? AND keyB=? AND keyC=?", ['1', '2', '3']), "keyA=1 AND keyB=2 AND keyC=3");
      //replace in the middle
      assert.strictEqual(parse("key=? AND key2='value'", null), "key=? AND key2='value'");
      //Nothing to replace here
      assert.strictEqual(parse("SELECT", []), "SELECT");
      assert.strictEqual(parse("SELECT", null), "SELECT");
    });
  });

  describe('Long', function () {
    var Long = types.Long;
    it('should convert from and to Buffer', function () {
      [
       //int64 decimal value    //hex value
        ['-123456789012345678', 'fe4964b459cf0cb2'],
        ['-800000000000000000', 'f4e5d43d13b00000'],
        ['-888888888888888888', 'f3aa0843dcfc71c8'],
        ['-555555555555555555', 'f84a452a6a1dc71d'],
        ['-789456',             'fffffffffff3f430'],
        ['-911111111111111144', 'f35b15458f4f8e18'],
        ['-9007199254740993',   'ffdfffffffffffff'],
        ['-1125899906842624',   'fffc000000000000'],
        ['555555555555555555',  '07b5bad595e238e3'],
        ['789456'            ,  '00000000000c0bd0'],
        ['888888888888888888',  '0c55f7bc23038e38']
      ].forEach(function (item) {
        var buffer = new Buffer(item[1], 'hex');
        var value = Long.fromBuffer(buffer);
        assert.strictEqual(value.toString(), item[0]);
        assert.strictEqual(Long.toBuffer(value).toString('hex'), buffer.toString('hex'),
          'Hexadecimal values should match for ' + item[1]);
      });
    });
  });

  describe('ResultStream', function () {
    it('should be readable as soon as it has data', function (done) {
      var buf = [];
      var stream = new types.ResultStream();
      
      stream.on('end', function streamEnd() {
        assert.equal(Buffer.concat(buf).toString(), 'Jimmy McNulty');
        done();
      });
      stream.on('readable', function streamReadable() {
        var item;
        while (item = stream.read()) {
          buf.push(item);
        }
      });
      
      stream.add(new Buffer('Jimmy'));
      stream.add(new Buffer(' '));
      stream.add(new Buffer('McNulty'));
      stream.add(null);
    });

    it('should buffer until is read', function (done) {
      var buf = [];
      var stream = new types.ResultStream();
      stream.add(new Buffer('Stringer'));
      stream.add(new Buffer(' '));
      stream.add(new Buffer('Bell'));
      stream.add(null);

      stream.on('end', function streamEnd() {
        assert.equal(Buffer.concat(buf).toString(), 'Stringer Bell');
        done();
      });
      stream.on('readable', function streamReadable() {
        var item;
        while (item = stream.read()) {
          buf.push(item);
        }
      });
    });

    it('should be readable until the end', function (done) {
      var buf = [];
      var stream = new types.ResultStream();
      stream.add(new Buffer('Omar'));
      stream.add(new Buffer(' '));

      stream.on('end', function streamEnd() {
        assert.equal(Buffer.concat(buf).toString(), 'Omar Little');
        done();
      });
      stream.on('readable', function streamReadable() {
        var item;
        while (item = stream.read()) {
          buf.push(item);
        }
      });

      stream.add(new Buffer('Little'));
      stream.add(null);
    });

    it('should be readable on objectMode', function (done) {
      var buf = [];
      var stream = new types.ResultStream({objectMode: true});
      //passing objects
      stream.add({toString: function (){return 'One'}});
      stream.add({toString: function (){return 'Two'}});
      stream.add(null);
      stream.on('end', function streamEnd() {
        assert.equal(buf.join(' '), 'One Two');
        done();
      });
      stream.on('readable', function streamReadable() {
        var item;
        while (item = stream.read()) {
          buf.push(item.toString());
        }
      });
    });
  });

  describe('Row', function () {
    it('should get the value by column name or index', function () {
      var columnList = [{name: 'first'}, {name: 'second'}];
      var row = new types.Row(columnList);
      row['first'] = 'value1';
      row['second'] = 'value2';

      assert.ok(row.get, 'It should contain a get method');
      assert.strictEqual(row.get('first'), row['first']);
      assert.strictEqual(row.get(0), row['first']);
      assert.strictEqual(row.get('second'), row['second']);
      assert.strictEqual(row.get(1), row['second']);
    })
  })
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