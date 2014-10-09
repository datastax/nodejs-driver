var assert = require('assert');
var util = require('util');
var events = require('events');
var uuid = require('node-uuid');
var async = require('async');
var utils = require('../../lib/utils.js');

var Client = require('../../lib/client.js');
var clientOptions = require('../../lib/client-options.js');
var types = require('../../lib/types.js');
var encoder = require('../../lib/encoder.js');
var dataTypes = types.dataTypes;
var loadBalancing = require('../../lib/policies/load-balancing.js');
var retry = require('../../lib/policies/retry.js');
var helper = require('../test-helper.js')

describe('encoder', function () {
  describe('#guessDataType()', function () {
    it('should guess the native types', function () {
      assertGuessed(1, dataTypes.int, 'Guess type for an integer (double) number failed');
      assertGuessed(1.01, dataTypes.double, 'Guess type for a double number failed');
      assertGuessed(true, dataTypes.boolean, 'Guess type for a boolean value failed');
      assertGuessed([1,2,3], dataTypes.list, 'Guess type for an Array value failed');
      assertGuessed('a string', dataTypes.text, 'Guess type for an string value failed');
      assertGuessed(new Buffer('bip bop'), dataTypes.blob, 'Guess type for a buffer value failed');
      assertGuessed(new Date(), dataTypes.timestamp, 'Guess type for a Date value failed');
      assertGuessed(new types.Long(10), dataTypes.bigint, 'Guess type for a Int 64 value failed');
      assertGuessed(types.uuid(), dataTypes.uuid, 'Guess type for a UUID value failed');
      assertGuessed(types.timeuuid(), dataTypes.uuid, 'Guess type for a Timeuuid value failed');
      assertGuessed({}, null, 'Objects must not be guessed');
    });

    function assertGuessed(value, expectedType, message) {
      var typeInfo = encoder.guessDataType(value);
      if (typeInfo === null) {
        if (expectedType !== null) {
          assert.ok(false, 'Type not guessed for value ' + value);
        }
        return;
      }
      assert.strictEqual(typeInfo.type, expectedType, message + ': ' + value);
    }
  });
  describe('#encode() and #decode', function () {
    var typeEncoder = encoder;
    it('should encode and decode maps', function () {
      var value = {value1: 'Surprise', value2: 'Madafaka'};
      //Minimum info, guessed
      var encoded = typeEncoder.encode(value, dataTypes.map);
      var decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.text]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //Minimum info, guessed
      value = {value1: 1.1, valueN: 1.2};
      encoded = typeEncoder.encode(value, dataTypes.map);
      decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.double]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //Minimum info string, guessed
      value = {value1: new Date(9999999), valueN: new Date(5555555)};
      encoded = typeEncoder.encode(value, 'map');
      decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.timestamp]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //Minimum info string, guessed
      value = {};
      value[types.uuid()] = 0;
      value[types.uuid()] = 2;
      encoded = typeEncoder.encode(value, 'map');
      decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.uuid], [dataTypes.int]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //full info string
      value = {value1: 1, valueN: -3};
      encoded = typeEncoder.encode(value, 'map<text,int>');
      decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.int]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //full info typeInfo
      value = {value1: 1, valueN: -33892};
      encoded = typeEncoder.encode(value, {type: dataTypes.map, subtypes: [dataTypes.string, dataTypes.int]});
      decoded = typeEncoder.decode(encoded, [dataTypes.map, [[dataTypes.text], [dataTypes.int]]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode and decode list<int>', function () {
      var value = [1, 2, 3, 4];
      var encoded = typeEncoder.encode(value, 'list<int>');
      var decoded = typeEncoder.decode(encoded, [dataTypes.list, [dataTypes.int]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode and decode a guessed double', function () {
      var value = 1111.1;
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, [dataTypes.double]);
      assert.strictEqual(decoded, value);
    });

    it('should encode and decode a guessed string', function () {
      var value = 'Pennsatucky';
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, [dataTypes.text]);
      assert.strictEqual(decoded, value);
    });

    it('should encode and decode a guessed int', function () {
      var value = 1;
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, [dataTypes.int]);
      assert.strictEqual(decoded, value);
    });

    it('should encode and decode list<double>', function () {
      var value = [1.1, 2.1, 3.1, 100.1];
      var encoded = typeEncoder.encode(value, 'list<double>');
      var decoded = typeEncoder.decode(encoded, [dataTypes.list, [dataTypes.double]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode and decode list<double> without hint', function () {
      var value = [1.1, 2.1, 3.1, 100.1];
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, [dataTypes.list, [dataTypes.double]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode and decode set<text>', function () {
      var value = ['Alex Vause', 'Piper Chapman', '3', '4'];
      var encoded = typeEncoder.encode(value, 'set<text>');
      var decoded = typeEncoder.decode(encoded, [dataTypes.set, [dataTypes.text]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
      //with type info
      encoded = typeEncoder.encode(value, {type: dataTypes.set, subtypes: [dataTypes.text]});
      decoded = typeEncoder.decode(encoded, [dataTypes.set, [dataTypes.text]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode and decode list<float> with typeInfo', function () {
      var value = [1.1122000217437744, 2.212209939956665, 3.3999900817871094, 4.412120819091797, -1000, 1];
      var encoded = typeEncoder.encode(value, {type: dataTypes.list, subtypes: [dataTypes.float]});
      var decoded = typeEncoder.decode(encoded, [dataTypes.list, [dataTypes.float]]);
      assert.strictEqual(util.inspect(decoded), util.inspect(value));
    });

    it('should encode undefined as null', function () {
      var hinted = typeEncoder.encode(undefined, 'set<text>');
      var unHinted = typeEncoder.encode();
      assert.strictEqual(hinted, null);
      assert.strictEqual(unHinted, null);
    });

    it('should throw on unknown types', function () {
      assert.throws(function () {
        typeEncoder.encode({});
      }, TypeError);
    });
    it('should throw when the typeInfo and the value source type does not match', function () {
      assert.throws(function () {
        typeEncoder.encode('hello', 'int');
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode('1', 'bigint');
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode('1.1', 'float');
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode(100, dataTypes.uuid);
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode(200, dataTypes.timeuuid);
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode('Its anybody in there? I know that you can hear me', dataTypes.blob);
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode(100, dataTypes.blob);
      }, TypeError);
      assert.throws(function () {
        typeEncoder.encode({}, dataTypes.list);
      }, TypeError);
    })
  });
  describe('#setRoutingKey', function () {
    it('should concat Array of buffers in the correct format',function () {
      var options = {
        routingKey: [new Buffer([1]), new Buffer([2]), new Buffer([3, 3])]
      };
      var initialRoutingKey = options.routingKey.toString('hex');
      encoder.setRoutingKey([1, 'text'], options);
      assert.strictEqual(options.routingKey.toString('hex'), '00010100000102000002030300');

      options = {
        routingKey: [new Buffer([1])]
      };
      encoder.setRoutingKey([1, 'text'], options);
      assert.strictEqual(options.routingKey.toString('hex'), '01');
    });
    it('should not affect Buffer routing keys', function () {
      var options = {
        routingKey: new Buffer([1, 2, 3, 4])
      };
      var initialRoutingKey = options.routingKey.toString('hex');
      encoder.setRoutingKey([1, 'text'], options);
      assert.strictEqual(options.routingKey.toString('hex'), initialRoutingKey);

      options = {
        routingIndexes: [1],
        routingKey: new Buffer([1, 2, 3, 4])
      };
      encoder.setRoutingKey([1, 'text'], options);
      assert.strictEqual(options.routingKey.toString('hex'), initialRoutingKey);
    });
    it('should build routing key based on routingIndexes', function () {
      var options = {
        hints: ['int'],
        routingIndexes: [0]
      };
      encoder.setRoutingKey([1], options);
      assert.strictEqual(options.routingKey.toString('hex'), '00000001');

      options = {
        hints: ['int', 'string', 'int'],
        routingIndexes: [0, 2]
      };
      encoder.setRoutingKey([1, 'yeah', 2], options);
      //length1 + buffer1 + 0 + length2 + buffer2 + 0
      assert.strictEqual(options.routingKey.toString('hex'), '0004' + '00000001' + '00' + '0004' + '00000002' + '00');

      options = {
        //less hints
        hints: ['int'],
        routingIndexes: [0, 2]
      };
      encoder.setRoutingKey([1, 'yeah', new Buffer([1, 1, 1, 1])], options);
      //length1 + buffer1 + 0 + length2 + buffer2 + 0
      assert.strictEqual(options.routingKey.toString('hex'), '0004' + '00000001' + '00' + '0004' + '01010101' + '00');
      options = {
        //no hints
        routingIndexes: [1, 2]
      };
      encoder.setRoutingKey([1, 'yeah', new Buffer([1, 1, 1, 1])], options);
      //length1 + buffer1 + 0 + length2 + buffer2 + 0
      assert.strictEqual(options.routingKey.toString('hex'), '0004' + new Buffer('yeah').toString('hex') + '00' + '0004' + '01010101' + '00');
    });
    it('should throw if the type could not be encoded', function () {
      assert.throws(function () {
        var options = {
          routingIndexes: [0]
        };
        encoder.setRoutingKey([{a: 1}], options);
      }, TypeError);
      assert.throws(function () {
        var options = {
          hints: ['int'],
          routingIndexes: [0]
        };
        encoder.setRoutingKey(['this is text'], options);
      }, TypeError);
    });
  });
});

describe('types', function () {
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

    it('should return a valid number for int greater than 2^53 and less than -2^53', function () {
      [
        new Long(0, 0x7FFFFFFF),
        new Long(0xFFFFFFFF, 0x7FFFFFFF),
        new Long(0xFFFFFFFF, 0x7FFFFF01)
      ].forEach(function (item) {
        assert.ok(item.toNumber() > Math.pow(2, 53), util.format('Value should be greater than 2^53 for %s', item));
      });
      [
        new Long(0, 0xF0000000),
        new Long(0, 0xF0000001)
      ].forEach(function (item) {
        assert.ok(item.toNumber() < Math.pow(2, 53), util.format('Value should be less than -2^53 for %s', item));
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
        }
      }
      var args = utils.parseCommonArgs('A QUERY 1', function (){});
      assert.ok(args && args.query && args.callback);
      assert.throws(utils.parseCommonArgs, Error, 'It must contain at least 2 arguments.');
      args = utils.parseCommonArgs('A QUERY 2', [1, 2, 3], function (){});
      testArgs(args, 3);
      assert.ok(util.isArray(args.params) && args.params.length === 3);
      args = utils.parseCommonArgs('A QUERY 3', [], function (){});
      testArgs(args, 3);
      assert.ok(util.isArray(args.params), 'Params should be set');
      args = utils.parseCommonArgs('A QUERY', [1, 2, 3], {}, function (){});
      testArgs(args, 4);
      assert.ok(args.params && args.options, 'Params and options must not be null');
    });
  });

  describe('#extend()', function () {
    it('should allow null sources', function () {
      var originalObject = {};
      var extended = utils.extend(originalObject, null);
      assert.strictEqual(originalObject, extended);
    });
  });

  describe('#funcCompare()', function () {
    it('should return a compare function valid for Array#sort', function () {
      var values = [
        {id: 1, getValue : function () { return 100;}},
        {id: 2, getValue : function () { return 3;}},
        {id: 3, getValue : function () { return 1;}}
      ];
      values.sort(utils.funcCompare('getValue'));
      assert.strictEqual(values[0].id, 3);
      assert.strictEqual(values[1].id, 2);
      assert.strictEqual(values[2].id, 1);
    });
  });

  describe('#binarySearch()', function () {
    it('should return the key index if found, or the bitwise compliment of the first larger value', function () {
      var compareFunc = function (a, b) {
        if (a > b) return 1;
        if (a < b) return -1;
        return 0;
      };
      var val;
      val = utils.binarySearch([0, 1, 2, 3, 4], 2, compareFunc);
      assert.strictEqual(val, 2);
      val = utils.binarySearch(['A', 'B', 'C', 'D', 'E'], 'D', compareFunc);
      assert.strictEqual(val, 3);
      val = utils.binarySearch(['A', 'B', 'C', 'D', 'Z'], 'M', compareFunc);
      assert.strictEqual(val, ~4);
      val = utils.binarySearch([0, 1, 2, 3, 4], 2.5, compareFunc);
      assert.strictEqual(val, ~3);
    });
  });

  describe('#deepExtend', function () {
    it('should override only the most inner props', function () {
      var value;
      //single values
      value = utils.deepExtend({}, {a: '1'});
      assert.strictEqual(value.a, '1');
      value = utils.deepExtend({a: '2'}, {a: '1'});
      assert.strictEqual(value.a, '1');
      value = utils.deepExtend({a: new Date()}, {a: new Date(100)});
      assert.strictEqual(value.a.toString(), new Date(100).toString());
      value = utils.deepExtend({a: 2}, {a: 1});
      assert.strictEqual(value.a, 1);
      //composed 1 level
      value = utils.deepExtend({a: { a1: 1, a2: 2}, b: 1000}, {a: {a2: 15}});
      assert.strictEqual(value.a.a2, 15);
      assert.strictEqual(value.a.a1, 1);
      assert.strictEqual(value.b, 1000);
      //composed 2 level
      value = utils.deepExtend({a: { a1: 1, a2: { a21: 10,  a22: 20}}}, {a: {a2: {a21: 11}}, b: { b1: 100, b2: 200}});
      assert.strictEqual(value.a.a2.a21, 11);
      assert.strictEqual(value.a.a2.a22, 20);
      assert.strictEqual(value.a.a1, 1);
      assert.strictEqual(value.b.b1, 100);
      assert.strictEqual(value.b.b2, 200);
      //multiple sources
      value = utils.deepExtend({z: 9}, {a: { a1: 1, a2: { a21: 10,  a22: 20}}}, {a: {a2: {a21: 11}}, b: { b1: 100, b2: 200}});
      assert.strictEqual(value.a.a2.a21, 11);
      assert.strictEqual(value.a.a2.a22, 20);
      assert.strictEqual(value.a.a1, 1);
      assert.strictEqual(value.b.b1, 100);
      assert.strictEqual(value.b.b2, 200);
      assert.strictEqual(value.z, 9);
      //!source
      value = utils.deepExtend({z: 3}, null);
      assert.strictEqual(value.z, 3);
      //undefined
      var o;
      value = utils.deepExtend({z: 4}, o);
      assert.strictEqual(value.z, 4);
    });
  });
});

describe('clientOptions', function () {
  describe('#extend', function () {
    it('should require contactPoints', function () {
      assert.doesNotThrow(function () {
        clientOptions.extend({contactPoints: ['host1', 'host2']});
      });
      assert.throws(function () {
        clientOptions.extend({contactPoints: {}});
      });
      assert.throws(function () {
        clientOptions.extend({});
      });
      assert.throws(function () {
        clientOptions.extend(null);
      });
      assert.throws(function () {
        clientOptions.extend(undefined);
      });
    });
    it('should create a new instance', function () {
      var a = {contactPoints: ['host1']};
      var options = clientOptions.extend(a);
      assert.notStrictEqual(a, options);
      assert.notStrictEqual(options, clientOptions.defaultOptions());
      //it should use baseOptions as source
      var b = {};
      options = clientOptions.extend(b, a);
      //B is the instance source
      assert.strictEqual(b, options);
      //A is the target
      assert.notStrictEqual(a, options);
      assert.notStrictEqual(options, clientOptions.defaultOptions());
    });
    it('should validate the policies', function () {
      var policy1 = new loadBalancing.RoundRobinPolicy();
      var policy2 = new retry.RetryPolicy();
      var options = clientOptions.extend({
        contactPoints: ['host1'],
        policies: {
          loadBalancing: policy1,
          retry: policy2
        }
      });
      assert.strictEqual(options.policies.loadBalancing, policy1);
      assert.strictEqual(options.policies.retry, policy2);

      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          policies: {
            loadBalancing: {}
          }
        });
      });
      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          policies: {
            //Use whatever object
            loadBalancing: new Connection()
          }
        });
      });
      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          policies: {
            //Use whatever object
            retry: new Connection()
          }
        });
      });
    });
  });
});

describe('exports', function () {
  it('should contain API', function () {
    //test that the exposed API is the one expected
    //it looks like a dumb test and it is, but it is necessary!
    var api = require('../../index.js');
    assert.strictEqual(api.Client, Client);
    assert.ok(api.errors);
    assert.ok(api.types);
    assert.ok(api.policies);
    assert.ok(api.auth);
    assert.strictEqual(api.policies.loadBalancing, loadBalancing);
    assert.strictEqual(api.policies.retry, retry);
    assert.strictEqual(api.policies.reconnection, require('../../lib/policies/reconnection.js'));
    assert.strictEqual(api.auth, require('../../lib/auth'));
  });
});
