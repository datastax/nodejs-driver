var assert = require('assert');
var util = require('util');
var async = require('async');
var utils = require('../../lib/utils');

var Encoder = require('../../lib/encoder');
var types = require('../../lib/types');
var dataTypes = types.dataTypes;
var helper = require('../test-helper');

describe('encoder', function () {
  describe('#guessDataType()', function () {
    var encoder = new Encoder(2, {});
    it('should guess the native types', function () {
      assertGuessed(1, dataTypes.double, 'Guess type for an integer (double) number failed');
      assertGuessed(1.01, dataTypes.double, 'Guess type for a double number failed');
      assertGuessed(true, dataTypes.boolean, 'Guess type for a boolean value failed');
      assertGuessed([1,2,3], dataTypes.list, 'Guess type for an Array value failed');
      assertGuessed('a string', dataTypes.text, 'Guess type for an string value failed');
      assertGuessed(new Buffer('bip bop'), dataTypes.blob, 'Guess type for a buffer value failed');
      assertGuessed(new Date(), dataTypes.timestamp, 'Guess type for a Date value failed');
      assertGuessed(new types.Long(10), dataTypes.bigint, 'Guess type for a Int 64 value failed');
      assertGuessed(types.Uuid.random(), dataTypes.uuid, 'Guess type for a UUID value failed');
      assertGuessed(types.TimeUuid.now(), dataTypes.uuid, 'Guess type for a TimeUuid value failed');
      assertGuessed(types.TimeUuid.now().toString(), dataTypes.uuid, 'Guess type for a string uuid value failed');
      assertGuessed(types.timeuuid(), dataTypes.uuid, 'Guess type for a Timeuuid value failed');
      assertGuessed(types.Integer.fromNumber(1), dataTypes.varint, 'Guess type for a varint value failed');
      assertGuessed(types.BigDecimal.fromString('1.01'), dataTypes.decimal, 'Guess type for a varint value failed');
      assertGuessed(types.Integer.fromBuffer(new Buffer([0xff])), dataTypes.varint, 'Guess type for a varint value failed');
      assertGuessed(new types.InetAddress(new Buffer([10, 10, 10, 2])), dataTypes.inet, 'Guess type for a inet value failed');
      assertGuessed({}, null, 'Objects must not be guessed');
    });

    function assertGuessed(value, expectedType, message) {
      var type = encoder.guessDataType(value);
      if (type === null) {
        if (expectedType !== null) {
          assert.ok(false, 'Type not guessed for value ' + value);
        }
        return;
      }
      assert.strictEqual(type.code, expectedType, message + ': ' + value);
    }
  });
  describe('#encode() and #decode()', function () {
    var typeEncoder = new Encoder(2, {});
    it('should encode and decode a guessed double', function () {
      var value = 1111;
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, {code: dataTypes.double});
      assert.strictEqual(decoded, value);
    });
    it('should encode and decode a guessed string', function () {
      var value = 'Pennsatucky';
      var encoded = typeEncoder.encode(value);
      var decoded = typeEncoder.decode(encoded, {code: dataTypes.text});
      assert.strictEqual(decoded, value);
    });
    it('should encode stringified uuids for backward-compatibility', function () {
      var uuid = types.Uuid.random();
      var encoded = typeEncoder.encode(uuid.toString(), types.dataTypes.uuid);
      assert.strictEqual(encoded.toString('hex'), uuid.getBuffer().toString('hex'));

      uuid = types.TimeUuid.now();
      encoded = typeEncoder.encode(uuid.toString(), types.dataTypes.uuid);
      assert.strictEqual(encoded.toString('hex'), uuid.getBuffer().toString('hex'));
    });
    it('should throw when string is not an uuid', function () {
      assert.throws(function () {
        typeEncoder.encode('', types.dataTypes.uuid);
      }, TypeError);
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
    });
    it('should encode Long/Date/Number/String as timestamps', function () {
      var encoder = new Encoder(2, {});
      var buffer = encoder.encode(types.Long.fromBits(0x00fafafa, 0x07090909), dataTypes.timestamp);
      assert.strictEqual(buffer.toString('hex'), '0709090900fafafa');
      buffer = encoder.encode(1421755130012, dataTypes.timestamp);
      assert.strictEqual(buffer.toString('hex'), '0000014b0735a09c');
      buffer = encoder.encode(new Date(1421755130012), dataTypes.timestamp);
      assert.throws(function () {
        encoder.encode(new Date('This is an invalid date string'), dataTypes.timestamp);
      }, TypeError);
      assert.strictEqual(buffer.toString('hex'), '0000014b0735a09c');
      buffer = encoder.encode(new Date(1421755130012), dataTypes.timestamp);
      assert.strictEqual(buffer.toString('hex'), '0000014b0735a09c');
      buffer = encoder.encode('Tue Jan 20 2015 13:00:35 GMT+0100 (CET)', dataTypes.timestamp);
      assert.strictEqual(buffer.toString('hex'), '0000014b07373ab8');
      assert.throws(function () {
        encoder.encode('This is an invalid date string', dataTypes.timestamp);
      }, TypeError);
    });
    it('should encode String/Number (not NaN) as int', function () {
      var encoder = new Encoder(2, {});
      var buffer = encoder.encode(0x071272ab, dataTypes.int);
      assert.strictEqual(buffer.toString('hex'), '071272ab');
      buffer = encoder.encode('0x071272ab', dataTypes.int);
      assert.strictEqual(buffer.toString('hex'), '071272ab');
      buffer = encoder.encode(-1, 'int');
      assert.strictEqual(buffer.toString('hex'), 'ffffffff');
      buffer = encoder.encode('-1', 'int');
      assert.strictEqual(buffer.toString('hex'), 'ffffffff');
      buffer = encoder.encode(0, 'int');
      assert.strictEqual(buffer.toString('hex'), '00000000');
      buffer = encoder.encode('0', 'int');
      assert.strictEqual(buffer.toString('hex'), '00000000');
      assert.throws(function () {
        encoder.encode(NaN, 'int');
      }, TypeError);
    });
    it('should encode String/Long/Number as bigint', function () {
      var encoder = new Encoder(2, {});
      var buffer = encoder.encode(types.Long.fromString('506946367331695353'), dataTypes.bigint);
      assert.strictEqual(buffer.toString('hex'), '0709090900fafaf9');
      buffer = encoder.encode('506946367331695353', dataTypes.bigint);
      assert.strictEqual(buffer.toString('hex'), '0709090900fafaf9');
      buffer = encoder.encode(0, dataTypes.bigint);
      assert.strictEqual(buffer.toString('hex'), '0000000000000000');
      buffer = encoder.encode(255, dataTypes.bigint);
      assert.strictEqual(buffer.toString('hex'), '00000000000000ff');
    });
    it('should encode String/Integer/Number as varint', function () {
      var encoder = new Encoder(2, {});
      var buffer = encoder.encode(types.Integer.fromString('33554433'), dataTypes.varint);
      assert.strictEqual(buffer.toString('hex'), '02000001');
      buffer = encoder.encode('-150012', dataTypes.varint);
      assert.strictEqual(buffer.toString('hex'), 'fdb604');
      buffer = encoder.encode('-1000000000012', dataTypes.varint);
      assert.strictEqual(buffer.toString('hex'), 'ff172b5aeff4');
      buffer = encoder.encode(-128, dataTypes.varint);
      assert.strictEqual(buffer.toString('hex'), '80');
      buffer = encoder.encode(-100, dataTypes.varint);
      assert.strictEqual(buffer.toString('hex'), '9c');
    });
    it('should encode String/BigDecimal/Number as decimal', function () {
      var encoder = new Encoder(2, {});
      var buffer = encoder.encode(types.BigDecimal.fromString('0.00256'), dataTypes.decimal);
      assert.strictEqual(buffer.toString('hex'), '000000050100');
      buffer = encoder.encode('-0.01', dataTypes.decimal);
      assert.strictEqual(buffer.toString('hex'), '00000002ff');
      buffer = encoder.encode('-25.5', dataTypes.decimal);
      assert.strictEqual(buffer.toString('hex'), '00000001ff01');
      buffer = encoder.encode(0.004, dataTypes.decimal);
      assert.strictEqual(buffer.toString('hex'), '0000000304');
      buffer = encoder.encode(-25.5, dataTypes.decimal);
      assert.strictEqual(buffer.toString('hex'), '00000001ff01');
    });
    it('should encode/decode InetAddress/Buffer as inet', function () {
      var InetAddress = types.InetAddress;
      var encoder = new Encoder(2, {});
      var val1 = new InetAddress(new Buffer([15, 15, 15, 1]));
      var encoded = encoder.encode(val1, dataTypes.inet);
      assert.strictEqual(encoded.toString('hex'), '0f0f0f01');
      var val2 = encoder.decode(encoded, {code: dataTypes.inet});
      assert.strictEqual(val2.toString(), '15.15.15.1');
      assert.ok(val1.equals(val2));
      val1 = new InetAddress(new Buffer('00000000000100112233445500aa00bb', 'hex'));
      encoded = encoder.encode(val1, dataTypes.inet);
      val2 = encoder.decode(encoded, {code: dataTypes.inet});
      assert.ok(val1.equals(val2));
      //Buffers are valid InetAddress
      encoded = encoder.encode(val1.getBuffer(), dataTypes.inet);
      assert.strictEqual(encoded.toString('hex'), val1.getBuffer().toString('hex'));
    });
    it('should decode uuids into Uuid', function () {
      var uuid = types.Uuid.random();
      var decoded = typeEncoder.decode(uuid.getBuffer(), {code: dataTypes.uuid});
      helper.assertInstanceOf(decoded, types.Uuid);
      assert.strictEqual(uuid.toString(), decoded.toString());
      assert.ok(uuid.equals(decoded));
      var decoded2 = typeEncoder.decode(types.Uuid.random().getBuffer(), {code: dataTypes.uuid});
      assert.ok(!decoded.equals(decoded2));
    });
    it('should decode timeuuids into TimeUuid', function () {
      var uuid = types.TimeUuid.now();
      var decoded = typeEncoder.decode(uuid.getBuffer(), {code: dataTypes.timeuuid});
      helper.assertInstanceOf(decoded, types.TimeUuid);
      assert.strictEqual(uuid.toString(), decoded.toString());
      assert.ok(uuid.equals(decoded));
      var decoded2 = typeEncoder.decode(types.TimeUuid.now().getBuffer(), {code: dataTypes.timeuuid});
      assert.ok(!decoded.equals(decoded2));
    });
    [2, 3].forEach(function (version) {
      var encoder = new Encoder(version, {});
      it(util.format('should encode and decode maps for protocol v%d', version), function () {
        var value = {value1: 'Surprise', value2: 'Madafaka'};
        //Minimum info, guessed
        var encoded = encoder.encode(value, dataTypes.map);
        var decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //Minimum info, guessed
        value = {value1: 1.1, valueN: 1.2};
        encoded = encoder.encode(value, dataTypes.map);
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.double}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //Minimum info string, guessed
        value = {value1: new Date(9999999), valueN: new Date(5555555)};
        encoded = encoder.encode(value, 'map');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.timestamp}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //Minimum info string, guessed
        value = {};
        value[types.uuid()] = 0;
        value[types.uuid()] = 2;
        encoded = encoder.encode(value, 'map');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.uuid}, {code: dataTypes.double}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //full info string
        value = {value1: 1, valueN: -3};
        encoded = encoder.encode(value, 'map<text,int>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.int}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //full info typeInfo
        value = {value1: 1, valueN: -33892};
        encoded = encoder.encode(value, {code: dataTypes.map, info: [dataTypes.string, dataTypes.int]});
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.int}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode maps with stringified keys for protocol v%d', version), function () {
        var value = {};
        value[new Date(1421756675488)] = 'date1';
        value[new Date(1411756633461)] = 'date2';

        var encoded = encoder.encode(value, {code: dataTypes.map, info: [{code: dataTypes.timestamp}, {code: dataTypes.text}]});
        var decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.timestamp}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value[101] = 'number1';
        value[102] = 'number2';
        encoded = encoder.encode(value, 'map<int, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.int}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value[types.Long.fromBits(0x12002001, 0x7f999299)] = 'bigint1';
        value[types.Long.fromBits(0x12002000, 0x7f999299)] = 'bigint2';
        encoded = encoder.encode(value, 'map<bigint, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.bigint}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['201'] = 'bigint1_1';
        value['202'] = 'bigint2_1';
        encoded = encoder.encode(value, 'map<bigint, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.bigint}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['2d5db74c-c2da-4e59-b5ec-d8ad3d0aefb9'] = 'uuid1';
        value['651b5c17-5357-4764-ae2d-21c409288822'] = 'uuid2';
        encoded = encoder.encode(value, 'map<uuid, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.uuid}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['1ab50440-a0ab-11e4-9d01-1dc0e727b460'] = 'timeuuid1';
        value['1820c4d0-a0ab-11e4-9d01-1dc0e727b460'] = 'timeuuid2';
        encoded = encoder.encode(value, 'map<timeuuid, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.timeuuid}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['988229782938247303441911118'] = 'varint1';
        value['988229782938247303441911119'] = 'varint2';
        encoded = encoder.encode(value, 'map<varint, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.varint}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['12.1'] = 'decimal1';
        value['12.90'] = 'decimal2';
        encoded = encoder.encode(value, 'map<decimal, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.decimal}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['127.0.0.1'] = 'inet1';
        value['12.10.10.2'] = 'inet2';
        encoded = encoder.encode(value, 'map<inet, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.inet}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));

        value = {};
        value['::1'] = 'inet1';
        value['::2233:0:0:b1'] = 'inet2';
        value['aabb::11:2233:4455:6677:88ff'] = 'inet3';
        encoded = encoder.encode(value, 'map<inet, text>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.inet}, {code: dataTypes.text}]});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode list<int> for protocol v%d', version), function () {
        var value = [1, 2, 3, 4];
        var encoded = encoder.encode(value, 'list<int>');
        var decoded = encoder.decode(encoded, {code: dataTypes.list, info: {code: dataTypes.int}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode list<double> for protocol v%d', version), function () {
        var value = [1, 2, 3, 100];
        var encoded = encoder.encode(value, 'list<double>');
        var decoded = encoder.decode(encoded, {code: dataTypes.list, info: {code: dataTypes.double}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode list<double> without hint for protocol v%d', version), function () {
        var value = [1, 2, 3, 100.1];
        var encoded = encoder.encode(value);
        var decoded = encoder.decode(encoded, {code: dataTypes.list, info: {code: dataTypes.double}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode set<text> for protocol v%d', version), function () {
        var value = ['Alex Vause', 'Piper Chapman', '3', '4'];
        var encoded = encoder.encode(value, 'set<text>');
        var decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.text}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
        //with type info
        encoded = encoder.encode(value, {code: dataTypes.set, info: {code: dataTypes.text}});
        decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.text}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode and decode list<float> with typeInfo for protocol v%d', version), function () {
        var value = [1.1122000217437744, 2.212209939956665, 3.3999900817871094, 4.412120819091797, -1000, 1];
        var encoded = encoder.encode(value, {code: dataTypes.list, info: {code: dataTypes.float}});
        var decoded = encoder.decode(encoded, {code: dataTypes.list, info: {code: dataTypes.float}});
        assert.strictEqual(util.inspect(decoded), util.inspect(value));
      });
      it(util.format('should encode/decode ES6 Set as maps for protocol v%d', version), function () {
        //noinspection JSUnresolvedVariable
        if (typeof Set !== 'function') {
          //Set not supported in Node.js runtime
          return;
        }
        //noinspection JSUnresolvedVariable
        var Es6Set = Set;
        var encoder = new Encoder(version, { encoding: { set: Es6Set}});
        var m = new Es6Set(['k1', 'k2', 'k3']);
        var encoded = encoder.encode(m, 'set<text>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000300026b3100026b3200026b33');
        }
        var decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.text}});
        helper.assertInstanceOf(decoded, Es6Set);
        assert.strictEqual(decoded.toString(), m.toString());

        m = new Es6Set([1, 2, 1000]);
        encoded = encoder.encode(m, 'set<int>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '00030004000000010004000000020004000003e8');
        }
        decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.text}});
        assert.strictEqual(decoded.toString(), m.toString());
      });
      it(util.format('should encode/decode Map polyfills as maps for protocol v%d', version), function () {
        var encoder = new Encoder(version, { encoding: { map: helper.Map}});
        var m = new helper.Map();
        m.set('k1', 'v1');
        m.set('k2', 'v2');
        var encoded = encoder.encode(m, 'map<text,text>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000200026b310002763100026b3200027632');
        }
        var decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.text}]});
        helper.assertInstanceOf(decoded, helper.Map);
        assert.strictEqual(decoded.arr.toString(), m.arr.toString());

        m = new helper.Map();
        m.set('k1', 1);
        m.set('k2', 2);
        m.set('k3', 3);
        encoded = encoder.encode(m, 'map<text,int>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000300026b3100040000000100026b3200040000000200026b33000400000003');
        }
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.int}]});
        assert.strictEqual(decoded.arr.toString(), m.arr.toString());
      });
      it(util.format('should encode/decode ES6 Map as maps for protocol v%d', version), function () {
        //noinspection JSUnresolvedVariable
        if (typeof Map !== 'function') {
          //Running on Node.js version where Ecmascript 6 Maps are not available
          return;
        }
        function getValues(m) {
          var arr = [];
          m.forEach(function (val, key) {
            arr.push([key, val])
          });
          return arr.toString();
        }
        //noinspection JSUnresolvedVariable
        var Es6Map = Map;
        var encoder = new Encoder(version, { encoding: { map: Es6Map}});
        var m = new Es6Map();
        m.set('k1', 'v1');
        m.set('k2', 'v2');
        var encoded = encoder.encode(m, 'map<text,text>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000200026b310002763100026b3200027632');
        }
        var decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.text}]});
        helper.assertInstanceOf(decoded, Es6Map);
        assert.strictEqual(getValues(decoded), getValues(m));

        m = new Es6Map();
        m.set('k1', 1);
        m.set('k2', 2);
        m.set('k3', 3);
        encoded = encoder.encode(m, 'map<text,int>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000300026b3100040000000100026b3200040000000200026b33000400000003');
        }
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.text}, {code: dataTypes.int}]});
        assert.strictEqual(getValues(decoded), getValues(m));

        m = new Es6Map();
        m.set(new Date('2005-08-05'), 10);
        m.set(new Date('2010-04-29'), 2);
        encoded = encoder.encode(m, 'map<timestamp,int>');
        decoded = encoder.decode(encoded, {code: dataTypes.map, info: [{code: dataTypes.timestamp}, {code: dataTypes.int}]});
        assert.strictEqual(getValues(decoded), getValues(m));
      });
      it(util.format('should encode/decode Set polyfills as maps for protocol v%d', version), function () {
        var encoder = new Encoder(version, { encoding: { set: helper.Set}});
        var m = new helper.Set(['k1', 'k2', 'k3']);
        var encoded = encoder.encode(m, 'set<text>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '000300026b3100026b3200026b33');
        }
        var decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.text}});
        helper.assertInstanceOf(decoded, helper.Set);
        assert.strictEqual(decoded.toString(), m.toString());

        m = new helper.Set([1, 2, 1000]);
        encoded = encoder.encode(m, 'set<int>');
        if (version === 2) {
          assert.strictEqual(encoded.toString('hex'), '00030004000000010004000000020004000003e8');
        }
        decoded = encoder.decode(encoded, {code: dataTypes.set, info: {code: dataTypes.int}});
        assert.strictEqual(decoded.toString(), m.toString());
      });
    });
  });
  describe('#setRoutingKey()', function () {
    var encoder = new Encoder(2, {});
    it('should concat Array of buffers in the correct format',function () {
      var options = {
        //Its an array of 3 values
        routingKey: [new Buffer([1]), new Buffer([2]), new Buffer([3, 3])]
      };
      encoder.setRoutingKey([1, 'text'], options);
      //The routingKey should have the form: [2-byte-length] + key + [0]
      assert.strictEqual(options.routingKey.toString('hex'), '00010100' + '00010200' + '0002030300');

      options = {
        //Its an array of 1 value
        routingKey: [new Buffer([1])]
      };
      encoder.setRoutingKey([], options);
      //the result should be a single value
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
      //The routing key should take precedence over routingIndexes
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
  describe('#parseTypeName()', function () {
    it('should parse single type names', function () {
      var encoder = new Encoder(2, {});
      var type = encoder.parseTypeName('org.apache.cassandra.db.marshal.Int32Type');
      assert.strictEqual(dataTypes.int, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.UUIDType');
      assert.strictEqual(dataTypes.uuid, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.UTF8Type');
      assert.strictEqual(dataTypes.varchar, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.BytesType');
      assert.strictEqual(dataTypes.blob, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.FloatType');
      assert.strictEqual(dataTypes.float, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.DoubleType');
      assert.strictEqual(dataTypes.double, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.BooleanType');
      assert.strictEqual(dataTypes.boolean, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.InetAddressType');
      assert.strictEqual(dataTypes.inet, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.DateType');
      assert.strictEqual(dataTypes.timestamp, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.TimestampType');
      assert.strictEqual(dataTypes.timestamp, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.LongType');
      assert.strictEqual(dataTypes.bigint, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.DecimalType');
      assert.strictEqual(dataTypes.decimal, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.IntegerType');
      assert.strictEqual(dataTypes.varint, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.CounterColumnType');
      assert.strictEqual(dataTypes.counter, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.TimeUUIDType');
      assert.strictEqual(dataTypes.timeuuid, type.code);
      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.AsciiType');
      assert.strictEqual(dataTypes.ascii, type.code);
    });
    it('should parse complex type names', function () {
      var encoder = new Encoder(2, {});
      var type = encoder.parseTypeName('org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)');
      assert.strictEqual(dataTypes.list, type.code);
      assert.ok(type.info);
      assert.strictEqual(dataTypes.int, type.info.code);

      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)');
      assert.strictEqual(dataTypes.set, type.code);
      assert.ok(type.info);
      assert.strictEqual(dataTypes.uuid, type.info.code);

      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TimeUUIDType)');
      assert.strictEqual(dataTypes.set, type.code);
      assert.ok(type.info);
      assert.strictEqual(dataTypes.timeuuid, type.info.code);

      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)');
      assert.strictEqual(dataTypes.map, type.code);
      assert.ok(util.isArray(type.info));
      assert.strictEqual(dataTypes.varchar, type.info[0].code);
      assert.strictEqual(dataTypes.bigint, type.info[1].code);
    });
    it('should parse frozen types', function () {
      var encoder = new Encoder(2, {});
      var type = encoder.parseTypeName('org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TimeUUIDType))');
      assert.strictEqual(dataTypes.list, type.code);
      assert.ok(type.info);
      assert.strictEqual(dataTypes.timeuuid, type.info.code);

      type = encoder.parseTypeName('org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))');
      assert.strictEqual(dataTypes.map, type.code);
      assert.ok(util.isArray(type.info));
      assert.strictEqual(dataTypes.varchar, type.info[0].code);
      assert.strictEqual(dataTypes.list, type.info[1].code);
      var subType = type.info[1].info;
      assert.ok(subType);
      assert.strictEqual(dataTypes.int, subType.code);
    });
    it('should parse udt types', function () {
      var encoder = new Encoder(2, {});
      var typeText =
        'org.apache.cassandra.db.marshal.UserType(' +
        'tester,70686f6e65,616c696173:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type' +
        ')';
      var dataType = encoder.parseTypeName(typeText);
      assert.strictEqual(dataTypes.udt, dataType.code);
      //Udt name
      assert.ok(dataType.info);
      assert.strictEqual('phone', dataType.info.name);
      assert.strictEqual(2, dataType.info.fields.length);
      assert.strictEqual('alias', dataType.info.fields[0].name);
      assert.strictEqual(dataTypes.varchar, dataType.info.fields[0].type.code);
      assert.strictEqual('number', dataType.info.fields[1].name);
      assert.strictEqual(dataTypes.varchar, dataType.info.fields[1].type.code);
    });
    it('should parse nested udt types', function () {
      var encoder = new Encoder(2, {});
      var typeText =
        'org.apache.cassandra.db.marshal.UserType(' +
        'tester,' +
        '61646472657373,' +
        '737472656574:org.apache.cassandra.db.marshal.UTF8Type,' +
        '5a4950:org.apache.cassandra.db.marshal.Int32Type,' +
        '70686f6e6573:org.apache.cassandra.db.marshal.SetType(' +
        'org.apache.cassandra.db.marshal.UserType(' +
        'tester,' +
        '70686f6e65,' +
        '616c696173:org.apache.cassandra.db.marshal.UTF8Type,' +
        '6e756d626572:org.apache.cassandra.db.marshal.UTF8Type))' +
        ')';
      var dataType = encoder.parseTypeName(typeText);
      assert.strictEqual(dataTypes.udt, dataType.code);
      assert.strictEqual('address', dataType.info.name);
      assert.strictEqual('tester', dataType.info.keyspace);
      var subTypes = dataType.info.fields;
      assert.strictEqual(3, subTypes.length);
      assert.strictEqual('street,ZIP,phones', subTypes.map(function (f) {return f.name}).join(','));
      assert.strictEqual(dataTypes.varchar, subTypes[0].type.code);
      assert.strictEqual(dataTypes.set, subTypes[2].type.code);
      //field name
      assert.strictEqual('phones', subTypes[2].name);

      var phonesSubType = subTypes[2].type.info;
      assert.strictEqual(dataTypes.udt, phonesSubType.code);
      assert.strictEqual('phone', phonesSubType.info.name);
      assert.strictEqual('tester', phonesSubType.info.keyspace);
      assert.strictEqual(2, phonesSubType.info.fields.length);
      assert.strictEqual('alias', phonesSubType.info.fields[0].name);
      assert.strictEqual('number', phonesSubType.info.fields[1].name);
    });
  });
});