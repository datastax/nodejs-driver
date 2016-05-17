"use strict";
var assert = require('assert');
var util = require('util');
var events = require('events');

var Client = require('../../lib/client.js');
var clientOptions = require('../../lib/client-options.js');
var types = require('../../lib/types');
var dataTypes = types.dataTypes;
var loadBalancing = require('../../lib/policies/load-balancing.js');
var retry = require('../../lib/policies/retry.js');
var Encoder = require('../../lib/encoder');
var utils = require('../../lib/utils.js');
var writers = require('../../lib/writers');
var helper = require('../test-helper.js');

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
  describe('Integer', function () {
    var Integer = types.Integer;
    var values = [
      //hex value                      |      string varint
      ['02000001',                            '33554433'],
      ['02000000',                            '33554432'],
      ['1111111111111111',                    '1229782938247303441'],
      ['01',                                  '1'],
      ['0400',                                '1024'],
      ['7fffffff',                            '2147483647'],
      ['02000000000001',                      '562949953421313'],
      ['ff',                                  '-1'],
      ['ff01',                                '-255'],
      ['faa8c4',                              '-350012'],
      ['eb233d9f',                            '-350012001'],
      ['f7d9c411c4',                          '-35001200188'],
      ['f0bdc0',                              '-1000000'],
      ['ff172b5aeff4',                        '-1000000000012'],
      ['9c',                                  '-100'],
      ['c31e',                                '-15586'],
      ['00c31e',                              '49950'],
      ['0500e3c2cef9eaaab3',                  '92297829382473034419'],
      ['033171cbe0fac2d665b78d4e',            '988229782938247303441911118'],
      ['fcce8e341f053d299a4872b2',            '-988229782938247303441911118'],
      ['00b70cefb9c19c9c5112972fd01a4e676d',  '243315893003967298069149506221212854125'],
      ['00ba0cef',                            '12193007'],
      ['00ffffffff',                          '4294967295']
    ];
    it('should create from buffer', function () {
      values.forEach(function (item) {
        var buffer = new Buffer(item[0], 'hex');
        var value = Integer.fromBuffer(buffer);
        assert.strictEqual(value.toString(), item[1]);
      });
    });
    it('should convert to buffer', function () {
      values.forEach(function (item) {
        var buffer = Integer.toBuffer(Integer.fromString(item[1]));
        assert.strictEqual(buffer.toString('hex'), item[0]);
      });
    });
  });
  describe('Tuple', function () {
    var Tuple = types.Tuple;
    describe('#get()', function () {
      it('should return the element at position', function () {
        var t = new Tuple('first', 'second');
        assert.strictEqual(t.get(0), 'first');
        assert.strictEqual(t.get(1), 'second');
        assert.strictEqual(t.get(2), undefined);
        assert.strictEqual(t.length, 2);
      });
    });
    describe('#toString()', function () {
      it('should return the string of the elements surrounded by parenthesis', function () {
        var id = types.Uuid.random();
        var decimal = types.BigDecimal.fromString('1.2');
        var t = new Tuple(id, decimal, 0);
        assert.strictEqual(t.toString(), '(' + id.toString() + ',' + decimal.toString() + ',0)');
      });
    });
    describe('#toJSON()', function () {
      it('should return the string of the elements surrounded by square brackets', function () {
        var id = types.TimeUuid.now();
        var decimal = types.BigDecimal.fromString('-1');
        var t = new Tuple(id, decimal, 1, {z: 1});
        assert.strictEqual(JSON.stringify(t), '["' + id.toString() + '","' + decimal.toString() + '",1,{"z":1}]');
      });
    });
    describe('#values()', function () {
      it('should return the Array representation of the Tuple', function () {
        var t = new Tuple('first2', 'second2', 'third2');
        assert.strictEqual(t.length, 3);
        var values = t.values();
        assert.ok(util.isArray(values));
        assert.strictEqual(values.length, 3);
        assert.strictEqual(values[0], 'first2');
        assert.strictEqual(values[1], 'second2');
        assert.strictEqual(values[2], 'third2');
      });
      it('when modifying the returned Array the Tuple should not change its values', function () {
        var t = new Tuple('first3', 'second3', 'third3');
        var values = t.values();
        assert.strictEqual(values.length, 3);
        values[0] = 'whatever';
        values.shift();
        assert.strictEqual(t.get(0), 'first3');
        assert.strictEqual(t.get(1), 'second3');
        assert.strictEqual(t.get(2), 'third3');
      });
    });
  });
  describe('LocalDate', function () {
    var LocalDate = types.LocalDate;
    describe('new LocalDate', function (){
      it('should refuse to create LocalDate from invalid values.', function () {
        assert.throws(function () { new types.LocalDate() }, Error);
        assert.throws(function () { new types.LocalDate(undefined) }, Error);
        // Outside of ES5 Date range.
        assert.throws(function () { new types.LocalDate(-271821, 4, 19) }, Error);
        assert.throws(function () { new types.LocalDate(275760, 9, 14) }, Error);
        // Outside of LocalDate range.
        assert.throws(function () { new types.LocalDate(-2147483649) }, Error);
        assert.throws(function () { new types.LocalDate(2147483648) }, Error);

      });
    });
    describe('#toString()', function () {
      it('should return the string in the form of yyyy-mm-dd', function () {
        assert.strictEqual(new LocalDate(2015, 2, 1).toString(), '2015-02-01');
        assert.strictEqual(new LocalDate(2015, 12, 13).toString(), '2015-12-13');
        assert.strictEqual(new LocalDate(101, 12, 14).toString(), '0101-12-14');
        assert.strictEqual(new LocalDate(-100, 11, 6).toString(), '-0100-11-06');
      });
    });
    describe('#fromBuffer() and #toBuffer()', function () {
      it('should encode and decode a LocalDate', function () {
        var value = new LocalDate(2010, 8, 5);
        var encoded = value.toBuffer();
        var decoded = LocalDate.fromBuffer(encoded);
        assert.strictEqual(decoded.toString(), value.toString());
        assert.ok(decoded.equals(value));
        assert.ok(value.equals(decoded));
      });
    });
    describe('#fromString()', function () {
      it('should parse the string representation as yyyy-mm-dd', function () {
        [
          ['1200-12-30', 1200, 12, 30],
          ['1-1-1', 1, 1, 1],
          ['21-2-1', 21, 2, 1],
          ['-21-2-1', -21, 2, 1],
          ['2010-4-29', 2010, 4, 29],
          ['-199-06-30', -199, 6, 30],
          ['1201-04-03', 1201, 4, 3],
          ['-1201-04-03', -1201, 4, 3],
          ['0-1-1', 0, 1, 1]
        ].forEach(function (item) {
            var value = LocalDate.fromString(item[0]);
            assert.strictEqual(value.year, item[1]);
            assert.strictEqual(value.month, item[2]);
            assert.strictEqual(value.day, item[3]);
        });
      });
      it('should parse the string representation as since epoch days', function () {
        [
          ['0', '1970-01-01'],
          ['1', '1970-01-02'],
          ['2147483647', '2147483647'],
          ['-2147483648', '-2147483648'],
          ['-719162', '0001-01-01']
        ].forEach(function (item) {
            var value = LocalDate.fromString(item[0]);
            assert.strictEqual(value.toString(), item[1]);
        });
      });
      it('should throw when string representation is invalid', function () {
        [
          '',
          '1880-1',
          '1880-1-z',
          undefined,
          null,
          '  '
        ].forEach(function (value) {
            assert.throws(function () {
              LocalDate.fromString(value);
            }, Error, 'For value: ' + value);
        });
      });
    });
  });
  describe('LocalTime', function () {
    var LocalTime = types.LocalTime;
    var Long = types.Long;
    var values = [
      //Long value         |     string representation  |   hour/min/sec/nanos
      ['1000000001',             '00:00:01.000000001',      [0, 0, 1, 1]],
      ['0',                      '00:00:00',                [0, 0, 0, 0]],
      ['3600000006001',          '01:00:00.000006001',      [1, 0, 0, 6001]],
      ['61000000000',            '00:01:01',                [0, 1, 1, 0]],
      ['610000030000',           '00:10:10.00003',          [0, 10, 10, 30000]],
      ['52171800000000',         '14:29:31.8',              [14, 29, 31, 800000000]],
      ['52171800600000',         '14:29:31.8006',           [14, 29, 31, 800600000]]
    ];
    describe('new LocalTime', function () {
      it('should refuse to create LocalTime from invalid values.', function () {
        // Not a long.
        assert.throws(function () { new types.LocalTime(23.0) }, Error);
        // < 0
        assert.throws(function () { new types.LocalTime(types.Long(-1)) }, Error);
        // > maxNanos
        assert.throws(function () { new types.LocalTime(Long.fromString('86400000000000')) }, Error);
      });
    });
    describe('#toString()', function () {
      it('should return the string representation', function () {
        values.forEach(function (item) {
          var val = new LocalTime(Long.fromString(item[0]));
          assert.strictEqual(val.toString(), item[1]);
        });
      });
    });
    describe('#toJSON()', function () {
      it('should return the string representation', function () {
        values.forEach(function (item) {
          var val = new LocalTime(Long.fromString(item[0]));
          assert.strictEqual(val.toString(), item[1]);
        });
      });
    });
    describe('#fromString()', function () {
      it('should parse the string representation', function () {
        values.forEach(function (item) {
          var val = LocalTime.fromString(item[1]);
          assert.ok(new LocalTime(Long.fromString(item[0])).equals(val));
          assert.ok(new LocalTime(Long.fromString(item[0]))
            .getTotalNanoseconds()
            .equals(val.getTotalNanoseconds()));
        });
      });
    });
    describe('#toBuffer() and fromBuffer()', function () {
      values.forEach(function (item) {
        var val = new LocalTime(Long.fromString(item[0]));
        var encoded = val.toBuffer();
        var decoded = LocalTime.fromBuffer(encoded);
        assert.ok(decoded.equals(val));
        assert.strictEqual(val.toString(), decoded.toString());
      });
    });
    describe('#hour #minute #second #nanosecond', function () {
      it('should get the correct parts', function () {
        values.forEach(function (item) {
          var val = new LocalTime(Long.fromString(item[0]));
          var parts = item[2];
          assert.strictEqual(val.hour, parts[0]);
          assert.strictEqual(val.minute, parts[1]);
          assert.strictEqual(val.second, parts[2]);
          assert.strictEqual(val.nanosecond, parts[3]);
        });
      });
    });
    describe('fromDate()', function () {
      it('should use the local time', function () {
        var date = new Date();
        var time = LocalTime.fromDate(date, 1);
        assert.strictEqual(time.hour, date.getHours());
        assert.strictEqual(time.minute, date.getMinutes());
        assert.strictEqual(time.second, date.getSeconds());
        assert.strictEqual(time.nanosecond, date.getMilliseconds() * 1000000 + 1);
      });
    });
    describe('fromMilliseconds', function () {
      it('should default nanoseconds to 0 when not provided', function () {
        var time = LocalTime.fromMilliseconds(1);
        assert.ok(time.equals(LocalTime.fromMilliseconds(1, 0)))
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
      var columns = [{name: 'first', type: { code: dataTypes.varchar}}, {name: 'second', type: { code: dataTypes.varchar}}];
      var row = new types.Row(columns);
      row['first'] = 'hello';
      row['second'] = 'world';
      assert.ok(row.get, 'It should contain a get method');
      assert.strictEqual(row['first'], 'hello');
      assert.strictEqual(row.get('first'), row['first']);
      assert.strictEqual(row.get(0), row['first']);
      assert.strictEqual(row.get('second'), row['second']);
      assert.strictEqual(row.get(1), row['second']);
    });
    it('should enumerate only columns defined', function () {
      var columns = [{name: 'col1', type: { code: dataTypes.varchar}}, {name: 'col2', type: { code: dataTypes.varchar}}];
      var row = new types.Row(columns);
      row['col1'] = 'val1';
      row['col2'] = 'val2';
      assert.strictEqual(JSON.stringify(row), JSON.stringify({col1: 'val1', col2: 'val2'}));
    });
    it('should be serializable to json', function () {
      var i;
      var columns = [{name: 'col1', type: { code: dataTypes.varchar}}, {name: 'col2', type: { code: dataTypes.varchar}}];
      var row = new types.Row(columns, [new Buffer('val1'), new Buffer('val2')]);
      row['col1'] = 'val1';
      row['col2'] = 'val2';
      assert.strictEqual(JSON.stringify(row), JSON.stringify({col1: 'val1', col2: 'val2'}));

      columns = [
        {name: 'cid', type: { code: dataTypes.uuid}},
        {name: 'ctid', type: { code: dataTypes.timeuuid}},
        {name: 'clong', type: { code: dataTypes.bigint}},
        {name: 'cvarint', type: { code: dataTypes.varint}}
      ];
      var rowValues = [
        types.Uuid.random(),
        types.TimeUuid.now(),
        types.Long.fromNumber(1000),
        types.Integer.fromNumber(22)
      ];
      row = new types.Row(columns);
      for (i = 0; i < columns.length; i++) {
        row[columns[i].name] = rowValues[i];
      }
      var expected = util.format('{"cid":"%s","ctid":"%s","clong":"1000","cvarint":"22"}',
        rowValues[0].toString(), rowValues[1].toString());
      assert.strictEqual(JSON.stringify(row), expected);
      rowValues = [
        types.BigDecimal.fromString("1.762"),
        new types.InetAddress(new Buffer([192, 168, 0, 1])),
        null];
      columns = [
        {name: 'cdecimal', type: { code: dataTypes.decimal}},
        {name: 'inet1', type: { code: dataTypes.inet}},
        {name: 'inet2', type: { code: dataTypes.inet}}
      ];
      row = new types.Row(columns);
      for (i = 0; i < columns.length; i++) {
        row[columns[i].name] = rowValues[i];
      }
      expected = '{"cdecimal":"1.762","inet1":"192.168.0.1","inet2":null}';
      assert.strictEqual(JSON.stringify(row), expected);
    });
    it('should have values that can be inspected', function () {
      var columns = [{name: 'col10', type: { code: dataTypes.varchar}}, {name: 'col2', type: { code: dataTypes.int}}];
      var row = new types.Row(columns);
      row['col10'] = 'val1';
      row['col2'] = 2;
      helper.assertContains(util.inspect(row), util.inspect({col10: 'val1', col2: 2}));
    });
  });
  describe('uuid() backward-compatibility', function () {
    it('should generate a random string uuid', function () {
      var uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      var val = types.uuid();
      assert.strictEqual(typeof val, 'string');
      assert.strictEqual(val.length, 36);
      assert.ok(uuidRegex.test(val));
      assert.notEqual(val, types.uuid());
    });
    it('should fill in the values in a buffer', function () {
      var buf = new Buffer(16);
      var val = types.uuid(null, buf);
      assert.strictEqual(val, buf);
    });
  });
  describe('timeuuid() backward-compatibility', function () {
    it('should generate a string uuid', function () {
      var uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      var val = types.timeuuid();
      assert.strictEqual(typeof val, 'string');
      assert.strictEqual(val.length, 36);
      assert.ok(uuidRegex.test(val));
      assert.notEqual(val, types.timeuuid());
    });
    it('should fill in the values in a buffer', function () {
      var buf = new Buffer(16);
      var val = types.timeuuid(null, buf);
      assert.strictEqual(val, buf);
    });
  });
  describe('generateTimestamp()', function () {
    it('should generate using date and microseconds parts', function () {
      var date = new Date();
      var value = types.generateTimestamp(date, 123);
      helper.assertInstanceOf(value, types.Long);
      assert.strictEqual(value.toString(), types.Long
        .fromNumber(date.getTime())
        .multiply(types.Long.fromInt(1000))
        .add(types.Long.fromInt(123))
        .toString());

      date = new Date('2010-04-29');
      value = types.generateTimestamp(date, 898);
      helper.assertInstanceOf(value, types.Long);
      assert.strictEqual(value.toString(), types.Long
        .fromNumber(date.getTime())
        .multiply(types.Long.fromInt(1000))
        .add(types.Long.fromInt(898))
        .toString());
    });
  });
});
describe('utils', function () {
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
      var o = undefined;
      value = utils.deepExtend({z: 4}, o);
      assert.strictEqual(value.z, 4);
    });
  });
});
describe('clientOptions', function () {
  describe('#extend()', function () {
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
            loadBalancing: new (function C1() {})()
          }
        });
      });
      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          policies: {
            //Use whatever object
            retry: new (function C2() {})()
          }
        });
      });
    });
    it('should validate the encoding options', function () {
      function DummyConstructor() {}
      assert.doesNotThrow(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          encoding: {}
        });
      });
      assert.doesNotThrow(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          encoding: { map: helper.Map}
        });
      });
      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          encoding: { map: 1}
        });
      });
      assert.throws(function () {
        clientOptions.extend({
          contactPoints: ['host1'],
          encoding: { map: DummyConstructor}
        });
      });
    });
  });
  describe('#defaultOptions()', function () {
    var options = clientOptions.defaultOptions();
    it('should set LOCAL_QUORUM as default consistency level', function () {
      assert.strictEqual(types.consistencies.localOne, options.queryOptions.consistency);
    });
    it('should set 12secs as default read timeout', function () {
      assert.strictEqual(12000, options.socketOptions.readTimeout);
    });
    it('should set useUndefinedAsUnset as true', function () {
      assert.strictEqual(true, options.encoding.useUndefinedAsUnset);
    });
  });
});
describe('writers', function () {
  describe('WriteQueue', function () {
    it('should buffer until threshold is passed', function (done) {
      var itemCallbackCounter = 0;
      var buffers = [];
      var socketMock = {
        write: function (buf, cb) {
          buffers.push(buf);
          setTimeout(cb, 50);
        }
      };
      var options = utils.extend({}, clientOptions.defaultOptions());
      options.socketOptions.coalescingThreshold = 50;
      var encoder = new Encoder(3, options);
      //noinspection JSCheckFunctionSignatures
      var queue = new writers.WriteQueue(socketMock, encoder, options);
      var request = {
        write: function () {
          return new Buffer(10);
        }
      };
      function itemCallback() {
        itemCallbackCounter++;
      }
      for (var i = 0; i < 10; i++) {
        //noinspection JSCheckFunctionSignatures
        queue.push(request, itemCallback);
      }
      setTimeout(function () {
        //10 frames
        assert.strictEqual(itemCallbackCounter, 10);
        assert.strictEqual(buffers.length, 3);
        //first part is only 1 message
        assert.strictEqual(buffers[0].length, 10);
        //second part contains 5 messages
        assert.strictEqual(buffers[1].length, 50);
        //second part contains 4 messages
        assert.strictEqual(buffers[2].length, 40);
        done();
      }, 500);
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
    assert.ok(typeof api.errors.DriverError, 'function');
    assert.ok(api.types);
    assert.ok(api.policies);
    assert.ok(api.auth);
    assert.ok(typeof api.auth.AuthProvider, 'function');
    //policies modules
    assert.strictEqual(api.policies.loadBalancing, loadBalancing);
    assert.strictEqual(typeof api.policies.loadBalancing.LoadBalancingPolicy, 'function');
    helper.assertInstanceOf(api.policies.defaultLoadBalancingPolicy(), api.policies.loadBalancing.LoadBalancingPolicy);
    assert.strictEqual(api.policies.retry, retry);
    assert.strictEqual(typeof api.policies.retry.RetryPolicy, 'function');
    helper.assertInstanceOf(api.policies.defaultRetryPolicy(), api.policies.retry.RetryPolicy);
    assert.strictEqual(api.policies.reconnection, require('../../lib/policies/reconnection'));
    assert.strictEqual(typeof api.policies.reconnection.ReconnectionPolicy, 'function');
    helper.assertInstanceOf(api.policies.defaultReconnectionPolicy(), api.policies.reconnection.ReconnectionPolicy);
    assert.strictEqual(api.auth, require('../../lib/auth'));
    //metadata module with classes
    assert.ok(api.metadata);
    assert.strictEqual(typeof api.metadata.Metadata, 'function');
    assert.strictEqual(api.metadata.Metadata, require('../../lib/metadata'));
    assert.ok(api.Encoder);
    assert.strictEqual(typeof api.Encoder, 'function');
    assert.strictEqual(api.Encoder, require('../../lib/encoder'));
    assert.ok(api.defaultOptions());
  });
});