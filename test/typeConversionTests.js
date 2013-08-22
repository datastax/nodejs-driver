var queryParser = require('../lib/utils.js').queryParser;
var types = require('../lib/types.js');
var util = require('util');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;
var uuid = require('node-uuid');
var dataTypes = types.dataTypes;

var con = new Connection({host:'localhost', port: 9042, maxRequests:32});
var keyspace = 'unittestkp1_conversionTests';
var testTable = "CREATE TABLE collectionTests (test_uuid uuid, textTextMapField map<text, text>, UUIDTextMapField map<uuid, text>, textListField list<text>, textSetField set<text>, PRIMARY KEY (test_uuid));";

module.exports = {
  connect: function (test) {
    helper.connectInit(function () {
      test.done();
    });
  },
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
  /**
   * Executes last, closes the connection
   */
  disconnect : function (test) {
    con.close(function () {
      test.done();
    });
  }
};
var helper = {
  //connects and sets up a keyspace
  connectInit: function (callback) {
    con.open(function (err) {
      if (err) console.log( err );
      con.execute("DROP KEYSPACE "+keyspace+";", [], function(err) {
        if (err) console.log( err );
        con.execute("CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [], function(err) {
          if (err) console.log( err );
          con.execute("USE "+keyspace+";", [], function(err) {
            if (err) console.log( err );
            callback();
          });
        });
      });
    });
  }
};
