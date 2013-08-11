var queryParser = require('../lib/utils.js').queryParser;
var types = require('../lib/types.js');
var util = require('util');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;
var uuid = require('node-uuid');

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
    test.ok(queryParser.encodeParam(1) === '1', 'Encodeparam for number failed');
    test.ok(queryParser.encodeParam(1.1) === '1.1', 'Encodeparam for number failed');
    test.ok(queryParser.encodeParam('text') === '\'text\'', 'Encodeparam for string failed');
    test.ok(queryParser.encodeParam("needs escapin' text") === "'needs escapin'' text'", 'Encodeparam for escaped string failed');
    test.ok(queryParser.encodeParam(null) === 'null', 'Encodeparam for null failed:' + queryParser.encodeParam(null));
    test.ok(queryParser.encodeParam([1,2,3]) === '[1,2,3]', 'Encodeparam for array failed');
    test.ok(queryParser.encodeParam([]) === '[]', 'Encodeparam for array failed: ' + queryParser.encodeParam([]));
    test.ok(queryParser.encodeParam(['one', 'two']) === '[\'one\',\'two\']', 'Encodeparam for list failed');
    test.ok(queryParser.encodeParam({value:['one', 'two'],hint:'set'}) === '{\'one\',\'two\'}', 'Encodeparam for set failed');
    test.ok(queryParser.encodeParam({value:{key1:'value1', key2:'value2'},hint:'map'}) === '{\'key1\':\'value1\',\'key2\':\'value2\'}', 'Encodeparam for map failed');
    test.ok(queryParser.encodeParam(new Int64('56789abcdef0123')).indexOf('056789abcdef0123') >= 0, 'Encodeparam for Int64 failed');
    test.ok(queryParser.encodeParam(new Date(2013,6,2, 04, 30, 05)) === '1372753805000', 'Encodeparam for Date failed: ' + queryParser.encodeParam(new Date(2013,6,2, 04, 30, 05)));
    test.done();
  },
  UUIDQueryTest: function (test){
    var sampleUUIDQuery = 'SELECT * FROM somekeyspace WHERE some_uuid = ?';
    var sampleUUID = 'd216de0b-dd70-4148-9a30-aaad53518fb2';
    var expected = sampleUUIDQuery.replace("?", sampleUUID); // Make sure we get an unquoted string here
    var actual = queryParser.parse('SELECT * FROM somekeyspace WHERE some_uuid = ?', [sampleUUID]);
    test.equal(actual, expected, 'Query with UUID failed:' + actual);
    test.done();
  },
  collectionTests: function(test){
    // Setup some test data
    var iAmASet = {0: "roomba", 1: "cats", 2:"dogs", 3:"snakes", 4:"rats"};
    var iAmAList = ["cats", "dogs", "snakes", "rats"];
    var iAmAMap = {"pet": "cat", "food": "yep", "water": "that too"};
    var iAmAUUIDKeyedMap = {};
    var key1 = uuid.v4();
    var key2 = uuid.v4();
    var key3 = uuid.v4();
    iAmAUUIDKeyedMap[key1] = "cat";
    iAmAUUIDKeyedMap[key2] = "dog";
    iAmAUUIDKeyedMap[key3] = "snake";
    var testUUID = uuid.v4();
    var testQuery = "INSERT INTO collectionTests (test_uuid, textTextMapField, UUIDTextMapField, textListField, textSetField) VALUES (?, ?, ?, ?, ?)";
    var testVals = [testUUID, iAmAMap, iAmAUUIDKeyedMap, iAmAList, iAmASet];
    var expected = "INSERT INTO collectionTests (test_uuid, textTextMapField, UUIDTextMapField, textListField, textSetField) VALUES ("+testUUID+", {'pet':'cat','food':'yep','water':'that too'}, {"+key1+":'cat',"+key2+":'dog',"+key3+":'snake'}, ['cats','dogs','snakes','rats'], {'roomba','cats','dogs','snakes','rats'})";
    var actual = queryParser.parse(testQuery, testVals);
    test.equal(actual, expected, 'Collection tests failed to generate valid CQL:' + actual);
    // TODO actual create the table, run the CQL-INSERT, SELECT it back, and verify
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