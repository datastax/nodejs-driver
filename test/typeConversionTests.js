var queryParser = require('../lib/utils.js').queryParser;
var types = require('../lib/types.js');
var util = require('util');
var Int64 = require('node-int64');
var Connection = require('../index.js').Connection;

var con = new Connection({host:'localhost', port: 9042, maxRequests:32});
var keyspace = new types.QueryLiteral('unittestkp1_conversionTests');

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
    test.ok(queryParser.encodeParam(null) === 'null', 'Encodeparam for null failed:' + queryParser.encodeParam(null));
    test.ok(queryParser.encodeParam([1,2,3]) === '[1,2,3]', 'Encodeparam for array failed');
    test.ok(queryParser.encodeParam([]) === '[]', 'Encodeparam for array failed: ' + queryParser.encodeParam([]));
    test.ok(queryParser.encodeParam(['one', 'two']) === '[\'one\',\'two\']', 'Encodeparam for list failed');
    test.ok(queryParser.encodeParam({value:['one', 'two'],hint:'set'}) === '{\'one\',\'two\'}', 'Encodeparam for set failed');
    test.ok(queryParser.encodeParam({value:{key1:'value1', key2:'value2'},hint:'map'}) === '{\'key1\':\'value1\',\'key2\':\'value2\'}', 'Encodeparam for map failed');
    test.ok(queryParser.encodeParam(new Int64('56789abcdef0123')).indexOf('056789abcdef0123') >= 0, 'Encodeparam for Int64 failed');
    test.ok(queryParser.encodeParam(new Date(2013,6,2, 04, 30, 05)) === '1372753805000', 'Encodeparam for Date failed: ' + queryParser.encodeParam(new Date(2013,6,2, 10, 30, 05)));
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
      if (err) throw err;
      con.execute("DROP KEYSPACE ?;", [keyspace], function(err) {
        con.execute("CREATE KEYSPACE ? WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};", [keyspace], function(err) {
          if (err) throw err;
          con.execute("USE ?;", [keyspace], function(err) {
            if (err) throw err;
            callback();
          });
        });
      });
    });
  }
}