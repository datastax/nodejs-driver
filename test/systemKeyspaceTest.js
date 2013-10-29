var utils = require('../lib/utils.js');
var config = require('./config.js');

var Client = require('../index.js').Client;
var Connection = require('../index.js').Connection;

var con = new Connection(utils.extend({}, config));
//declaration order is execution order in nodeunit
module.exports = {
  connect : function (test) {
    con.on('log', function(type, message) {
      //console.log(type, message);
    });
    con.open(function (err) {
      test.ifError(err, 'Connection error');
      test.done();
    });
  },
  selectAllColumns: function (test) {
    con.execute('select * from system.schema_keyspaces;', [], function(err, result) {
      if (err) {
        test.fail(err);
      }
      else {
        test.ok(result.rows.length > 0, 'No keyspaces');
        if (result.rows.length > 1) {
          test.ok(result.rows[0].get('keyspace_name') != result.rows[1].get('keyspace_name'), 'The yielded keyspaces name should be different');
        }
      }
      test.done();
    });
  },
  select2Column: function (test) {
    con.execute('select keyspace_name, strategy_class from system.schema_keyspaces;', [], function(err, result) {
      test.ok(!err, 'Execute error');
      if (!err) {
        test.ok(result.rows.length > 0, 'No keyspaces');
        test.ok(result.meta.columns.length === 2, 'There must be 2 columns');
        if (result.rows.length > 0) {
          test.ok(result.rows[0].get('keyspace_name'), 'Get cell by value failed');
        }
      }
      test.done();
    });
  },
  selectNoMatch: function (test) {
    con.execute('select * from system.schema_keyspaces where keyspace_name=?;', ['does_not_exist'], function(err, result) {
      test.ok(!err, 'Execute error');
      if (!err) {
        test.ok(result.rows.length === 0, 'It must return an empty result set, with an empty rows array.');
      }
      test.done();
    });
  },
  select1Match: function (test) {
    con.execute('select * from system.schema_keyspaces where keyspace_name=?;', ['system'], function(err, result) {
      test.ok(!err, 'Execute error');
      if (!err) {
        test.ok(result.rows.length === 1, 'It must return an result set with 1 row.');
      }
      test.done();
    });
  },
  /**
   * Executes last, closes the connection
   */
  disconnect : function (test) {
    test.ok(con.connected, 'Connection not connected to host.');
    con.close(function () {
      test.done();
    });
  }
};