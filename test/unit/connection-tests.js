var assert = require('assert');
var async = require('async');
var util = require('util');

var Connection = require('../../lib/connection.js');
var defaultOptions = require('../../lib/client-options.js').defaultOptions();
var types = require('../../lib/types');
var utils = require('../../lib/utils.js');
var helper = require('../test-helper.js');

describe('Connection', function () {
  describe('#prepareOnce()', function () {
    function prepareAndAssert(connection, query) {
      return (function (cb) {
        connection.prepareOnce(query, function (err, r) {
          assert.ifError(err);
          assert.strictEqual(query, r);
          cb();
        });
      });
    }
    it('should prepare different queries', function (done) {
      var connection = newInstance();
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          cb(null, r.query);
        });
      };
      async.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY2'),
        prepareAndAssert(connection, 'QUERY3')
      ], function (err) {
        assert.ifError(err);
        done();
      });
    });
    it('should prepare different queries with keyspace', function (done) {
      var connection = newInstance();
      connection.keyspace = 'ks1';
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          cb(null, r.query);
        });
      };
      async.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY2'),
        prepareAndAssert(connection, 'QUERY3')
      ], function (err) {
        assert.ifError(err);
        done();
      });
    });
    it('should prepare the same query once', function (done) {
      var connection = newInstance();
      var ioCount = 0;
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          ioCount++;
          cb(null, r.query);
        });
      };
      async.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1')
      ], function (err) {
        assert.ifError(err);
        assert.strictEqual(ioCount, 1);
        done();
      });
    });
    it('should prepare the same query once with keyspace', function (done) {
      var connection = newInstance();
      connection.keyspace = 'ks1';
      var ioCount = 0;
      //override sendStream behaviour
      connection.sendStream = function(r, o, cb) {
        setImmediate(function () {
          ioCount++;
          cb(null, r.query);
        });
      };
      async.parallel([
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1'),
        prepareAndAssert(connection, 'QUERY1')
      ], function (err) {
        assert.ifError(err);
        assert.strictEqual(ioCount, 1);
        done();
      });
    });
  });
});

function newInstance(address){
  if (!address) {
    address = helper.baseOptions.contactPoints[0];
  }
  var logEmitter = function () {};
  var options = utils.extend({logEmitter: logEmitter}, defaultOptions);
  return new Connection(address, 1, options);
}