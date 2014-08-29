var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

describe('Client', function () {
  describe('#_getPrepared()', function () {
    var Client = rewire('../../lib/client.js');
    var requestHandlerMock = function () {this.counter = 0;};
    var prepareCounter;
    requestHandlerMock.prototype.send = function noop (query, options, cb) {
      //make it async
      setTimeout(function () {
        prepareCounter++;
        cb(null, {id: new Buffer([0])});
      }, 50);
    };
    Client.__set__("RequestHandler", requestHandlerMock);
    it('should prepare making request if not exist', function (done) {
      var client = new Client({contactPoints: ['host']});
      prepareCounter = 0;
      client._getPrepared('QUERY1', function (err, id) {
        assert.equal(err, null);
        assert.notEqual(id, null);
        assert.strictEqual(id.constructor.name, 'Buffer');
        assert.strictEqual(prepareCounter, 1);
        done();
      });
    });
    it('should prepare make the same request once and queue the rest', function (done) {
      var client = new Client({contactPoints: ['host']});
      prepareCounter = 0;
      async.parallel([
        function (nextParallel) {
          async.times(100, function (n, next) {
            client._getPrepared('QUERY ONE', next);
          }, function (err, results) {
            assert.equal(err, null);
            assert.ok(results);
            var id = results[0];
            assert.notEqual(id, null);
            nextParallel();
          });
        },
        function (nextParallel) {
          async.times(100, function (n, next) {
            client._getPrepared('QUERY TWO', next);
          }, function (err, results) {
            assert.equal(err, null);
            assert.ok(results);
            var id = results[0];
            assert.notEqual(id, null);
            nextParallel();
          });
        }
      ], function (err) {
        if (err) return done(err);
        assert.strictEqual(prepareCounter, 2);
        done();
      });
    });
    it('should check for overflow and remove older', function (done) {
      var maxPrepared = 10;
      var client = new Client({contactPoints: ['host'], maxPrepared: maxPrepared});
      async.timesSeries(maxPrepared + 2, function (n, next) {
        client._getPrepared('QUERY ' + n.toString(), next);
      }, function (err) {
        if (err) return done(err);
        assert.strictEqual(client.preparedQueries.__length, maxPrepared);
        done();
      });
    });
    it('should callback in error if request send fails', function (done) {
      requestHandlerMock.prototype.send = function noop (query, options, cb) {
        setTimeout(function () {
          cb(new Error());
        }, 50);
      };
      var client = new Client({contactPoints: ['host']});
      client._getPrepared('QUERY1', function (err, id) {
        assert.ok(err, 'It should callback with error');
        assert.equal(id, null);
        done();
      });
    });
  });
});