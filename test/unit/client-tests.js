var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var helper = require('../test-helper.js');
var errors = require('../../lib/errors.js');
var utils = require('../../lib/utils.js');
var HostMap = require('../../lib/host.js').HostMap;
var Metadata = require('../../lib/metadata.js');

describe('Client', function () {
  describe('#connect()', function () {
    this.timeout(35000);
    it('should fail if no host name can be resolved', function (done) {
      var options = utils.extend({}, helper.baseOptions, {contactPoints: ['not1.existent-host', 'not2.existent-host']});
      var Client = require('../../lib/client.js');
      var client = new Client(options);
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        assert.ok(err.message.indexOf('resolve') > 0);
        assert.ok(!client.hosts);
        done();
      });
    });
    it('should connect once and queue if multiple calls in parallel', function (done) {
      var Client = rewire('../../lib/client.js');
      var initCounter = 0;
      var emitCounter = 0;
      var controlConnectionMock = function () {
        this.hosts = new HostMap();
        this.metadata = new Metadata();
        this.init = function (cb) {
          initCounter++;
          //Async
          setTimeout(cb, 100);
        }
      };
      Client.__set__("ControlConnection", controlConnectionMock);
      var client = new Client(helper.baseOptions);
      client.on('connected', function () {emitCounter++;});
      async.times(1000, function (n, next) {
        client.connect(function (err) {
          assert.ifError(err);
          next();
        });
      }, function (err){
        assert.ifError(err);
        assert.strictEqual(emitCounter, 1);
        assert.strictEqual(initCounter, 1);
        done();
      });
    });
  });
  describe('#_getPrepared()', function () {
    var Client = rewire('../../lib/client.js');
    var requestHandlerMock = function () {this.counter = 0;};
    var prepareCounter;
    requestHandlerMock.prototype.send = function noop (query, options, cb) {
      //make it async
      setTimeout(function () {
        prepareCounter++;
        cb(null, {id: new Buffer([0]), meta: {}});
      }, 50);
    };
    Client.__set__("RequestHandler", requestHandlerMock);
    it('should prepare making request if not exist', function (done) {
      var client = new Client({contactPoints: ['host']});
      prepareCounter = 0;
      client._getPrepared('QUERY1', function (err, id, meta) {
        assert.equal(err, null);
        assert.notEqual(id, null);
        assert.notEqual(meta, null);
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
      client._getPrepared('QUERY1', function (err, id, meta) {
        assert.ok(err, 'It should callback with error');
        assert.equal(id, null);
        assert.equal(meta, null);
        done();
      });
    });
  });
});