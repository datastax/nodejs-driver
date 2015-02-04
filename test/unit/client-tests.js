var assert = require('assert');
var async = require('async');
var util = require('util');
var rewire = require('rewire');

var policies = require('../../lib/policies');
var helper = require('../test-helper.js');
var errors = require('../../lib/errors.js');
var utils = require('../../lib/utils.js');
var HostMap = require('../../lib/host.js').HostMap;
var Host = require('../../lib/host.js').Host;
var Metadata = require('../../lib/metadata.js');

describe('Client', function () {
  describe('constructor', function () {
    it('should throw an exception when contactPoints are not provided', function () {
      var Client = require('../../lib/client');
      assert.throws(function () {
        new Client({});
      }, TypeError);
      assert.throws(function () {
        new Client({contactPoints: []});
      }, TypeError);
      assert.throws(function () {
        new Client(null);
      }, TypeError);
      assert.throws(function () {
        new Client();
      }, TypeError);
    });
  });
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
      var options = utils.extend({
        contactPoints: helper.baseOptions.contactPoints,
        policies: {
          loadBalancing: new policies.loadBalancing.RoundRobinPolicy()
        }
      });
      var client = new Client(options);
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
      client.metadata = new Metadata(client.options);
      prepareCounter = 0;
      //noinspection JSAccessibilityCheck
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
      client.metadata = new Metadata(client.options);
      prepareCounter = 0;
      async.parallel([
        function (nextParallel) {
          async.times(100, function (n, next) {
            //noinspection JSAccessibilityCheck
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
      client.metadata = new Metadata(client.options);
      async.timesSeries(maxPrepared + 2, function (n, next) {
        client._getPrepared('QUERY ' + n.toString(), next);
      }, function (err) {
        if (err) return done(err);
        assert.strictEqual(client.metadata.preparedQueries.__length, maxPrepared);
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
      client.metadata = new Metadata(client.options);
      client._getPrepared('QUERY1', function (err, id, meta) {
        assert.ok(err, 'It should callback with error');
        assert.equal(id, null);
        assert.equal(meta, null);
        done();
      });
    });
  });
  describe('#_executeAsPrepared()', function () {
    it('should adapt the parameters into array', function (done) {
      var Client = rewire('../../lib/client.js');
      var requestHandlerMock = function () {};
      requestHandlerMock.prototype.send = function () { };
      Client.__set__("RequestHandler", requestHandlerMock);
      var client = new Client(helper.baseOptions);
      client.connect = function (cb) {setImmediate(cb); };
      //noinspection JSAccessibilityCheck
      client._getPrepared = function (q, cb) { cb (null, new Buffer(0), {columns: [{name: 'abc', type: 2}, {name: 'def', type: 2}]});};
      requestHandlerMock.prototype.send = function (req) {
        assert.ok(req);
        assert.strictEqual(util.inspect(req.params), util.inspect([100, 101]));
        done();
      };
      //noinspection JSAccessibilityCheck
      client._executeAsPrepared('SELECT ...', {def: 101, abc: 100}, {}, helper.noop);
    });
    it('should keep the parameters if an array is provided', function (done) {
      var Client = rewire('../../lib/client.js');
      var requestHandlerMock = function () {};
      requestHandlerMock.prototype.send = function () { };
      Client.__set__("RequestHandler", requestHandlerMock);
      var client = new Client(helper.baseOptions);
      client.connect = function (cb) {setImmediate(cb); };
      //noinspection JSAccessibilityCheck
      client._getPrepared = function (q, cb) { cb (null, new Buffer(0), {columns: [{name: 'abc', type: 2}]});};
      requestHandlerMock.prototype.send = function (req) {
        assert.ok(req);
        assert.strictEqual(util.inspect(req.params), util.inspect([101]));
        done();
      };
      //noinspection JSAccessibilityCheck
      client._executeAsPrepared('SELECT ...', [101], {}, helper.noop);
    });
    it('should callback with error if named parameters are not provided', function (done) {
      var Client = rewire('../../lib/client.js');
      var requestHandlerMock = function () {};
      requestHandlerMock.prototype.send = function () { };
      Client.__set__("RequestHandler", requestHandlerMock);
      var client = new Client(helper.baseOptions);
      client.connect = function (cb) {setImmediate(cb); };
      //noinspection JSAccessibilityCheck
      client._getPrepared = function (q, cb) { cb (null, new Buffer(0), {columns: [{name: 'abc', type: 2}]});};
      async.series([function (next) {
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {not_the_same_name: 100}, {}, function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          assert.ok(err.message.indexOf('Parameter') >= 0);
          next();
        });
      }, function (next) {
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {}, {}, function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          assert.ok(err.message.indexOf('Parameter') >= 0);
          next();
        });
      }, function (next) {
        //different casing
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {ABC: 100}, {}, function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          assert.ok(err.message.indexOf('Parameter') >= 0);
          next();
        });
      }], done);
    });
  });
  describe('#execute()', function () {
    it('should not support named parameters for simple statements', function (done) {
      //when supported with protocol v3, replace test to use protocol versions
      var Client = require('../../lib/client');
      var client = new Client(helper.baseOptions);
      //fake connected
      client.connect = function (cb) {cb(); };
      client.execute('QUERY', {named: 'parameter'}, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
  });
  describe('#batch()', function () {
    var Client = rewire('../../lib/client.js');
    var requestHandlerMock = function () {this.counter = 0;};
    requestHandlerMock.prototype.send = function noop (query, options, cb) {
      //make it async
      setTimeout(function () {
        cb(null, {meta: {}});
      }, 50);
    };
    Client.__set__("RequestHandler", requestHandlerMock);
    it('should internally call to connect', function (done) {
      var client = new Client(helper.baseOptions);
      var connectCalled = false;
      client.connect = function (cb) {
        connectCalled = true;
        cb();
      };
      client.batch([], function (err) {
        assert.ifError(err);
        assert.strictEqual(connectCalled, true);
        done();
      });
    });
  });
  describe('#shutdown()', function () {
    var options = utils.extend({}, helper.baseOptions, {
      policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(100)},
      logEmitter: helper.noop
    });
    it('should callback when called multiple times serially', function (done) {
      var hosts = new HostMap();
      var h1 = new Host('192.1.1.1', 1, options);
      h1.datacenter = "dc1";
      h1.pool.connections = [{close: setImmediate}];
      var h2 = new Host('192.1.1.2', 1, options);
      h2.datacenter = "dc1";
      h2.pool.connections = [{close: setImmediate}];
      hosts.push(h1.address, h1);
      hosts.push(h2.address, h2);
      var Client = rewire('../../lib/client.js');
      var controlConnectionMock = function () {
        this.hosts = hosts;
        this.metadata = new Metadata();
        this.init = setImmediate;
      };
      Client.__set__("ControlConnection", controlConnectionMock);
      var client = new Client(options);
      async.series([
        client.connect.bind(client),
        function shutDownMultiple(seriesNext) {
          async.timesSeries(10, function(n, next) {
            client.shutdown(next);
          }, seriesNext);
        }
      ], done);
    });
    it('should callback when called multiple times in parallel', function (done) {
      var hosts = new HostMap();
      var h1 = new Host('192.1.1.1', 1, options);
      h1.datacenter = "dc1";
      h1.pool.connections = [{close: setImmediate}];
      var h2 = new Host('192.1.1.2', 1, options);
      h2.datacenter = "dc1";
      h2.pool.connections = [{close: setImmediate}];
      hosts.push(h1.address, h1);
      hosts.push(h2.address, h2);
      var Client = rewire('../../lib/client.js');
      var controlConnectionMock = function () {
        this.hosts = hosts;
        this.metadata = new Metadata();
        this.init = setImmediate;
      };
      Client.__set__("ControlConnection", controlConnectionMock);
      var client = new Client(options);
      async.series([
        client.connect.bind(client),
        function shutDownMultiple(seriesNext) {
          async.times(10, function(n, next) {
            client.shutdown(next);
          }, seriesNext);
        }
      ], done);
    });
  });
  describe('#waitForSchemaAgreement()', function () {
    var Client = require('../../lib/client.js');
    it('should use the control connection to retrieve schema information', function (done) {
      var client = new Client(helper.baseOptions);
      client.hosts = {length: 3};
      var localCalls = 0;
      var peerCalls = 0;
      client.controlConnection = {
        getLocalSchemaVersion: function (cb) {
          localCalls++;
          setImmediate(function () { cb(null, '1'); });
        },
        getPeersSchemaVersions: function (cb) {
          peerCalls++;
          setImmediate(function () { cb(null, ['1', '1']); })
        }
      };
      client.waitForSchemaAgreement(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCalls, 1);
        assert.strictEqual(peerCalls, 1);
        done();
      });
    });
    it('should continue querying until the version matches', function (done) {
      var client = new Client(helper.baseOptions);
      client.hosts = {length: 3};
      var localCalls = 0;
      var peerCalls = 0;
      client.controlConnection = {
        getLocalSchemaVersion: function (cb) {
          localCalls++;
          setImmediate(function () { cb(null, '3'); });
        },
        getPeersSchemaVersions: function (cb) {
          peerCalls++;
          //The third time it gets called versions will match
          setImmediate(function () { cb(null, [peerCalls]); })
        }
      };
      client.waitForSchemaAgreement(function (err) {
        assert.ifError(err);
        assert.strictEqual(localCalls, 3);
        assert.strictEqual(peerCalls, 3);
        done();
      });
    });
    it('should timeout if there is no agreement', function (done) {
      var client = new Client(utils.extend({}, helper.baseOptions, {protocolOptions: {maxSchemaAgreementWaitSeconds: 1}}));
      client.hosts = {length: 3};
      var localCalls = 0;
      var peerCalls = 0;
      client.controlConnection = {
        getLocalSchemaVersion: function (cb) {
          localCalls++;
          setImmediate(function () { cb(null, '1'); });
        },
        getPeersSchemaVersions: function (cb) {
          peerCalls++;
          //The versions are always different
          setImmediate(function () { cb(null, ['2']); })
        }
      };
      client.waitForSchemaAgreement(function (err) {
        assert.ifError(err);
        assert.ok(localCalls > 0);
        assert.ok(peerCalls > 0);
        done();
      });
    });
    it('should callback when there is an error retrieving versions', function (done) {
      var client = new Client(helper.baseOptions);
      client.hosts = {length: 3};
      var dummyError = new Error('dummy error');
      client.controlConnection = {
        getLocalSchemaVersion: function (cb) {
          setImmediate(function () { cb(); });
        },
        getPeersSchemaVersions: function (cb) {
          setImmediate(function () { cb(dummyError); });
        }
      };
      client.waitForSchemaAgreement(function (err) {
        assert.strictEqual(err, dummyError);
        done();
      });
    });
  });
});