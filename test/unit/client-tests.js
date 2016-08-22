var assert = require('assert');
var util = require('util');
var rewire = require('rewire');

var policies = require('../../lib/policies');
var helper = require('../test-helper');
var errors = require('../../lib/errors');
var utils = require('../../lib/utils');
var types = require('../../lib/types');
var requests = require('../../lib/requests');
var HostMap = require('../../lib/host').HostMap;
var Host = require('../../lib/host').Host;
var Metadata = require('../../lib/metadata');
var Encoder = require('../../lib/encoder');
var ProfileManager = require('../../lib/execution-profile').ProfileManager;
var ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
var clientOptions = require('../../lib/client-options');

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
    it('should create Metadata instance', function () {
      var Client = require('../../lib/client');
      var client = new Client({ contactPoints: ['192.168.10.10'] });
      helper.assertInstanceOf(client.metadata, Metadata);
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
        assert.ok(err.message.indexOf('resolve') > 0, 'Message was: ' + err.message);
        assert.ok(!client.hosts);
        done();
      });
    });
    it('should connect once and queue if multiple calls in parallel', function (done) {
      var Client = rewire('../../lib/client.js');
      var initCounter = 0;
      var emitCounter = 0;
      var options = utils.extend({
        contactPoints: helper.baseOptions.contactPoints,
        policies: {
          loadBalancing: new policies.loadBalancing.RoundRobinPolicy()
        }
      });
      var ccMock = getControlConnectionMock(null, options);
      ccMock.prototype.init = function (cb) {
        initCounter++;
        //Async
        setTimeout(cb, 100);
      };
      Client.__set__("ControlConnection", ccMock);
      var client = new Client(options);
      client.on('connected', function () {emitCounter++;});
      utils.times(1000, function (n, next) {
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
    it('should fail when trying to connect after shutdown', function (done) {
      var Client = require('../../lib/client');
      var options = utils.extend({
        contactPoints: helper.baseOptions.contactPoints,
        policies: {
          loadBalancing: new policies.loadBalancing.RoundRobinPolicy()
        }
      });
      var client = new Client(options);
      client.controlConnection = {
        init: helper.callbackNoop,
        hosts: new HostMap(),
        host: { setDistance: utils.noop },
        profileManager: new ProfileManager(options),
        shutdown: utils.noop
      };
      utils.series([
        client.connect.bind(client),
        client.shutdown.bind(client)
      ], function (err) {
        assert.ifError(err);
        client.connect(function (err) {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          assert.ok(err.message.indexOf('after shutdown') >= 0, 'Error message does not contain occurrence: ' + err.message);
          done();
        });
      });
    });
  });
  describe('#_getPrepared()', function () {
    var Client = rewire('../../lib/client.js');
    var requestHandlerMock = function () {};
    var prepareCounter;
    requestHandlerMock.prototype.send = function noop (query, options, cb) {
      //make it async
      setTimeout(function () {
        prepareCounter++;
        cb(null, {id: new Buffer([0]), meta: {}});
      }, 50);
    };
    Client.__set__("RequestHandler", requestHandlerMock);
    var options = clientOptions.defaultOptions();
    var queryOptions = {
      loadBalancing: options.policies.loadBalancing,
      retry: options.policies.retry,
      executionProfile: new ExecutionProfile('p1')
    };
    it('should prepare making request if not exist', function (done) {
      var client = new Client({contactPoints: ['host']});
      client.metadata = new Metadata(options);
      prepareCounter = 0;
      //noinspection JSAccessibilityCheck
      client._getPrepared('QUERY1', queryOptions, function (err, id, meta) {
        assert.equal(err, null);
        assert.notEqual(id, null);
        assert.notEqual(meta, null);
        //noinspection JSUnresolvedVariable
        assert.strictEqual(id.constructor.name, 'Buffer');
        assert.strictEqual(prepareCounter, 1);
        done();
      });
    });
    it('should prepare make the same request once and queue the rest', function (done) {
      var client = new Client({contactPoints: ['host']});
      client.metadata = new Metadata(options);
      prepareCounter = 0;
      utils.parallel([
        function (nextParallel) {
          utils.times(100, function (n, next) {
            //noinspection JSAccessibilityCheck
            client._getPrepared('QUERY ONE', queryOptions, next);
          }, function (err) {
            assert.ifError(err);
            nextParallel();
          });
        },
        function (nextParallel) {
          utils.times(100, function (n, next) {
            //noinspection JSAccessibilityCheck
            client._getPrepared('QUERY TWO', queryOptions, next);
          }, function (err) {
            assert.ifError(err);
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
      utils.timesSeries(maxPrepared + 2, function (n, next) {
        //noinspection JSAccessibilityCheck
        client._getPrepared('QUERY ' + n.toString(), queryOptions, next);
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
      client.metadata = new Metadata(options);
      //noinspection JSAccessibilityCheck
      client._getPrepared('QUERY1', queryOptions, function (err, id, meta) {
        assert.ok(err, 'It should callback with error');
        assert.equal(id, null);
        assert.equal(meta, null);
        done();
      });
    });
  });
  describe('#_executeAsPrepared()', function () {
    var queryOptions = {
      prepare: true,
      executionProfile: new ExecutionProfile('p2')
    };
    it('should adapt the parameters into array', function (done) {
      var requestHandlerMock = function () {};
      var client = newConnectedInstance(requestHandlerMock);
      client._getPrepared = function (q, o, cb) { cb (null,
        new Buffer(0), { columns: [{name: 'abc', type: 2}, {name: 'def', type: 2}]});
      };
      requestHandlerMock.prototype.send = function (req) {
        assert.ok(req);
        assert.strictEqual(util.inspect(req.params), util.inspect([100, 101]));
        done();
      };
      client._executeAsPrepared('SELECT ...', {def: 101, abc: 100}, queryOptions, helper.throwop);
    });
    it('should keep the parameters if an array is provided', function (done) {
      var requestHandlerMock = function () {};
      var client = newConnectedInstance(requestHandlerMock);
      client._getPrepared = function (q, o, cb) { cb (null, new Buffer(0), {columns: [{name: 'abc', type: 2}]});};
      requestHandlerMock.prototype.send = function (req) {
        assert.ok(req);
        assert.strictEqual(util.inspect(req.params), util.inspect([101]));
        done();
      };
      client._executeAsPrepared('SELECT ...', [101], queryOptions, helper.throwop);
    });
    it('should callback with error if named parameters are not provided', function (done) {
      var requestHandlerMock = function () {};
      requestHandlerMock.prototype.send = helper.callbackNoop;
      var client = newConnectedInstance(requestHandlerMock);
      client._getPrepared = function (q, o, cb) { cb (null, new Buffer(0), {columns: [{name: 'abc', type: 2}]});};
      utils.series([function (next) {
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {not_the_same_name: 100}, queryOptions, function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          assert.ok(err.message.indexOf('Parameter') >= 0);
          next();
        });
      }, function (next) {
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {}, queryOptions, function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          assert.ok(err.message.indexOf('Parameter') >= 0);
          next();
        });
      }, function (next) {
        //different casing
        //noinspection JSAccessibilityCheck
        client._executeAsPrepared('SELECT ...', {ABC: 100}, queryOptions, function (err) {
          assert.ifError(err);
          next();
        });
      }], done);
    });
  });
  describe('#execute()', function () {
    it('should not support named parameters for simple statements', function (done) {
      var client = newConnectedInstance();
      client.execute('QUERY', {named: 'parameter'}, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should build the routingKey based on routingNames', function (done) {
      var requestHandlerMock = function () {};
      var client = newConnectedInstance(requestHandlerMock);
      client._getPrepared = function (q, o, cb) {
        cb(null, new Buffer([1]), { columns: [
          { name: 'key1', type: { code: types.dataTypes.int} },
          { name: 'key2', type: { code: types.dataTypes.int} }
        ]});
      };
      //noinspection JSAccessibilityCheck
      client._getEncoder = function () {
        return new Encoder(2, client.options);
      };
      requestHandlerMock.prototype.send = function (q, options, cb) {
        assert.ok(options);
        assert.ok(options.routingKey);
        assert.strictEqual(options.routingKey.toString('hex'), '00040000000100' + '00040000000200');
        cb();
        done();
      };
      var query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      var queryOptions = { prepare: true, routingNames: ['key1', 'key2']};
      client.execute(query, {key2: 2, key1: 1}, queryOptions, helper.throwop);
    });
    it('should fill parameter information for string hints', function (done) {
      var options;
      var requestHandlerMock = function () {};
      var client = newConnectedInstance(requestHandlerMock);
      client.metadata.getUdt = function (ks, n, cb) {
        cb(null, {keyspace: ks, name: n});
      };
      requestHandlerMock.prototype.send = function (q, o, cb) {
        options = o;
        cb();
      };
      var query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      var queryOptions = { prepare: false, hints: ['int', 'list<uuid>', 'map<text, timestamp>', 'udt<ks1.udt1>', 'map<uuid, set<text>>']};
      client.execute(query, [0, 1, 2, 3, 4], queryOptions, function (err) {
        assert.ifError(err);
        assert.ok(options);
        assert.ok(options.hints);
        assert.ok(options.hints[0]);
        assert.strictEqual(options.hints[0].code, types.dataTypes.int);
        assert.ok(options.hints[1]);
        assert.strictEqual(options.hints[1].code, types.dataTypes.list);
        assert.ok(options.hints[1].info);
        assert.strictEqual(options.hints[1].info.code, types.dataTypes.uuid);
        assert.ok(options.hints[2]);
        assert.strictEqual(options.hints[2].code, types.dataTypes.map);
        assert.ok(util.isArray(options.hints[2].info));
        assert.strictEqual(options.hints[2].info[0].code, types.dataTypes.text);
        assert.strictEqual(options.hints[2].info[1].code, types.dataTypes.timestamp);
        assert.ok(options.hints[3]);
        assert.strictEqual(options.hints[3].code, types.dataTypes.udt);
        assert.ok(options.hints[3].info);
        assert.strictEqual(options.hints[3].info.keyspace, 'ks1');
        //nested collections
        assert.ok(options.hints[4]);
        assert.strictEqual(options.hints[4].code, types.dataTypes.map);
        assert.ok(util.isArray(options.hints[4].info));
        assert.ok(options.hints[4].info[0]);
        assert.strictEqual(options.hints[4].info[0].code, types.dataTypes.uuid);
        assert.strictEqual(options.hints[4].info[1].code, types.dataTypes.set);
        assert.strictEqual(options.hints[4].info[1].info.code, types.dataTypes.text);
        done();
      });
    });
    it('should callback with an argument error when the hints are not valid strings', function (done) {
      var options;
      var requestHandlerMock = function () {};
      var client = newConnectedInstance(requestHandlerMock);
      requestHandlerMock.prototype.send = function (q, o, cb) {
        options = o;
        cb();
      };
      var query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      utils.series([
        function (next) {
          client.execute(query, [], { hints: ['int2']}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function (next) {
          client.execute(query, [], { hints: ['map<zeta>']}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        },
        function (next) {
          client.execute(query, [], { hints: ['udt<myudt>']}, function (err) {
            helper.assertInstanceOf(err, TypeError);
            next();
          });
        }
      ], done);
    });
    it('should throw argument error when last parameter is not a function', function () {
      var client = newConnectedInstance();
      assert.throws(function () {
        client.execute('QUERY', [], {});
      }, errors.ArgumentError);
      assert.throws(function () {
        client.execute('QUERY', []);
      }, errors.ArgumentError);
      assert.throws(function () {
        client.execute('QUERY');
      }, errors.ArgumentError);
    });
    it('should pass optional parameters as null when not defined', function () {
      var client = newConnectedInstance();
      var params = null;
      client._innerExecute = function (q, p, o, c) {
        params = [q, p, o, c];
      };
      client.execute('Q1', [], utils.noop);
      assert.deepEqual(params, ['Q1', [], clientOptions.createQueryOptions(client, null), utils.noop]);
      client.execute('Q2', utils.noop);
      assert.deepEqual(params, ['Q2', null, clientOptions.createQueryOptions(client, null), utils.noop]);
      client.execute('Q3', null, { fetchSize: 20 }, utils.noop);
      assert.deepEqual(params, ['Q3', null, clientOptions.createQueryOptions(client, { fetchSize: 20 }), utils.noop]);
    });
    it('should use the default execution profile options', function () {
      var Client = require('../../lib/client');
      var profile = new ExecutionProfile('default', {
        consistency: types.consistencies.three,
        readTimeout: 12345,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      var options = getOptions({ profiles: [ profile ] });
      var client = new Client(options);
      var queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      client.execute('Q', [], { }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['loadBalancing', 'name']);
    });
    it('should use the provided execution profile options', function () {
      var Client = require('../../lib/client');
      var profile = new ExecutionProfile('profile1', {
        consistency: types.consistencies.three,
        readTimeout: 54321,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      var options = getOptions({ profiles: [ profile ] });
      var client = new Client(options);
      var queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      // profile by name
      client.execute('Q1', [], { executionProfile: 'profile1' }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['loadBalancing', 'name']);
      var previousQueryOptions = queryOptions;
      // profile by instance
      client.execute('Q1', [], { executionProfile: profile }, utils.noop);
      helper.compareProps(queryOptions, previousQueryOptions, Object.keys(queryOptions), ['executionProfile']);
    });
    it('should override the provided execution profile options with provided options', function () {
      var Client = require('../../lib/client');
      var profile = new ExecutionProfile('profile1', {
        consistency: types.consistencies.three,
        readTimeout: 54321,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      var options = getOptions({ profiles: [ profile ] });
      var client = new Client(options);
      var queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      // profile by name
      client.execute('Q1', [], { consistency: types.consistencies.all, executionProfile: 'profile1' }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['consistency', 'loadBalancing', 'name']);
      assert.strictEqual(queryOptions.consistency, types.consistencies.all);
      var previousQueryOptions = queryOptions;
      // profile by instance
      client.execute('Q1', [], { consistency: types.consistencies.all, executionProfile: profile }, utils.noop);
      helper.compareProps(queryOptions, previousQueryOptions, Object.keys(queryOptions), ['executionProfile']);
    });
  });
  describe('#eachRow()', function () {
    it('should pass optional parameters as null when not defined', function () {
      var createQueryOptions = clientOptions.createQueryOptions;
      var client = newConnectedInstance();
      var params = null;
      client._innerExecute = function (q, p, o, c) {
        params = [q, p, o, c];
      };
      client.eachRow('Q 2p', utils.noop);
      assert.deepEqual(params.slice(0, 3), ['Q 2p', null, createQueryOptions(client, null, utils.noop)]);
      client.eachRow('Q 3p', [], utils.noop);
      assert.deepEqual(params.slice(0, 3), ['Q 3p', [], createQueryOptions(client, null, utils.noop)]);
      client.eachRow('Q 4p', [1], utils.noop, helper.callbackNoop);
      assert.deepEqual(params.slice(0, 3), ['Q 4p', [1], createQueryOptions(client, null, utils.noop)]);
      client.eachRow('Q 4p 2', [2], { fetchSize: 1}, utils.noop);
      assert.deepEqual(params.slice(0, 3), ['Q 4p 2', [2], createQueryOptions(client, { fetchSize: 1 }, utils.noop)]);
      client.eachRow('Q 5p', [3], { fetchSize: 1}, utils.noop, helper.callbackNoop);
      assert.deepEqual(params.slice(0, 3), ['Q 5p', [3], createQueryOptions(client, { fetchSize: 1 }, utils.noop)]);
    });
  });
  describe('#batch()', function () {
    var Client = rewire('../../lib/client.js');
    var requestHandlerMock = function () {};
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
        client.profileManager = newProfileManager();
        cb();
      };
      client.batch(['q1'], function (err) {
        assert.ifError(err);
        assert.strictEqual(connectCalled, true);
        done();
      });
    });
  });
  describe('#batch(queries, {prepare: 1}, callback)', function () {
    it('should callback with error if the queries are not string', function () {
      var client = newConnectedInstance();
      assert.throws(function () {
        client.batch([{noQuery: true}], {prepare: true}, helper.throwop);
      }, errors.ArgumentError);
    });
    it('should callback with error if the queries are undefined', function () {
      var client = newConnectedInstance();
      assert.throws(function () {
        client.batch([undefined, undefined, 'q3'], {prepare: true}, helper.throwop);
      }, errors.ArgumentError);
    });
    it('should prepare for the first time', function (done) {
      var called;
      var handlerMock = function () {};
      handlerMock.prototype.prepareMultiple = function (queries, cbs, o, callback) {
        called = true;
        assert.strictEqual(queries.length, 3);
        assert.strictEqual(queries[0], 'q1');
        assert.strictEqual(queries[1], 'q2');
        callback();
      };
      handlerMock.prototype.send = helper.callbackNoop;
      var client = newConnectedInstance(handlerMock);
      client.metadata = new Metadata(client.options);
      client.batch(['q1', 'q2', 'q3'], {prepare: 1}, function (err) {
        assert.ifError(err);
        assert.ok(called);
        done();
      });
    });
    it('should only prepare the ones that are not', function (done) {
      var called;
      var handlerMock = function () {};
      handlerMock.prototype.prepareMultiple = function (queries, cbs, o, callback) {
        called = true;
        assert.strictEqual(queries.length, 3);
        assert.strictEqual(queries[0], 'q1');
        assert.strictEqual(queries[1], 'q3');
        callback();
      };
      handlerMock.prototype.send = helper.callbackNoop;
      var client = newConnectedInstance(handlerMock);
      client.metadata = new Metadata(client.options);
      //q2 and q4 are prepared
      client.metadata.getPreparedInfo(null, 'q2').queryId = new Buffer(3);
      client.metadata.getPreparedInfo(null, 'q4').queryId = new Buffer(3);
      client.batch(['q1', 'q2', 'q3', 'q4', 'q5'], {prepare: 1}, function (err) {
        assert.ifError(err);
        assert.ok(called);
        done();
      });
    });
    it('should only prepare the ones that are not and wait for the ones preparing', function (done) {
      var sendMultipleCalled;
      var preparingCallbackCalled;
      var handlerMock = function () {};
      handlerMock.prototype.prepareMultiple = function (queries, cbs, o, callback) {
        sendMultipleCalled = true;
        assert.strictEqual(queries.length, 2);
        assert.strictEqual(queries[0], 'q1');
        assert.strictEqual(queries[1], 'q5');
        callback();
      };
      handlerMock.prototype.send = helper.callbackNoop;
      var client = newConnectedInstance(handlerMock);
      client.metadata = new Metadata(client.options);
      //q3 and q4 are prepared
      client.metadata.getPreparedInfo(null, 'q3').queryId = new Buffer(3);
      client.metadata.getPreparedInfo(null, 'q4').queryId = new Buffer(3);
      //q2 is being prepared
      var q2Info = client.metadata.getPreparedInfo(null, 'q2');
      q2Info.preparing = true;
      q2Info.once = function (name, cb) {
        q2Info.dummyCb = cb;
      };
      client.batch(['q1', 'q2', 'q3', 'q4', 'q5'], {prepare: 1}, function (err) {
        assert.ifError(err);
        assert.ok(sendMultipleCalled);
        assert.ok(preparingCallbackCalled);
        done();
      });
      setTimeout(function () {
        q2Info.preparing = false;
        q2Info.queryId = new Buffer(1);
        preparingCallbackCalled = true;
        q2Info.dummyCb();
      }, 80);
    });
  });
  describe('#shutdown()', function () {
    var options = clientOptions.extend({}, helper.baseOptions, {
      policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(100)},
      logEmitter: helper.noop
    });
    it('should set connected flag to false when hosts successfully shutdown', function(done) {
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
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      var client = new Client(options);
      client.shutdown(function(){
        assert.equal(client.connected, false);
        done();
      });
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
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      var client = new Client(options);
      utils.series([
        client.connect.bind(client),
        function shutDownMultiple(seriesNext) {
          utils.timesSeries(10, function(n, next) {
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
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      var client = new Client(options);
      utils.series([
        client.connect.bind(client),
        function shutDownMultiple(seriesNext) {
          utils.times(100, function(n, next) {
            client.shutdown(next);
          }, seriesNext);
        }
      ], done);
    });
    it('should not attempt reconnection and log after shutdown', function (done) {
      var rp = new policies.reconnection.ConstantReconnectionPolicy(50);
      var Client = require('../../lib/client');
      var client = new Client(utils.extend({}, helper.baseOptions, { policies: { reconnection: rp } }));
      var logEvents = [];
      client.on('log', logEvents.push.bind(logEvents));
      client.connect(function (err) {
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        client.shutdown(function clientShutdownCallback(err) {
          assert.ifError(err);
          logEvents.length = 0;
          setTimeout(function assertAfterSomeTime() {
            assert.strictEqual(
              logEvents.length, 0, 'Expected no log events after shutdown but was: ' + util.inspect(logEvents));
            done();
          }, 400);
        });
      })
    });
  });
  describe('#_waitForSchemaAgreement()', function () {
    var Client = require('../../lib/client.js');
    it('should use the control connection to retrieve schema information', function (done) {
      var client = new Client(helper.baseOptions);
      client.hosts = {length: 3};
      var localCalls = 0;
      var peerCalls = 0;
      client.metadata = {
        getLocalSchemaVersion: function (c, cb) {
          localCalls++;
          setImmediate(function () { cb(null, '1'); });
        },
        getPeersSchemaVersions: function (c, cb) {
          peerCalls++;
          setImmediate(function () { cb(null, ['1', '1']); })
        }
      };
      client._waitForSchemaAgreement(null, function (err) {
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
      client.metadata = {
        getLocalSchemaVersion: function (c, cb) {
          localCalls++;
          setImmediate(function () { cb(null, '3'); });
        },
        getPeersSchemaVersions: function (c, cb) {
          peerCalls++;
          //The third time it gets called versions will match
          setImmediate(function () { cb(null, [peerCalls]); })
        }
      };
      client._waitForSchemaAgreement(null, function (err) {
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
      client.metadata = {
        getLocalSchemaVersion: function (c, cb) {
          localCalls++;
          setImmediate(function () { cb(null, '1'); });
        },
        getPeersSchemaVersions: function (c, cb) {
          peerCalls++;
          //The versions are always different
          setImmediate(function () { cb(null, ['2']); })
        }
      };
      client._waitForSchemaAgreement(null, function (err) {
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
      client.metadata = {
        getLocalSchemaVersion: function (c, cb) {
          setImmediate(function () { cb(); });
        },
        getPeersSchemaVersions: function (c, cb) {
          setImmediate(function () { cb(dummyError); });
        }
      };
      client._waitForSchemaAgreement(null, function (err) {
        assert.strictEqual(err, dummyError);
        done();
      });
    });
  });
  describe('#_waitForPendingPrepares()', function () {
    var Client = rewire('../../lib/client.js');
    it('should return the same amount when no query is being prepared', function (done) {
      var client = new Client(helper.baseOptions);
      client.metadata = new Metadata(client.options);
      var queriesInfo = [{info: {}, query: 'q1'}, {info: {}, query: 'q2'}];
      //noinspection JSAccessibilityCheck
      client._waitForPendingPrepares(queriesInfo, function (err, toPrepare) {
        assert.ifError(err);
        assert.strictEqual(Object.keys(toPrepare).length, 2);
        done();
      });
    });
    it('should return distinct queries to prepare', function (done) {
      var client = new Client(helper.baseOptions);
      client.metadata = new Metadata(client.options);
      var queriesInfo = [{info: {}, query: 'same query'}, {info: {}, query: 'same query'}];
      //noinspection JSAccessibilityCheck
      client._waitForPendingPrepares(queriesInfo, function (err, toPrepare) {
        assert.ifError(err);
        assert.strictEqual(Object.keys(toPrepare).length, 1);
        done();
      });
    });
    it('should wait for queries being prepared', function (done) {
      var client = new Client(helper.baseOptions);
      client.metadata = new Metadata(client.options);
      var cbs = [];
      var queriesInfo = [
        {info: {}, query: 'query1'},
        {info: { preparing: true, once: function (name, cb) { cbs.push(cb); }}},
        {info: { preparing: true, once: function (name, cb) { cbs.push(cb); }}}
      ];
      var calledBack = false;
      //noinspection JSAccessibilityCheck
      client._waitForPendingPrepares(queriesInfo, function (err, toPrepare) {
        assert.ifError(err);
        assert.ok(calledBack);
        assert.strictEqual(Object.keys(toPrepare).length, 1);
        assert.strictEqual(Object.keys(toPrepare)[0], 'query1');
        done();
      });
      setTimeout(function () {
        calledBack = true;
        cbs.forEach(function (cb) { cb();});
      }, 50);
    });
  });
  describe('#_setQueryOptions()', function () {
    it('should not allow named parameters when protocol version is lower than 3 and the query is not prepared', function (done) {
      var Client = rewire('../../lib/client');
      var client = new Client(helper.baseOptions);
      client.controlConnection = { protocolVersion: 2};
      client.profileManager = newProfileManager();
      //noinspection JSAccessibilityCheck
      client._setQueryOptions({ prepare: false}, { named: 'val'}, null, function (err) {
        assert.ok(err);
        done();
      });
    });
    it('should adapt user hints using table metadata', function (done) {
      var Client = rewire('../../lib/client');
      var client = new Client(helper.baseOptions);
      var metaCalled = 0;
      client._getEncoder = function () { return { setRoutingKey: helper.noop} };
      client.controlConnection = { protocolVersion: 2};
      client.metadata = {
        adaptUserHints: function (ks, h, cb) {
          metaCalled++;
          setImmediate(function () {
            h[1] = types.dataTypes.text;
            cb();
          });
        }
      };
      var options = { prepare: false, hints: [null, 'text']};
      client.profileManager = newProfileManager();
      //noinspection JSAccessibilityCheck
      client._setQueryOptions(options, ['one', 'two'], null, function (err) {
        assert.ifError(err);
        assert.strictEqual(options.hints.length, 2);
        assert.strictEqual(options.hints[1], types.dataTypes.text);
        assert.strictEqual(metaCalled, 1);
        done();
      });
    });
    it('should use meta columns', function (done) {
      var Client = rewire('../../lib/client');
      var client = new Client(helper.baseOptions);
      client._getEncoder = function () { return { setRoutingKey: helper.noop} };
      client.controlConnection = { protocolVersion: 2};
      var meta = {
        columns: [
          { type: types.dataTypes.int },
          { type: types.dataTypes.text }
        ]
      };
      var options = { prepare: true, routingKey: new Buffer(2)};
      client.profileManager = newProfileManager();
      //noinspection JSAccessibilityCheck
      client._setQueryOptions(options, [1, 'two'], meta, function (err) {
        assert.ifError(err);
        assert.strictEqual(options.hints.length, 2);
        assert.strictEqual(options.hints[0], types.dataTypes.int);
        assert.strictEqual(options.hints[1], types.dataTypes.text);
        done();
      });
    });
    it('should use the meta partition keys to fill the routingIndexes', function (done) {
      var Client = rewire('../../lib/client');
      var client = new Client(helper.baseOptions);
      client._getEncoder = function () { return { setRoutingKey: helper.noop} };
      client.controlConnection = { protocolVersion: 4};
      var meta = {
        columns: [
          { type: types.dataTypes.uuid },
          { type: types.dataTypes.text }
        ],
        partitionKeys: [1, 0]
      };
      var options = { prepare: true};
      client.profileManager = newProfileManager();
      //noinspection JSAccessibilityCheck
      client._setQueryOptions(options, [types.Uuid.random(), 'another'], meta, function (err) {
        assert.ifError(err);
        assert.strictEqual(options.hints.length, 2);
        assert.strictEqual(options.hints[0], types.dataTypes.uuid);
        assert.strictEqual(options.hints[1], types.dataTypes.text);
        assert.ok(options.routingIndexes);
        assert.strictEqual(options.routingIndexes[0], 1);
        assert.strictEqual(options.routingIndexes[1], 0);
        done();
      });
    });
    it('should use the table metadata to fill in the routing indexes', function (done) {
      var Client = rewire('../../lib/client');
      var client = new Client(helper.baseOptions);
      var metaCalled = 0;
      client._getEncoder = function () { return { setRoutingKey: helper.noop} };
      client.controlConnection = { protocolVersion: 3};
      var meta = {
        keyspace: 'ks1',
        table: 'table1',
        columns: [
          { type: types.dataTypes.uuid },
          { type: types.dataTypes.text },
          { type: types.dataTypes.timeuuid }
        ]
      };
      meta.columnsByName = {
        'val': 1,
        'id1': 0,
        'id2': 2
      };
      client.metadata = {
        getTable: function (ks, tbl, cb) {
          assert.strictEqual(ks, meta.keyspace);
          assert.strictEqual(tbl, meta.table);
          setImmediate(function () {
            metaCalled++;
            cb(null, {
              partitionKeys: [ { name: 'id1'}, { name: 'id2'} ]
            });
          });
        }
      };
      var options = { prepare: true};
      client.profileManager = newProfileManager();
      utils.timesSeries(20, function (n, next) {
        var params = [types.Uuid.random(), 'hello', types.TimeUuid.now()];
        //noinspection JSAccessibilityCheck
        client._setQueryOptions(options, params, meta, function (err) {
          assert.ifError(err);
          assert.strictEqual(metaCalled, 1);
          assert.strictEqual(options.hints.length, 3);
          assert.strictEqual(options.hints[0], types.dataTypes.uuid);
          assert.strictEqual(options.hints[1], types.dataTypes.text);
          assert.strictEqual(options.hints[2], types.dataTypes.timeuuid);
          assert.ok(options.routingIndexes);
          assert.strictEqual(options.routingIndexes.length, 2);
          assert.strictEqual(options.routingIndexes[0], 0);
          assert.strictEqual(options.routingIndexes[1], 2);
          next();
        });
      }, done);
    });
  });
});

function getControlConnectionMock(hosts, options) {
  function ControlConnectionMock() {
    this.hosts = hosts || new HostMap();
    this.metadata = new Metadata();
    this.profileManager = newProfileManager(options);
    //noinspection JSUnresolvedVariable
    this.host = { setDistance: utils.noop };
    this.shutdown = utils.noop;
  }
  ControlConnectionMock.prototype.init = setImmediate;

  return ControlConnectionMock;
}

function getOptions(options) {
  return clientOptions.extend({ contactPoints: ['hostip1']}, options);
}

function newProfileManager(options) {
  return new ProfileManager(getOptions(options));
}

function newConnectedInstance(requestHandlerMock) {
  var Client = rewire('../../lib/client.js');
  Client.__set__("RequestHandler", requestHandlerMock || function () {});
  var client = new Client(helper.baseOptions);
  client._getEncoder = function () { return new Encoder(2, {})};
  client.connect = helper.callbackNoop;
  return client;
}