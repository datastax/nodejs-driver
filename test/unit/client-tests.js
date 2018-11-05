'use strict';
const assert = require('assert');
const util = require('util');
const rewire = require('rewire');

const policies = require('../../lib/policies');
const helper = require('../test-helper');
const errors = require('../../lib/errors');
const utils = require('../../lib/utils');
const types = require('../../lib/types');
const HostMap = require('../../lib/host').HostMap;
const Host = require('../../lib/host').Host;
const Metadata = require('../../lib/metadata');
const Encoder = require('../../lib/encoder');
const ProfileManager = require('../../lib/execution-profile').ProfileManager;
const ExecutionProfile = require('../../lib/execution-profile').ExecutionProfile;
const clientOptions = require('../../lib/client-options');
const PrepareHandler = require('../../lib/prepare-handler');

// allow non-global require as Client gets rewired.
/* eslint-disable global-require */

describe('Client', function () {
  describe('constructor', function () {
    it('should throw an exception when contactPoints are not provided', function () {
      const Client = require('../../lib/client');
      assert.throws(function () {
        return new Client({});
      }, TypeError);
      assert.throws(function () {
        return new Client({contactPoints: []});
      }, TypeError);
      assert.throws(function () {
        return new Client(null);
      }, TypeError);
      assert.throws(function () {
        return new Client();
      }, TypeError);
    });
    it('should create Metadata instance', function () {
      const Client = require('../../lib/client');
      const client = new Client({ contactPoints: ['192.168.10.10'] });
      helper.assertInstanceOf(client.metadata, Metadata);
    });
  });
  describe('#connect()', function () {
    this.timeout(35000);
    it('should fail if no host name can be resolved', function (done) {
      const options = utils.extend({}, helper.baseOptions, {contactPoints: ['not1.existent-host', 'not2.existent-host']});
      const Client = require('../../lib/client.js');
      const client = new Client(options);
      client.connect(function (err) {
        assert.ok(err);
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        assert.ok(err.message.indexOf('resolve') > 0, 'Message was: ' + err.message);
        assert.ok(client.hosts);
        assert.strictEqual(client.hosts.length, 0);
        done();
      });
    });
    it('should connect once and queue if multiple calls in parallel', function (done) {
      const Client = rewire('../../lib/client.js');
      let initCounter = 0;
      let emitCounter = 0;
      const options = utils.extend({
        contactPoints: helper.baseOptions.contactPoints,
        policies: {
          loadBalancing: new policies.loadBalancing.RoundRobinPolicy()
        },
        pooling: {
          warmup: false
        }
      });
      const ccMock = getControlConnectionMock(null, options);
      ccMock.prototype.init = function (cb) {
        initCounter++;
        //Async
        setTimeout(cb, 100);
      };
      Client.__set__("ControlConnection", ccMock);
      const client = new Client(options);
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
      const Client = require('../../lib/client');
      const options = utils.extend({
        contactPoints: helper.baseOptions.contactPoints,
        policies: {
          loadBalancing: new policies.loadBalancing.RoundRobinPolicy()
        },
        pooling: {
          warmup: false
        }
      });
      const client = new Client(options);
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
    context('with no callback specified', function () {
      it('should return a promise', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.connect();
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          done();
        });
      });
    });
  });
  describe('#execute()', function () {
    it('should not support named parameters for simple statements', function (done) {
      const client = newConnectedInstance();
      client.execute('QUERY', {named: 'parameter'}, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should build the routingKey based on routingNames', function (done) {
      const requestHandlerMock = {
        send: function (request, options, client, cb) {
          assert.ok(options);
          assert.ok(options.routingKey);
          // eslint-disable-next-line no-useless-concat
          assert.strictEqual(options.routingKey.toString('hex'), '00040000000100' + '00040000000200');
          cb();
          done();
        }
      };
      const prepareHandlerMock = {
        getPrepared: function (c, lbp, q, ks, cb) {
          cb(null, utils.allocBufferFromArray([1]), {
            columns: [
              { name: 'key1', type: { code: types.dataTypes.int} },
              { name: 'key2', type: { code: types.dataTypes.int} }
            ],
            partitionKeys: [0, 1]
          });
        }
      };
      const client = newConnectedInstance(requestHandlerMock, null, prepareHandlerMock);
      const query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      const queryOptions = { prepare: true };
      client.execute(query, { key2: 2, key1: 1 }, queryOptions, helper.throwop);
    });
    it('should build the routingKey based on routingNames for query requests', function (done) {
      const requestHandlerMock = {
        send: function (request, options, client, cb) {
          assert.ok(options);
          assert.ok(options.routingKey);
          // eslint-disable-next-line no-useless-concat
          assert.strictEqual(options.routingKey.toString('hex'), '00040000000100' + '00040000000200');
          cb();
          done();
        }
      };
      const client = newConnectedInstance(requestHandlerMock, null);
      client.controlConnection = { protocolVersion: types.protocolVersion.maxSupported };
      const query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      const queryOptions = { routingNames: ['key1', 'key2'], hints: { key1: 'int', key2: 'int' } };
      client.execute(query, { key2: 2, key1: 1 }, queryOptions, helper.throwop);
    });
    it('should fill parameter information for string hints', function (done) {
      let options;
      const requestHandlerMock = {
        send: function (r, o, client, cb) {
          options = o;
          cb();
        }
      };
      const client = newConnectedInstance(requestHandlerMock);
      client.metadata.getUdt = function (ks, n, cb) {
        cb(null, {keyspace: ks, name: n});
      };
      const query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
      const queryOptions = { prepare: false, hints: ['int', 'list<uuid>', 'map<text, timestamp>', 'udt<ks1.udt1>', 'map<uuid, set<text>>']};
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
      const requestHandlerMock = {
        send: function (request, o, client, cb) {
          cb();
        }
      };
      const client = newConnectedInstance(requestHandlerMock);
      const query = 'SELECT * FROM dummy WHERE id2=:key2 and id1=:key1';
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
    it('should pass optional parameters as null when not defined', function () {
      const client = newConnectedInstance();
      let params = null;
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
      const Client = require('../../lib/client');
      const profile = new ExecutionProfile('default', {
        consistency: types.consistencies.three,
        readTimeout: 12345,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      const options = getOptions({ profiles: [ profile ] });
      const client = new Client(options);
      let queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      client.execute('Q', [], { }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['loadBalancing', 'name']);
    });
    it('should use the provided execution profile options', function () {
      const Client = require('../../lib/client');
      const profile = new ExecutionProfile('profile1', {
        consistency: types.consistencies.three,
        readTimeout: 54321,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      const options = getOptions({ profiles: [ profile ] });
      const client = new Client(options);
      let queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      // profile by name
      client.execute('Q1', [], { executionProfile: 'profile1' }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['loadBalancing', 'name']);
      const previousQueryOptions = queryOptions;
      // profile by instance
      client.execute('Q1', [], { executionProfile: profile }, utils.noop);
      helper.compareProps(queryOptions, previousQueryOptions, Object.keys(queryOptions), ['executionProfile']);
    });
    it('should override the provided execution profile options with provided options', function () {
      const Client = require('../../lib/client');
      const profile = new ExecutionProfile('profile1', {
        consistency: types.consistencies.three,
        readTimeout: 54321,
        retry: new policies.retry.RetryPolicy(),
        serialConsistency: types.consistencies.localSerial
      });
      const options = getOptions({ profiles: [ profile ] });
      const client = new Client(options);
      let queryOptions = null;
      client._innerExecute = function (q, p, o) {
        queryOptions = o;
      };
      // profile by name
      client.execute('Q1', [], { consistency: types.consistencies.all, executionProfile: 'profile1' }, utils.noop);
      helper.compareProps(queryOptions, profile, Object.keys(profile), ['consistency', 'loadBalancing', 'name']);
      assert.strictEqual(queryOptions.consistency, types.consistencies.all);
      const previousQueryOptions = queryOptions;
      // profile by instance
      client.execute('Q1', [], { consistency: types.consistencies.all, executionProfile: profile }, utils.noop);
      helper.compareProps(queryOptions, previousQueryOptions, Object.keys(queryOptions), ['executionProfile']);
    });
    it('should set the timestamp', function (done) {
      let actualOptions;
      const handlerMock = {
        send: function (r, o, client, cb) {
          actualOptions = o;
          cb(null, {});
        }
      };
      const client = newConnectedInstance(handlerMock);
      utils.eachSeries([1, 2, 3, 4], function (version, next) {
        client.controlConnection.protocolVersion = version;
        client.execute('Q', function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          if (version > 2) {
            assert.ok(actualOptions.timestamp);
            assert.ok((actualOptions.timestamp instanceof types.Long) || typeof actualOptions.timestamp === 'number');
          }
          else {
            assert.strictEqual(actualOptions.timestamp, undefined);
          }
          next();
        });
      }, done);
    });
    it('should not set the timestamp when timestampGeneration is null', function (done) {
      let actualOptions;
      const handlerMock = {
        send: function (r, o, client, cb) {
          actualOptions = o;
          cb(null, {});
        }
      };
      const client = newConnectedInstance(handlerMock, { policies: { timestampGeneration: null }});
      client.controlConnection.protocolVersion = 4;
      client.execute('Q', function (err) {
        assert.ifError(err);
        assert.ok(actualOptions);
        assert.strictEqual(actualOptions.timestamp, undefined);
        done();
      });
    });
    context('with no callback specified', function () {
      it('should return a promise', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.execute('Q', [ 1, 2 ], { prepare: true });
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          done();
        });
      });
      it('should reject the promise when an ExecutionProfile is not found', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.execute('Q', [ 1, 2 ], { executionProfile: 'non_existent' });
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          done();
        });
      });
    });
  });
  describe('#eachRow()', function () {
    it('should pass optional parameters as null when not defined', function () {
      const createQueryOptions = clientOptions.createQueryOptions;
      const client = newConnectedInstance();
      let params = null;
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
    const Client = rewire('../../lib/client.js');
    const requestHandlerMock = {
      send: function (r, o, client, cb) {
        // Make it async
        setTimeout(function () {
          cb(null, {meta: {}});
        }, 50);
      }
    };
    Client.__set__("RequestHandler", requestHandlerMock);
    it('should internally call to connect', function (done) {
      const client = new Client(helper.baseOptions);
      let connectCalled = false;
      client.connect = function (cb) {
        connectCalled = true;
        cb();
      };
      client.batch(['q1'], function (err) {
        assert.ifError(err);
        assert.strictEqual(connectCalled, true);
        done();
      });
    });
    it('should set the timestamp', function (done) {
      let actualOptions;
      const handlerMock = {
        send: function (r, o, client, cb) {
          actualOptions = o;
          cb(null, {});
        }
      };
      const client = newConnectedInstance(handlerMock);
      utils.eachSeries([1, 2, 3, 4], function (version, next) {
        client.controlConnection.protocolVersion = version;
        client.batch(['q1', 'q2', 'q3'], function (err) {
          assert.ifError(err);
          assert.ok(actualOptions);
          if (version > 2) {
            assert.ok(actualOptions.timestamp);
            assert.ok((actualOptions.timestamp instanceof types.Long) || typeof actualOptions.timestamp === 'number');
          }
          else {
            assert.strictEqual(actualOptions.timestamp, undefined);
          }
          next();
        });
      }, done);
    });
    context('with no callback specified', function () {
      it('should return a promise', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.batch(['Q'], null);
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.NoHostAvailableError);
          done();
        });
      });
      it('should reject the promise when queries is not an Array', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.batch('Q', null);
        helper.assertInstanceOf(p, Promise);
        p.catch(function (err) {
          helper.assertInstanceOf(err, errors.ArgumentError);
          done();
        });
      });
    });
  });
  describe('#batch(queries, {prepare: 1}, callback)', function () {
    it('should callback with error if the queries are not string', function (done) {
      const client = newConnectedInstance(undefined, undefined, PrepareHandler);
      client.batch([{ noQuery: true }], { prepare: true }, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
    it('should callback with error if the queries are undefined', function (done) {
      const client = newConnectedInstance(undefined, undefined, PrepareHandler);
      client.batch([ undefined, undefined, 'q3' ], { prepare: true }, function (err) {
        helper.assertInstanceOf(err, errors.ArgumentError);
        done();
      });
    });
  });
  describe('#shutdown()', function () {
    const options = clientOptions.extend({}, helper.baseOptions, {
      policies: { reconnection: new policies.reconnection.ConstantReconnectionPolicy(100)},
      logEmitter: helper.noop
    });
    it('should set connected flag to false when hosts successfully shutdown', function(done) {
      const hosts = new HostMap();
      const h1 = new Host('192.1.1.1', 1, options);
      h1.datacenter = "dc1";
      h1.pool.connections = [{close: setImmediate}];
      const h2 = new Host('192.1.1.2', 1, options);
      h2.datacenter = "dc1";
      h2.pool.connections = [{close: setImmediate}];
      hosts.push(h1.address, h1);
      hosts.push(h2.address, h2);
      const Client = rewire('../../lib/client.js');
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      const client = new Client(options);
      client.shutdown(function(){
        assert.equal(client.connected, false);
        done();
      });
    });
    it('should callback when called multiple times serially', function (done) {
      const hosts = new HostMap();
      const h1 = new Host('192.1.1.1', 1, options);
      h1.datacenter = "dc1";
      h1.pool.connections = [{close: setImmediate}];
      const h2 = new Host('192.1.1.2', 1, options);
      h2.datacenter = "dc1";
      h2.pool.connections = [{close: setImmediate}];
      hosts.push(h1.address, h1);
      hosts.push(h2.address, h2);
      const Client = rewire('../../lib/client.js');
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      const client = new Client(options);
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
      const hosts = new HostMap();
      const h1 = new Host('192.1.1.1', 1, options);
      h1.datacenter = "dc1";
      h1.pool.connections = [{close: setImmediate}];
      const h2 = new Host('192.1.1.2', 1, options);
      h2.datacenter = "dc1";
      h2.pool.connections = [{close: setImmediate}];
      hosts.push(h1.address, h1);
      hosts.push(h2.address, h2);
      const Client = rewire('../../lib/client.js');
      Client.__set__("ControlConnection", getControlConnectionMock(hosts));
      const client = new Client(options);
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
      const rp = new policies.reconnection.ConstantReconnectionPolicy(50);
      const Client = require('../../lib/client');
      const client = new Client(utils.extend({}, helper.baseOptions, { policies: { reconnection: rp } }));
      const logEvents = [];
      client.on('log', logEvents.push.bind(logEvents));
      client.connect(function (err) {
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        client.shutdown(function clientShutdownCallback(err) {
          assert.ifError(err);
          setTimeout(function () {
            logEvents.length = 0;
            setTimeout(function assertAfterSomeTime() {
              assert.strictEqual(
                logEvents.length, 0, 'Expected no log events after shutdown but was: ' + util.inspect(logEvents));
              done();
            }, 100);
          }, 20);
        });
      });
    });
    context('with no callback specified', function () {
      it('should return a promise', function (done) {
        const Client = require('../../lib/client');
        const client = new Client(helper.baseOptions);
        const p = client.shutdown();
        helper.assertInstanceOf(p, Promise);
        p.then(done);
      });
    });
  });
  describe('#_waitForSchemaAgreement()', function () {
    this.timeout(5000);

    const Client = require('../../lib/client');

    it('should continue querying until the version matches', function (done) {
      const client = new Client(helper.baseOptions);
      client.hosts = { length: 5 };
      let calls = 0;
      client.metadata = {
        compareSchemaVersions: (c, cb) => {
          cb(null, ++calls === 3);
        }
      };
      client._waitForSchemaAgreement(null, function (err) {
        assert.ifError(err);
        assert.strictEqual(calls, 3);
        done();
      });
    });

    it('should timeout if there is no agreement', function (done) {
      const client = new Client(utils.extend({}, helper.baseOptions, {
        protocolOptions: { maxSchemaAgreementWaitSeconds: 1 }
      }));
      client.hosts = { length: 5 };
      let calls = 0;
      client.metadata = {
        compareSchemaVersions: (c, cb) => {
          calls++;
          cb(null, false);
        }
      };

      client._waitForSchemaAgreement(null, function (err) {
        assert.ifError(err);
        assert.ok(calls > 0);
        done();
      });
    });

    it('should callback when there is an error retrieving versions', function (done) {
      const client = new Client(helper.baseOptions);
      client.hosts = {length: 3};
      const dummyError = new Error('dummy error');
      client.metadata = {
        compareSchemaVersions: (c, cb) => cb(dummyError)
      };

      client._waitForSchemaAgreement(null, function (err) {
        assert.strictEqual(err, dummyError);
        done();
      });
    });
  });
});

function getControlConnectionMock(hosts, options) {
  function ControlConnectionMock() {
    this.hosts = hosts || new HostMap();
    this.metadata = new Metadata(options || {});
    this.profileManager = newProfileManager(options);
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

function newConnectedInstance(requestHandlerMock, options, prepareHandlerMock) {
  const Client = rewire('../../lib/client.js');
  Client.__set__("RequestHandler", requestHandlerMock || function () {});
  Client.__set__("PrepareHandler", prepareHandlerMock || function () {});
  const client = new Client(utils.extend({}, helper.baseOptions, options));
  client._getEncoder = () => new Encoder(2, {});
  client.connect = helper.callbackNoop;
  return client;
}