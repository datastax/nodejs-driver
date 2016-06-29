"use strict";
var assert = require('assert');
var events = require('events');
var rewire = require('rewire');
var dns = require('dns');

var helper = require('../test-helper.js');
var ControlConnection = require('../../lib/control-connection');
var Host = require('../../lib/host').Host;
var utils = require('../../lib/utils');
var Metadata = require('../../lib/metadata');
var types = require('../../lib/types');
var clientOptions = require('../../lib/client-options');
var ProfileManager = require('../../lib/execution-profile').ProfileManager;

describe('ControlConnection', function () {
  describe('constructor', function () {
    it('should create a new metadata instance', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      helper.assertInstanceOf(cc.metadata, Metadata);
    });
  });
  describe('#init()', function () {
    this.timeout(20000);
    var useLocalhost;
    before(function (done) {
      dns.resolve('localhost', function (err) {
        if (err) {
          helper.trace('localhost can not be resolved');
        }
        useLocalhost = !err;
        done();
      });
    });
    function testResolution(CcMock, expectedHosts, done) {
      var cc = new CcMock(clientOptions.extend({ contactPoints: ['my-host-name'] }));
      cc.getConnection = helper.callbackNoop;
      cc.refreshOnConnection = helper.callbackNoop;
      cc.init(function (err) {
        assert.ifError(err);
        var hosts = cc.hosts.values();
        assert.deepEqual(hosts.map(function (h) { return h.address; }), expectedHosts);
        done();
      });
    }
    it('should resolve IPv4 and IPv6 addresses', function (done) {
      if (!useLocalhost) {
        return done();
      }
      var cc = new ControlConnection(clientOptions.extend({ contactPoints: ['localhost'] }));
      cc.getConnection = helper.callbackNoop;
      cc.refreshOnConnection = helper.callbackNoop;
      cc.init(function (err) {
        assert.ifError(err);
        var hosts = cc.hosts.values();
        assert.strictEqual(hosts.length, 2);
        assert.deepEqual(hosts.map(function (h) { return h.address; }).sort(), [ '127.0.0.1:9042', '::1:9042' ]);
        done();
      });
    });
    it('should resolve all IPv4 and IPv6 addresses provided by dns.resolve()', function (done) {
      var ControlConnectionMock = rewire('../../lib/control-connection');
      ControlConnectionMock.__set__('dns', {
        resolve4: function (name, cb) {
          cb(null, ['1', '2']);
        },
        resolve6: function (name, cb) {
          cb(null, ['10', '20']);
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      });
      testResolution(ControlConnectionMock, [ '1:9042', '2:9042', '10:9042', '20:9042' ], done);
    });
    it('should ignore IPv4 or IPv6 resolution errors', function (done) {
      var ControlConnectionMock = rewire('../../lib/control-connection');
      ControlConnectionMock.__set__('dns', {
        resolve4: function (name, cb) {
          cb(null, ['1', '2']);
        },
        resolve6: function (name, cb) {
          cb(new Error('Test error'));
        },
        lookup: function () {
          throw new Error('dns.lookup() should not be used');
        }
      });
      testResolution(ControlConnectionMock, [ '1:9042', '2:9042'], done);
    });
    it('should use dns.lookup() as failover', function (done) {
      var ControlConnectionMock = rewire('../../lib/control-connection');
      ControlConnectionMock.__set__('dns', {
        resolve4: function (name, cb) {
          cb(new Error('Test error'));
        },
        resolve6: function (name, cb) {
          cb(new Error('Test error'));
        },
        lookup: function (name, cb) {
          cb(null, '123');
        }
      });
      testResolution(ControlConnectionMock, [ '123:9042' ], done);
    });
    it('should use dns.lookup() when no address was resolved', function (done) {
      var ControlConnectionMock = rewire('../../lib/control-connection');
      ControlConnectionMock.__set__('dns', {
        resolve4: function (name, cb) {
          cb(null);
        },
        resolve6: function (name, cb) {
          cb(null, []);
        },
        lookup: function (name, cb) {
          cb(null, '123');
        }
      });
      testResolution(ControlConnectionMock, [ '123:9042' ], done);
    });
  });
  describe('#nodeSchemaChangeHandler()', function () {
    it('should update keyspace metadata information', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      cc.log = helper.noop;
      var ksName = 'ks1';
      var refreshedKeyspaces = [];
      var refreshedObjects = [];
      cc.scheduleKeyspaceRefresh = function (name, b, cb) {
        refreshedKeyspaces.push(name);
        if (cb) cb();
      };
      cc.scheduleObjectRefresh = function (h, ks, cqlObject) {
        h();
        refreshedObjects.push(ks + '-' + (cqlObject || ''));
      };
      cc.metadata.keyspaces = {};
      cc.metadata.keyspaces[ksName] = { tables: { 'tbl1': {} }, views: {} };
      cc.nodeSchemaChangeHandler({schemaChangeType: 'DROPPED', keyspace: ksName, isKeyspace: true});
      assert.strictEqual(refreshedKeyspaces.length, 0);
      assert.deepEqual(refreshedObjects, [ ksName + '-' ]);
      cc.nodeSchemaChangeHandler({ schemaChangeType: 'CREATED', keyspace: ksName, isKeyspace: true});
      assert.deepEqual(refreshedKeyspaces, [ ksName ]);
      cc.nodeSchemaChangeHandler({ schemaChangeType: 'UPDATED', keyspace: ksName, isKeyspace: true});
      assert.deepEqual(refreshedKeyspaces, [ ksName, ksName ]);
      cc.nodeSchemaChangeHandler({ schemaChangeType: 'UPDATED', keyspace: ksName, isKeyspace: true});
      assert.deepEqual(refreshedKeyspaces, [ ksName, ksName, ksName ]);
      cc.metadata.keyspaces[ksName] = { tables: { 'tbl1': {} }, views: {} };
      cc.nodeSchemaChangeHandler({ schemaChangeType: 'UPDATED', keyspace: ksName, table: 'tbl1'});
      // clears the internal state
      assert.ok(!cc.metadata.keyspaces[ksName].tables['tbl1']);
    });
  });
  describe('#nodeStatusChangeHandler()', function () {
    it('should call event address toString() to get', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var toStringCalled = false;
      var hostsGetCalled = false;
      var cc = newInstance(options);
      cc.hosts = { get : function () { hostsGetCalled = true;}};
      var event = { inet: { address: { toString: function () { toStringCalled = true; return 'host1';}}}};
      cc.nodeStatusChangeHandler(event);
      assert.strictEqual(toStringCalled, true);
      assert.strictEqual(hostsGetCalled, true);
    });
    it('should set the node down when distance is ignored', function () {
      var downSet = 0;
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.hosts = { get : function () { return {
        setDown: function () { downSet++; },
        setDistance: function () { return types.distance.ignored; }
      }}};
      var event = { inet: { address: { toString: function () { return 'host1';}}}};
      cc.nodeStatusChangeHandler(event);
      assert.strictEqual(downSet, 1);
    });
    it('should not set the node down when distance is not ignored', function () {
      var downSet = 0;
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.hosts = { get : function () { return {
        setDown: function () { downSet++;},
        datacenter: 'dc1',
        setDistance: helper.noop
      } }};
      var event = { inet: { address: { toString: function () { return 'host1';}}}};
      cc.nodeStatusChangeHandler(event);
      assert.strictEqual(downSet, 0);
    });
  });
  describe('#getAddressForPeerHost()', function() {
    it('should handle null, 0.0.0.0 and valid addresses', function (done) {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var peer = getInet([100, 100, 100, 100]);
      utils.series([
        function (next) {
          var row = {'rpc_address': getInet([1, 2, 3, 4]), peer: peer};
          cc.getAddressForPeerHost(row, 9042, function (endPoint) {
            assert.strictEqual(endPoint, '1.2.3.4:9042');
            next();
          });
        },
        function (next) {
          var row = {'rpc_address': getInet([0, 0, 0, 0]), peer: peer};
          cc.getAddressForPeerHost(row, 9001, function (endPoint) {
            //should return peer address
            assert.strictEqual(endPoint, '100.100.100.100:9001');
            next();
          });
        },
        function (next) {
          var row = {'rpc_address': null, peer: peer};
          cc.getAddressForPeerHost(row, 9042, function (endPoint) {
            //should callback with null
            assert.strictEqual(endPoint, null);
            next();
          });
        }
      ], done);
    });
    it('should call the AddressTranslator', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var address = null;
      var port = null;
      options.policies.addressResolution = { translate: function (addr, p, cb) {
        address = addr;
        port = p;
        cb(addr + ':' + p);
      }};
      var cc = newInstance(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var row = {'rpc_address': getInet([5, 2, 3, 4]), peer: null};
      cc.getAddressForPeerHost(row, 9055, function (endPoint) {
        assert.strictEqual(endPoint, '5.2.3.4:9055');
        assert.strictEqual(address, '5.2.3.4');
        assert.strictEqual(port, 9055);
      });
    });
  });
  describe('#setPeersInfo()', function () {
    it('should use not add invalid addresses', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.host = new Host('18.18.18.18', 1, options);
      cc.log = helper.noop;
      var rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1])},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1])},
        //should not be added
        {'rpc_address': null, peer: new Buffer([1, 1, 1, 1])},
        //should use peer address
        {'rpc_address': getInet([0, 0, 0, 0]), peer: getInet([5, 5, 5, 5])}
      ];
      //noinspection JSCheckFunctionSignatures
      cc.setPeersInfo(true, {rows: rows}, function (err) {
        assert.ifError(err);
        assert.strictEqual(cc.hosts.length, 3);
        assert.ok(cc.hosts.get('5.4.3.2:9042'));
        assert.ok(cc.hosts.get('9.8.7.6:9042'));
        assert.ok(cc.hosts.get('5.5.5.5:9042'));
      });
    });
    it('should set the host datacenter and cassandra version', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      //dummy
      cc.host = new Host('18.18.18.18', 1, options);
      cc.log = helper.noop;
      var rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1]), data_center: 'dc100', release_version: '2.1.4'},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1]), data_center: 'dc101', release_version: '2.1.4'}
      ];
      //noinspection JSCheckFunctionSignatures
      cc.setPeersInfo(true, {rows: rows}, function (err) {
        assert.ifError(err);
        assert.strictEqual(cc.hosts.length, 2);
        assert.ok(cc.hosts.get('5.4.3.2:9042'));
        assert.strictEqual(cc.hosts.get('5.4.3.2:9042').datacenter, 'dc100');
        assert.strictEqual(cc.hosts.get('5.4.3.2:9042').cassandraVersion, '2.1.4');
        assert.ok(cc.hosts.get('9.8.7.6:9042'));
        assert.strictEqual(cc.hosts.get('9.8.7.6:9042').datacenter, 'dc101');
        assert.strictEqual(cc.hosts.get('9.8.7.6:9042').cassandraVersion, '2.1.4');
      });
    });
  });
  describe('#refreshOnConnection()', function () {
    it('should subscribe to current host events first in case IO fails', function (done) {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.host = new Host('18.18.18.18:9042', 1, options);
      cc.log = helper.noop;
      var fakeError = new Error('fake error');
      var hostDownCalled;
      cc.hostDownHandler = function () {
        hostDownCalled = true;
      };
      cc.refreshHosts = function (up, cb) {
        //for this to fail, there should be a query executing in parallel that resulted in host.setDown()
        cc.host.setDown();
        cb(fakeError);
      };
      cc.refreshOnConnection(false, function (err) {
        assert.strictEqual(err, fakeError);
        assert.strictEqual(hostDownCalled, true);
        cc.host.shutdown(helper.noop);
        done();
      });
    });
  });
  describe('#listenHostsForUp()', function () {
    it('should subscribe to event up to all hosts', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      var hosts = [new events.EventEmitter(), new events.EventEmitter()];
      cc.hosts = {
        values: function () {
          return hosts;
        }
      };
      cc.listenHostsForUp();
      assert.strictEqual(hosts[0].listeners('up').length, 1);
      assert.strictEqual(hosts[1].listeners('up').length, 1);
    });
    it('should unsubscribe to all hosts once up is emitted and call refresh()', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      var hosts = [new events.EventEmitter(), new events.EventEmitter()];
      var refreshCalled = 0;
      cc.hosts = {
        values: function () {
          return hosts;
        }
      };
      cc.refresh = function () {
        refreshCalled++;
      };
      //add another listener
      hosts[0].on('other', function () {});
      cc.listenHostsForUp();
      assert.strictEqual(hosts[0].listeners('up').length, 1);
      assert.strictEqual(hosts[1].listeners('up').length, 1);
      //the second node is back up
      hosts[1].emit('up');
      assert.strictEqual(refreshCalled, 1);
      assert.strictEqual(hosts[0].listeners('up').length, 0);
      assert.strictEqual(hosts[1].listeners('up').length, 0);
      //the other listener is still there
      assert.strictEqual(hosts[0].listeners('other').length, 1);
    });
  });
  describe('#getConnectionToNewHost()', function () {
    it('should use the iterator from the load balancing policy', function (done) {
      var hosts = [{
        borrowConnection: function (cb2) {
          cb2(null, {});
        },
        setDistance: helper.noop,
        isUp: function () { return true; }
      }];
      var options = clientOptions.extend({}, helper.baseOptions);
      options.policies.loadBalancing = {
        newQueryPlan: function (k, o, cb) {
          cb(null, utils.arrayIterator(hosts));
        },
        getDistance: function () {return types.distance.local; }
      };
      var cc = newInstance(options);
      cc.getConnectionToNewHost(function (err, c, h) {
        assert.ifError(err);
        assert.strictEqual(h, hosts[0]);
        done();
      });
    });
    it('should use the current hosts if the new query plan fails', function (done) {
      var hosts = [{
        borrowConnection: function (cb2) {
          cb2(null, {});
        },
        setDistance: helper.noop,
        isUp: function () { return true; }
      }];
      var options = clientOptions.extend({}, helper.baseOptions);
      options.policies.loadBalancing = {
        newQueryPlan: function (k, o, cb) {
          cb(new Error('test dummy error'));
        },
        getDistance: function () {return types.distance.local; }
      };
      var cc = newInstance(options);
      cc.hosts = { values: function () { return hosts; } };
      cc.getConnectionToNewHost(function (err, c, h) {
        assert.ifError(err);
        assert.strictEqual(h, hosts[0]);
        done();
      });
    });
    it('should call listenHostsForUp() if no connection acquired', function (done) {
      var hosts = [{
        borrowConnection: function (cb) {
          cb(new Error('Test dummy error'));
        },
        setDistance: helper.noop,
        isUp: function () { return true; }
      }];
      var options = clientOptions.extend({}, helper.baseOptions);
      options.policies.loadBalancing = {
        newQueryPlan: function (k, o, cb) {
          cb(null, utils.arrayIterator(hosts));
        },
        getDistance: function () {return types.distance.local; }
      };
      var listenCalled = 0;
      var cc = newInstance(options);
      cc.listenHostsForUp = function () {
        listenCalled++;
      };
      cc.getConnectionToNewHost(function (err, c, h) {
        assert.ifError(err);
        assert.ok(!c);
        assert.ok(!h);
        assert.strictEqual(listenCalled, 1);
        done();
      });
    });
    it('should check if the host is ignored', function (done) {
      var borrowCalled = 0;
      var hosts = [{
        borrowConnection: function (cb) {
          borrowCalled++;
          cb(null, {});
        },
        setDistance: helper.noop,
        isUp: function () { return true; }
      }];
      var options = clientOptions.extend({}, helper.baseOptions);
      options.policies.loadBalancing = {
        newQueryPlan: function (k, o, cb) {
          cb(null, utils.arrayIterator(hosts));
        },
        getDistance: function () {return types.distance.ignored; }
      };
      var cc = newInstance(options);
      cc.listenHostsForUp = helper.noop;
      cc.getConnectionToNewHost(function (err, c, h) {
        assert.ifError(err);
        assert.ok(!c);
        assert.ok(!h);
        assert.strictEqual(borrowCalled, 0);
        done();
      });
    });
  });
});

/**
 * @param {Array} bytes
 * @returns {exports.InetAddress}
 */
function getInet(bytes) {
  return new types.InetAddress(new Buffer(bytes));
}

function newInstance(options) {
  return new ControlConnection(options, new ProfileManager(options))
}