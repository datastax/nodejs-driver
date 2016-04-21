"use strict";
var assert = require('assert');
var async = require('neo-async');
var events = require('events');

var helper = require('../test-helper.js');
var ControlConnection = require('../../lib/control-connection.js');
var Host = require('../../lib/host').Host;
var utils = require('../../lib/utils');
var Metadata = require('../../lib/metadata');
var types = require('../../lib/types');
var clientOptions = require('../../lib/client-options.js');

describe('ControlConnection', function () {
  describe('constructor', function () {
    it('should create a new metadata instance', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      helper.assertInstanceOf(cc.metadata, Metadata);
    });
  });
  describe('#nodeSchemaChangeHandler()', function () {
    it('should update keyspace metadata information', function () {
      var cc = new ControlConnection(clientOptions.extend({}, helper.baseOptions));
      cc.log = helper.noop;
      var ksName = 'dummy';
      //mock connection
      cc.connection = {
        sendStream: function (a, b, c) {
          c(null, {rows: [{'keyspace_name': ksName, 'strategy_options': null}]});
        }
      };
      assert.strictEqual(Object.keys(cc.metadata.keyspaces).length, 0);
      cc.nodeSchemaChangeHandler({schemaChangeType: 'CREATED', keyspace: ksName});
      assert.ok(cc.metadata.keyspaces[ksName]);
      cc.nodeSchemaChangeHandler({schemaChangeType: 'DROPPED', keyspace: ksName});
      assert.strictEqual(typeof cc.metadata.keyspaces[ksName], 'undefined');
      //check that the callback error does not throw
      cc.connection.sendStream =  function (a, b, c) {
        c(new Error('Fake error'));
      };
      assert.doesNotThrow(function () {
        cc.nodeSchemaChangeHandler({schemaChangeType: 'CREATED', keyspace: ksName});
      });
      //and the keyspace was not added
      assert.strictEqual(typeof cc.metadata.keyspaces[ksName], 'undefined');
    });
  });
  describe('#nodeStatusChangeHandler()', function () {
    it('should call event address toString() to get', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var toStringCalled = false;
      var hostsGetCalled = false;
      var cc = new ControlConnection(options);
      cc.hosts = { get : function () { hostsGetCalled = true;}};
      var event = { inet: { address: { toString: function () { toStringCalled = true; return 'host1';}}}};
      cc.nodeStatusChangeHandler(event);
      assert.strictEqual(toStringCalled, true);
      assert.strictEqual(hostsGetCalled, true);
    });
    it('should set the node down', function () {
      var downSet = false;
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = new ControlConnection(options);
      cc.hosts = { get : function () { return { setDown: function () { downSet = true;}} }};
      var event = { inet: { address: { toString: function () { return 'host1';}}}};
      cc.nodeStatusChangeHandler(event);
      assert.strictEqual(downSet, true);
    });
  });
  describe('#getAddressForPeerHost()', function() {
    it('should handle null, 0.0.0.0 and valid addresses', function (done) {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = new ControlConnection(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var peer = getInet([100, 100, 100, 100]);
      async.series([
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
      var cc = new ControlConnection(options);
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
      var cc = new ControlConnection(options);
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
      var cc = new ControlConnection(options);
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
      var cc = new ControlConnection(options);
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
        getDistance: helper.noop
      };
      var cc = new ControlConnection(options);
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
        getDistance: helper.noop
      };
      var cc = new ControlConnection(options);
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
        getDistance: helper.noop
      };
      var listenCalled = 0;
      var cc = new ControlConnection(options);
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
        getDistance: function () {
          return types.distance.ignored;
        }
      };
      var cc = new ControlConnection(options);
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