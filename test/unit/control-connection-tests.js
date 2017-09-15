/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
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
var errors = require('../../lib/errors');
var policies = require('../../lib/policies');
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
      var cc = new CcMock(clientOptions.extend({ contactPoints: ['my-host-name'] }), null, getContext({
        queryResults: { 'system\\.peers': {
          rows: expectedHosts
            .filter(function (address) { return address !== '1:9042'; })
            .map(function (address) { return { 'rpc_address': address.split(':')[0] }; })
        }}
      }));
      cc.init(function (err) {
        var hosts = cc.hosts.values();
        cc.shutdown();
        assert.ifError(err);
        assert.deepEqual(hosts.map(function (h) { return h.address; }), expectedHosts);
        done();
      });
    }
    it('should resolve IPv4 and IPv6 addresses', function (done) {
      if (!useLocalhost) {
        return done();
      }
      var cc = newInstance({ contactPoints: [ 'localhost' ] }, getContext());
      cc.init(function (err) {
        cc.shutdown();
        assert.ifError(err);
        var hosts = cc.hosts.values();
        assert.strictEqual(hosts.length, 2);
        assert.deepEqual(hosts.map(function (h) { return h.address; }).sort(), [ '127.0.0.1:9042', '::1:9042' ]);
        done();
      });
    });
    it('should resolve IPv4 and IPv6 addresses with non default port', function (done) {
      if (!useLocalhost) {
        return done();
      }
      var cc = newInstance({ contactPoints: [ 'localhost:9999' ] }, getContext());
      cc.init(function (err) {
        cc.shutdown();
        assert.ifError(err);
        var hosts = cc.hosts.values();
        assert.ok(hosts.length >= 1);
        assert.strictEqual(hosts.filter(function (h) { return h.address === '127.0.0.1:9999'; }).length, 1);
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
    it('should continue iterating through the hosts when borrowing a connection fails', function (done) {
      var hosts = [];
      var cc = newInstance({ contactPoints: [ '::1', '::2' ] }, getContext({ hosts: hosts, failBorrow: [ 0 ] }));
      cc.init(function (err) {
        cc.shutdown();
        assert.ifError(err);
        assert.strictEqual(hosts.length, 2);
        assert.ok(cc.initialized);
        done();
      });
    });
    it('should callback with NoHostAvailableError when borrowing all connections fail', function (done) {
      var hosts = [];
      var cc = newInstance({ contactPoints: [ '::1', '::2' ] }, getContext({ hosts: hosts, failBorrow: [ 0, 1] }));
      cc.init(function (err) {
        cc.shutdown();
        helper.assertInstanceOf(err, errors.NoHostAvailableError);
        assert.strictEqual(Object.keys(err.innerErrors).length, 2);
        assert.strictEqual(hosts.length, 2);
        assert.ok(!cc.initialized);
        done();
      });
    });
    it('should continue iterating through the hosts when metadata retrieval fails', function (done) {
      var hosts = [];
      var cc = newInstance({ contactPoints: [ '::1', '::2' ] }, getContext({
        hosts: hosts, queryResults: { '::1': 'Test error, failed query' }
      }));
      cc.init(function (err) {
        cc.shutdown();
        assert.ifError(err);
        done();
      });
    });
    it('should listen to socketClose and reconnect', function (done) {
      var state = {};
      var hostsTried = [];
      var lbp = new policies.loadBalancing.RoundRobinPolicy();
      var cc = newInstance({ contactPoints: [ '::1', '::2' ], policies: { loadBalancing: lbp } }, getContext({
        state: state, hosts: hostsTried
      }));
      cc.init(function (err) {
        assert.ifError(err);
        assert.ok(state.connection);
        assert.strictEqual(hostsTried.length, 1);
        lbp.init(null, cc.hosts, utils.noop);
        state.connection.emit('socketClose');
        setImmediate(function () {
          // Attempted reconnection and succeeded
          assert.strictEqual(hostsTried.length, 2);
          cc.shutdown();
          done();
        });
      });
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
    it('should call the AddressTranslator', function (done) {
      var options = clientOptions.extend({}, helper.baseOptions);
      var address = null;
      var port = null;
      options.policies.addressResolution = policies.defaultAddressTranslator();
      options.policies.addressResolution.translate = function (addr, p, cb) {
        address = addr;
        port = p;
        cb(addr + ':' + p);
      };
      var cc = newInstance(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var row = {'rpc_address': getInet([5, 2, 3, 4]), peer: null};
      cc.getAddressForPeerHost(row, 9055, function (endPoint) {
        assert.strictEqual(endPoint, '5.2.3.4:9055');
        assert.strictEqual(address, '5.2.3.4');
        assert.strictEqual(port, 9055);
        done();
      });
    });
  });
  describe('#setPeersInfo()', function () {
    it('should use not add invalid addresses', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = newInstance(options);
      cc.host = new Host('18.18.18.18', 1, options);
      var rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1])},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1])},
        //should not be added
        {'rpc_address': null, peer: utils.allocBufferFromArray([1, 1, 1, 1])},
        //should use peer address
        {'rpc_address': getInet([0, 0, 0, 0]), peer: getInet([5, 5, 5, 5])}
      ];
      cc.setPeersInfo(true, null, { rows: rows }, function (err) {
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
      var rows = [
        //valid rpc address
        {'rpc_address': getInet([5, 4, 3, 2]), peer: getInet([1, 1, 1, 1]), data_center: 'dc100', release_version: '2.1.4'},
        //valid rpc address
        {'rpc_address': getInet([9, 8, 7, 6]), peer: getInet([1, 1, 1, 1]), data_center: 'dc101', release_version: '2.1.4'}
      ];
      cc.setPeersInfo(true, null, { rows: rows }, function (err) {
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
  describe('#refresh()', function () {
    it('should schedule reconnection when it cant borrow a connection', function (done) {
      var state = {};
      var hostsTried = [];
      var lbp = new policies.loadBalancing.RoundRobinPolicy();
      lbp.queryPlanCount = 0;
      lbp.newQueryPlan = function (ks, o, cb) {
        if (lbp.queryPlanCount++ === 0) {
          // Return an empty query plan the first time
          return cb(null, utils.arrayIterator([]));
        }
        return cb(null, utils.arrayIterator(lbp.hosts.values()));
      };
      var rp = new policies.reconnection.ConstantReconnectionPolicy(10);
      rp.nextDelayCount = 0;
      rp.newSchedule = function () {
        return {
          next: function () {
            rp.nextDelayCount++;
            return { value: 10, done: false};
          }
        };
      };
      var cc = newInstance({ contactPoints: [ '::1', '::2' ], policies: { loadBalancing: lbp, reconnection: rp } },
        getContext({ state: state, hosts: hostsTried }));
      cc.init(function (err) {
        assert.ifError(err);
        assert.ok(state.connection);
        assert.strictEqual(hostsTried.length, 1);
        lbp.init(null, cc.hosts, utils.noop);
        state.connection.emit('socketClose');
        var previousConnection = state.connection;
        setImmediate(function () {
          // Attempted reconnection and there isn't a host available
          assert.strictEqual(hostsTried.length, 1);
          // Scheduled reconnection
          assert.strictEqual(rp.nextDelayCount, 1);
          setTimeout(function () {
            // Reconnected
            assert.strictEqual(hostsTried.length, 2);
            // Changed connection
            assert.notEqual(state.connection, previousConnection);
            cc.shutdown();
            done();
          }, 20);
        });
      });
    });
  });
});

/**
 * @param {Array} bytes
 * @returns {exports.InetAddress}
 */
function getInet(bytes) {
  return new types.InetAddress(utils.allocBufferFromArray(bytes));
}

/** @return {ControlConnection} */
function newInstance(options, context) {
  options = clientOptions.extend(options || {});
  return new ControlConnection(options, new ProfileManager(options), context);
}

function getFakeConnection(endpoint, queryResults) {
  queryResults = queryResults || {};
  var c = new events.EventEmitter();
  c.protocolVersion = types.protocolVersion.maxSupported;
  c.endpoint = endpoint;
  c.connected = true;
  c.requests = [];
  var queryResultKeys = Object.keys(queryResults);
  var defaultResult = { rows: [ {} ] };
  c.sendStream = function (request, options, cb) {
    c.requests.push(request);
    var result;
    for (var i = 0; i < queryResultKeys.length; i++) {
      var key = queryResultKeys[i];
      var re = new RegExp(key);
      if (re.test(request.query) || re.test(endpoint)) {
        result = queryResults[key];
        break;
      }
    }
    if (typeof result === 'string') {
      return cb(new Error(result));
    }
    cb(null, result || defaultResult);
  };
  return c;
}

/**
 * Gets the ControlConnection context
 * @param {{hosts: Array|undefined, failBorrow: Array|undefined, queryResults: Object|undefined,
 *   state: Object|undefined}} [options]
 */
function getContext(options) {
  options = options || {};
  // hosts that the ControlConnection used to borrow a connection
  var hosts = options.hosts || [];
  var state = options.state || {};
  var failBorrow = options.failBorrow || [];
  return {
    borrowHostConnection: function (h, callback) {
      var i = hosts.length;
      hosts.push(h);
      state.host = h;
      if (failBorrow.indexOf(i) >= 0) {
        return callback(new Error('Test error'));
      }
      state.connection = getFakeConnection(h.address, options.queryResults);
      return callback(null, state.connection);
    }
  };
}