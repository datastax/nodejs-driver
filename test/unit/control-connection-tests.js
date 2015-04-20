var assert = require('assert');
var async = require('async');

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
      var cc = new ControlConnection(helper.baseOptions);
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
    it('should handle null, 0.0.0.0 and valid addresses', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = new ControlConnection(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var peer = getInet([100, 100, 100, 100]);

      var row = {'rpc_address': getInet([1, 2, 3, 4]), peer: peer};
      var address = cc.getAddressForPeerHost(row, 9042);
      assert.strictEqual(address, '1.2.3.4:9042');

      row = {'rpc_address': getInet([0, 0, 0, 0]), peer: peer};
      address = cc.getAddressForPeerHost(row, 9001);
      //should return peer address
      assert.strictEqual(address, '100.100.100.100:9001');

      row = {'rpc_address': null, peer: peer};
      address = cc.getAddressForPeerHost(row, 9042);
      //should return peer address
      assert.strictEqual(address, null);
    });
    it('should call the AddressTranslator', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var address = null;
      var port = null;
      options.policies.addressResolution = { translate: function (addr, p) {
        address = addr;
        port = p;
        return addr + ':' + p;
      }};
      var cc = new ControlConnection(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var row = {'rpc_address': getInet([5, 2, 3, 4]), peer: null};
      var endPoint = cc.getAddressForPeerHost(row, 9055);
      assert.strictEqual(endPoint, '5.2.3.4:9055');
      assert.strictEqual(address, '5.2.3.4');
      assert.strictEqual(port, 9055);
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
      cc.setPeersInfo(true, {rows: rows});
      assert.strictEqual(cc.hosts.length, 3);
      assert.ok(cc.hosts.get('5.4.3.2:9042'));
      assert.ok(cc.hosts.get('9.8.7.6:9042'));
      assert.ok(cc.hosts.get('5.5.5.5:9042'));
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