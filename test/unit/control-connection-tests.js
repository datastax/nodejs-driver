var assert = require('assert');
var async = require('async');

var helper = require('../test-helper.js');
var ControlConnection = require('../../lib/control-connection.js');
var Host = require('../../lib/host.js').Host;
var utils = require('../../lib/utils.js');
var clientOptions = require('../../lib/client-options.js');

describe('ControlConnection', function () {
  describe('#nodeSchemaChangeHandler()', function () {
    it('should update keyspace metadata information', function () {
      var cc = new ControlConnection(helper.baseOptions);
      cc.log = helper.noop;
      var ksName = 'dummy';
      //mock connection
      cc.connection = {
        sendStream: function (a, b, c) {
          c(null, {rows: [{'keyspace_name': ksName}]});
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
  describe('#getAddressForPeerHost()', function() {
    it('should handle null, 0.0.0.0 and valid addresses', function () {
      var options = clientOptions.extend({}, helper.baseOptions);
      var cc = new ControlConnection(options);
      cc.host = new Host('2.2.2.2', 1, options);
      cc.log = helper.noop;
      var peer = new Buffer([100, 100, 100, 100]);

      var row = {'rpc_address': new Buffer([1, 2, 3, 4]), peer: peer};
      var address = cc.getAddressForPeerHost(row);
      assert.strictEqual(address, '1.2.3.4');

      row = {'rpc_address': new Buffer([0, 0, 0, 0]), peer: peer};
      address = cc.getAddressForPeerHost(row);
      //should return peer address
      assert.strictEqual(address, '100.100.100.100');

      row = {'rpc_address': null, peer: peer};
      address = cc.getAddressForPeerHost(row);
      //should return peer address
      assert.strictEqual(address, null);
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
        {'rpc_address': new Buffer([5, 4, 3, 2]), peer: new Buffer([1, 1, 1, 1])},
        //valid rpc address
        {'rpc_address': new Buffer([9, 8, 7, 6]), peer: new Buffer([1, 1, 1, 1])},
        //should not be added
        {'rpc_address': null, peer: new Buffer([1, 1, 1, 1])},
        //should use peer address
        {'rpc_address': new Buffer([0, 0, 0, 0]), peer: new Buffer([5, 5, 5, 5])}
      ];
      cc.setPeersInfo(true, {rows: rows});
      assert.strictEqual(cc.hosts.length, 3);
      assert.ok(cc.hosts.get('5.4.3.2'));
      assert.ok(cc.hosts.get('9.8.7.6'));
      assert.ok(cc.hosts.get('5.5.5.5'));
    });
  });
});