var assert = require('assert');
var async = require('async');

var helper = require('../test-helper.js');
var ControlConnection = require('../../lib/control-connection.js');
var utils = require('../../lib/utils.js');

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
});