'use strict';
var assert = require('assert');
var async = require('async');
var version = require('../../index.js').version;
var helper = require('../helper');
var cassandra = require('cassandra-driver');
var DseClient = require('../../lib/dse-client');

describe('DseClient', function() {
  this.timeout(60000);
  before(function(done) {
    helper.ccm.startAll(1, {}, done);
  });
  after(helper.ccm.remove.bind(helper.ccm));
  it('should log the module versions on first connect only', function(done) {
    var client = new DseClient(helper.getOptions());
    var versionLogRE = /Using DSE driver v(.*) with core driver v(.*)/;
    var versionMessage = undefined;

    client.on('log', function(level, className, message) {
      var match = message.match(versionLogRE);
      if(match) {
        versionMessage = { level: level, match: match };
      }
    });

    async.series([
      client.connect.bind(client),
      function ensureLogged(next) {
        assert.ok(versionMessage);
        assert.strictEqual(versionMessage.level, 'info');
        // versions should match those from the modules.
        assert.strictEqual(versionMessage.match[1], version);
        assert.strictEqual(versionMessage.match[2], cassandra.version);
        versionMessage = undefined;
        next();
      },
      client.connect.bind(client),
      function ensureNotLogged(next) {
        assert.strictEqual(versionMessage, undefined);
        next();
      },
      client.shutdown.bind(client)
    ],done);
  });
});
