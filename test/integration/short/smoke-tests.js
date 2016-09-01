"use strict";
var assert = require('assert');

var helper = require('../../test-helper.js');
var utils = require('../../../lib/utils.js');
var Client = require('../../../lib/client.js');
var Connection = require('../../../lib/connection.js');
var defaultOptions = require('../../../lib/client-options.js').defaultOptions();

describe('Smoke Tests', function () {
  this.timeout(120000);
  describe('with single node cluster', function () {
    var keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    var selectAllQuery = 'SELECT * FROM ' + table;
    var client = newInstance();
    before(function (done) {
      utils.series([
        helper.ccmHelper.start(1),
        function (next) {
          client.execute(helper.createKeyspaceCql(keyspace, 1), next);
        },
        function (next) {
          client.execute(helper.createTableCql(table), next);
        }
      ], done);
    });
    after(function (done) {
      utils.series([
        client.shutdown.bind(client),
        helper.ccmHelper.remove
      ], done);
    });

    var maxProtocolVersion = getProtocolVersion();
    var minProtocolVersion = getMinProtocolVersion();
    for (var p = maxProtocolVersion; p >= minProtocolVersion; p--) {
      describe('With protocol version ' + p, function () {
        var protocolVersion = p;
        var protocolClient = newInstance({ protocolOptions: { maxVersion: protocolVersion }});
        it('should connect with that protocol version', function (done) {
          var contactPoint = helper.baseOptions.contactPoints[0] + ':' + defaultOptions.protocolOptions.port;
          var options = utils.deepExtend({logEmitter: helper.noop}, defaultOptions);
          var localCon = new Connection(contactPoint, protocolVersion, options);
          localCon.open(function (err) {
            assert.ifError(err);
            assert.ok(localCon.connected, 'Must be status connected');
            assert.equal(localCon.protocolVersion, protocolVersion);
            localCon.close(done);
          });
        });
        it('should execute a basic query', function (done) {
          protocolClient.execute(helper.queries.basic, function (err, result) {
            assert.equal(err, null);
            assert.notEqual(result, null);
            assert.notEqual(result.rows, null);
            done();
          });
        });
        after(protocolClient.shutdown.bind(protocolClient));
      });
    }
    it('should handle 500 parallel queries', function (done) {
      utils.times(500, function (n, next) {
        client.execute(helper.queries.basic, [], next);
      }, done)
    });
    it('should handle several concurrent executes while the pool is not ready', function (done) {
      var client = newInstance({pooling: {
        coreConnectionsPerHost: {
          //lots of connections per host
          '0': 100,
          '1': 1,
          '2': 0
        }}});
      var execute = function (next) {
        client.execute(selectAllQuery, next);
      };
      utils.parallel([
        function (parallelNext) {
          utils.parallel(helper.fillArray(400, execute), parallelNext);
        },
        function (parallelNext) {
          utils.times(200, function (n, next) {
            setTimeout(function () {
              execute(next);
            }, n * 5 + 50);
          }, parallelNext);
        }
      ], function () {
        client.shutdown(done);
      });
    });
  });
});

function newInstance(options) {
  return new Client(utils.deepExtend({}, helper.baseOptions, options));
}

/**
 * Gets the max supported protocol version for the current Cassandra version
 * @returns {number}
 */
function getProtocolVersion() {
  //expected protocol version
  if (helper.getCassandraVersion().indexOf('2.1.') === 0) {
    return 3;
  }
  if (helper.getCassandraVersion().indexOf('2.0.') === 0) {
    return 2;
  }
  if (helper.getCassandraVersion().indexOf('1.') === 0) {
    return 1;
  }
  return 4;
}

/**
 * Gets the minimum supported protocol version for the current Cassandra version
 *
 * For < C* 3.0 returns 1.  Otherwise returns maximum supported protocol
 * version - 1.
 *
 * @returns {number}
 */
function getMinProtocolVersion() {
  if (helper.getCassandraVersion().indexOf('2') === 0
    || helper.getCassandraVersion().indexOf('1') === 0) {
    return 1;
  } else {
    return getProtocolVersion() - 1;
  }
}
