"use strict";
const assert = require('assert');
const util = require('util');

const helper = require('../../test-helper');
const Client = require('../../../lib/client');
const utils = require('../../../lib/utils');
const types = require('../../../lib/types');

describe('Client', function () {
  this.timeout(60000);
  context('with ssl enabled', function () {
    const keyspace = helper.getRandomName('ks');
    var table = keyspace + '.' + helper.getRandomName('table');
    var setupQueries = [
      helper.createKeyspaceCql(keyspace, 1),
      helper.createTableCql(table)
    ];
    before(helper.ccmHelper.start(1, { ssl: true }));
    before(helper.executeTask(newInstance(), setupQueries));
    after(helper.ccmHelper.remove);
    describe('#connect()', function () {
      it('should connect to a ssl enabled cluster', function (done) {
        const client = newInstance();
        client.connect(function (err) {
          assert.ifError(err);
          assert.strictEqual(client.hosts.length, 1);
          helper.finish(client, done)();
        });
      });
    });
    describe('#execute()', function () {
      it('should handle multiple requests in parallel with queueing', function (done) {
        const parallelLimit = helper.isCassandraGreaterThan('2.0') ? 2100 : 200;
        const client = newInstance();
        utils.series([
          client.connect.bind(client),
          function insert(next) {
            const query = util.format('INSERT INTO %s (id, text_sample) VALUES (?, ?)', table);
            utils.timesLimit(40000, parallelLimit, function (n, timesNext) {
              client.execute(query, [ types.Uuid.random(), 'value ' + n ], { prepare: true }, timesNext);
            }, next);
          }
        ], helper.finish(client, done));
      });
    });
  });
});

/** @returns {Client}  */
function newInstance(options) {
  return new Client(utils.deepExtend({ sslOptions: {} }, helper.baseOptions, options));
}