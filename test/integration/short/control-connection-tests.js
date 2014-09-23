var assert = require('assert');
var async = require('async');

var helper = require('../../test-helper.js');
var ControlConnection = require('../../../lib/control-connection.js');
var utils = require('../../../lib/utils.js');
var clientOptions = require('../../../lib/client-options.js');

var options = clientOptions.extend(helper.baseOptions);

describe('ControlConnection', function () {
  this.timeout(120000);
  describe('#init()', function () {
    beforeEach(helper.ccmHelper.start(2));
    afterEach(helper.ccmHelper.remove);
    it('should retrieve local host and peers', function (done) {
      var cc = new ControlConnection(options);
      cc.init(function () {
        assert.equal(cc.hosts.length, 2);
        cc.hosts.forEach(function (h) {
          assert.ok(h.datacenter);
          assert.ok(h.rack);
          assert.ok(h.tokens);
        });
        done();
      });
    });
    it('should subscribe to STATUS_CHANGE events', function (done) {
      var cc = new ControlConnection(options);
      cc.init(function () {
        helper.ccmHelper.exec(['node2', 'stop'], function (err) {
          if (err) return done(err);
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 2);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 1);
            done();
          }, 3000);
        });
      });
    });
    it('should subscribe to TOPOLOGY_CHANGE events and refresh ring info', function (done) {
      var cc = new ControlConnection(options);
      async.series([
        cc.init.bind(cc),
        function (next) {
          //add a node
          helper.ccmHelper.bootstrapNode(3, next);
        },
        function (next) {
          //start the node
          helper.ccmHelper.startNode(3, next);
        },
        function (next) {
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 3);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 2);
            next();
          }, 3000);
        }
      ], done);
    });
    it('should reconnect when host used goes down', function (done) {
      var cc = new ControlConnection(options);
      cc.init(function () {
        //initialize the load balancing policy
        options.policies.loadBalancing.init(null, cc.hosts, function () {});
        //it should be using the first node: kill it
        helper.ccmHelper.exec(['node1', 'stop'], function (err) {
          if (err) return done(err);
          //A little help here
          cc.hosts.slice(0)[0].setDown();
          setTimeout(function () {
            var hosts = cc.hosts.slice(0);
            assert.strictEqual(hosts.length, 2);
            var countUp = hosts.reduce(function (value, host) {
              value += host.isUp() ? 1 : 0;
              return value;
            }, 0);
            assert.strictEqual(countUp, 1);
            done();
          }, 3000);
        });
      });
    });
  });
});