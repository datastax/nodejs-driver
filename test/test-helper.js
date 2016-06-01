"use strict";
var assert = require('assert');
var util = require('util');
var path = require('path');
var policies = require('../lib/policies');
var types = require('../lib/types');
var utils = require('../lib/utils.js');

util.inherits(RetryMultipleTimes, policies.retry.RetryPolicy);

var helper = {
  /**
   * Sync throws the error
   * @type Function
   */
  throwop: function (err) {
    if (err) throw err;
  },
  /** @type Function */
  noop: function () {
    //do nothing
  },
  /** @type Function */
  failop: function () {
    throw new Error('Method should not be called');
  },
  /**
   * Uses the last parameter as callback, invokes it via setImmediate
   */
  callbackNoop: function () {
    var args = Array.prototype.slice.call(arguments);
    var cb = args[args.length-1];
    if (typeof cb !== 'function') {
      throw new Error('Helper method needs a callback as last parameter');
    }
    setImmediate(cb);
  },
  /**
   * @type {ClientOptions}
   */
  baseOptions: (function () {
    return {
      //required
      contactPoints: ['127.0.0.1'],
      // retry all queries multiple times (for improved test resiliency).
      policies: { retry: new RetryMultipleTimes(3) }
    };
  })(),
  /**
   * Returns a pseudo-random name in the form of 'ab{n}', n being an int zero padded with string length 16
   * @returns {string}
   */
  getRandomName: function (prefix) {
    if (!prefix) {
      prefix = 'ab';
    }
    var value = Math.floor(Math.random() * utils.maxInt);
    return prefix + ('000000000000000' + value.toString()).slice(-16);
  },
  ipPrefix: '127.0.0.',
  Ccm: Ccm,
  ccmHelper: {
    /**
     * @returns {Function}
     */
    start: function (nodeLength, options) {
      return (function (done) {
        new Ccm().startAll(nodeLength, options, function (err) {
          done(err);
        });
      });
    },
    remove: function (callback) {
      new Ccm().remove(callback);
    },
    removeIfAny: function (callback) {
      new Ccm().remove(function () {
        //ignore err
        if (callback) callback();
      });
    },
    pauseNode: function (nodeIndex, callback) {
      new Ccm().exec(['node' + nodeIndex, 'pause'], callback);
    },
    resumeNode: function (nodeIndex, callback) {
      new Ccm().exec(['node' + nodeIndex, 'resume'], callback);
    },
    /**
     * Adds a new node to the cluster
     * @param {Number} nodeIndex 1 based index of the node
     * @param {Function} callback
     */
    bootstrapNode: function (nodeIndex, callback) {
      var ipPrefix = helper.ipPrefix;
      new Ccm().exec([
        'add',
        'node' + nodeIndex,
        '-i',
        ipPrefix + nodeIndex,
        '-j',
        (7000 + 100 * nodeIndex).toString(),
        '-b'
      ], callback);
    },
    /**
     * @param {Number} nodeIndex 1 based index of the node
     * @param {Function} callback
     */
    startNode: function (nodeIndex, callback) {
      var args = ['node' + nodeIndex, 'start', '--wait-other-notice', '--wait-for-binary-proto'];
      if (process.platform.indexOf('win') === 0 && helper.isCassandraGreaterThan('2.2.4')) {
        args.push('--quiet-windows')
      }
      new Ccm().exec(args, callback);
    },
    /**
     * @param {Number} nodeIndex 1 based index of the node
     * @param {Function} callback
     */
    stopNode: function (nodeIndex, callback) {
      new Ccm().exec(['node' + nodeIndex, 'stop'], callback);
    },
    /**
     * @param {Number} nodeIndex 1 based index of the node
     * @param {Function} callback
     */
    decommissionNode: function (nodeIndex, callback) {
      new Ccm().exec(['node' + nodeIndex, 'decommission'], callback);
    },
    exec: function (params, callback) {
      new Ccm().exec(params, callback);
    }
  },
  /**
   * Returns a cql string with a CREATE TABLE command containing all common types
   * @param {String} tableName
   * @returns {String}
   */
  createTableCql: function (tableName) {
    return  util.format('CREATE TABLE %s (' +
      '   id uuid primary key,' +
      '   ascii_sample ascii,' +
      '   text_sample text,' +
      '   int_sample int,' +
      '   bigint_sample bigint,' +
      '   float_sample float,' +
      '   double_sample double,' +
      '   decimal_sample decimal,' +
      '   blob_sample blob,' +
      '   boolean_sample boolean,' +
      '   timestamp_sample timestamp,' +
      '   inet_sample inet,' +
      '   timeuuid_sample timeuuid,' +
      '   map_sample map<text, text>,' +
      '   list_sample list<text>,' +
      '   list_sample2 list<int>,' +
      '   set_sample set<text>)', tableName);
  },
  /**
   * Returns a cql string with a CREATE TABLE command 1 partition key and 1 clustering key
   * @param {String} tableName
   * @returns {String}
   */
  createTableWithClusteringKeyCql: function (tableName) {
    return  util.format('CREATE TABLE %s (' +
    '   id1 uuid,' +
    '   id2 timeuuid,' +
    '   text_sample text,' +
    '   int_sample int,' +
    '   bigint_sample bigint,' +
    '   float_sample float,' +
    '   double_sample double,' +
    '   map_sample map<uuid, int>,' +
    '   list_sample list<timeuuid>,' +
    '   set_sample set<int>,' +
    '   PRIMARY KEY (id1, id2))', tableName);
  },
  createKeyspaceCql: function (keyspace, replicationFactor, durableWrites) {
    return util.format('CREATE KEYSPACE %s' +
      ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : %d}' +
      ' AND durable_writes = %s;',
      keyspace,
      replicationFactor || 1,
      !!durableWrites
    );
  },
  assertValueEqual: function (val1, val2) {
    if (val1 === null && val2 === null) {
      return;
    }
    if (val1 instanceof Buffer && val2 instanceof Buffer) {
      val1 = val1.toString('hex');
      val2 = val2.toString('hex');
    }
    if ((val1 instanceof types.Long && val2 instanceof types.Long) ||
        (val1 instanceof Date && val2 instanceof Date) ||
        (val1 instanceof types.InetAddress && val2 instanceof types.InetAddress) ||
        (val1 instanceof types.Uuid && val2 instanceof types.Uuid)) {
      val1 = val1.toString();
      val2 = val2.toString();
    }
    if (util.isArray(val1) ||
        (val1.constructor && val1.constructor.name === 'Object') ||
        val1 instanceof helper.Map) {
      val1 = util.inspect(val1, {depth: null});
      val2 = util.inspect(val2, {depth: null});
    }
    assert.strictEqual(val1, val2);
  },
  assertInstanceOf: function (instance, constructor) {
    assert.notEqual(instance, null, 'Expected instance, obtained ' + instance);
    assert.ok(instance instanceof constructor, 'Expected instance of ' + constructor.name + ', actual constructor: ' + instance.constructor.name);
  },
  assertNotInstanceOf: function (instance, constructor) {
    assert.notEqual(instance, null, 'Expected instance, obtained ' + instance);
    assert.ok(!(instance instanceof constructor), 'Expected instance different than ' + constructor.name + ', actual constructor: ' + instance.constructor.name);
  },
  assertContains: function (value, searchValue, caseInsensitive) {
    assert.strictEqual(typeof value, 'string');
    var message = 'String: "%s" does not contain "%s"';
    if (caseInsensitive !== false) {
      value = value.toLowerCase();
      searchValue = searchValue.toLowerCase();
    }
    assert.ok(value.indexOf(searchValue) >= 0, util.format(message, value, searchValue));
  },
  /**
   * Returns a function that waits on schema agreement before executing callback
   * @param {Client} client
   * @param {Function} callback
   * @returns {Function}
   */
  waitSchema: function (client, callback) {
    return (function (err) {
      if (err) return callback(err);
      if (!client.hosts) {
        throw new Error('No hosts on Client')
      }
      if (client.hosts.length === 1) {
        return callback();
      }
      setTimeout(callback, 200 * client.hosts.length);
    });
  },
  /**
   * @returns {Function} A function with a single callback param, applying the fn with parameters
   */
  toTask: function (fn, context) {
    var params = Array.prototype.slice.call(arguments, 2);
    return (function (next) {
      params.push(next);
      fn.apply(context, params);
    });
  },
  wait: function (ms, callback) {
    if (!ms) {
      ms = 0;
    }
    return (function (err) {
      if (err) return callback(err);
      setTimeout(callback, ms);
    });
  },
  getCassandraVersion: function() {
    //noinspection JSUnresolvedVariable
    var version = process.env.TEST_CASSANDRA_VERSION;
    if (!version) {
      version = '3.0.5';
    }
    return version;
  },
  /**
   * Determines if the current Cassandra instance version is greater than or equals to the version provided
   * @param {String} version The version in string format, dot separated.
   * @returns {Boolean}
   */
  isCassandraGreaterThan: function (version) {
    var instanceVersion = this.getCassandraVersion().split('.').map(function (x) { return parseInt(x, 10);});
    var compareVersion = version.split('.').map(function (x) { return parseInt(x, 10) || 0;});
    for (var i = 0; i < compareVersion.length; i++) {
      var compare = compareVersion[i] || 0;
      if (instanceVersion[i] > compare) {
        //is greater
        return true;
      }
      else if (instanceVersion[i] < compare) {
        //is smaller
        return false;
      }
    }
    //are equal
    return true;
  },
  log: function(levels) {
    if (!levels) {
      levels = ['info', 'warning', 'error'];
    }
    return (function (l) {
      if (levels.indexOf(l) >= 0) {
        //noinspection JSUnresolvedVariable
        console.log.apply(console, arguments);
      }
    });
  },
  /**
   * @returns {Array}
   */
  fillArray: function (length, val) {
    var result = new Array(length);
    for (var i = 0; i < length; i++) {
      result[i] = val;
    }
    return result;
  },
  /**
   * @returns {Array}
   */
  iteratorToArray: function (iterator) {
    var result = [];
    var item = iterator.next();
    while (!item.done) {
      result.push(item.value);
      item = iterator.next();
    }
    return result;
  },
  /**
   * @param arr
   * @param {Function|String} predicate function to compare or property name to compare
   * @param val
   * @returns {*}
   */
  find: function (arr, predicate, val) {
    if (arr == null) {
      throw new TypeError('Array.prototype.find called on null or undefined');
    }
    if (typeof predicate === 'string') {
      var propName = predicate;
      predicate = function (item) {
        return (item && item[propName] === val);
      };
    }
    if (typeof predicate !== 'function') {
      throw new TypeError('predicate must be a function');
    }
    var value;
    for (var i = 0; i < arr.length; i++) {
      value = arr[i];
      if (predicate.call(null, value, i, arr)) {
        return value;
      }
    }
    return undefined;
  },
  /**
   * @param {Array} arr
   * @param {Function }predicate
   */
  first: function (arr, predicate) {
    var filterArr = arr.filter(predicate);
    if (filterArr.length === 0) {
      throw new Error('Item not found: ' + predicate);
    }
    return filterArr[0];
  },
  /**
   * Returns the values of an object
   * @param {Object} obj
   */
  values : function (obj) {
    var vals = [];
    for (var key in obj) {
      if (!obj.hasOwnProperty(key)) {
        continue;
      }
      vals.push(obj[key]);
    }
    return vals;
  },
  Map: MapPolyFill,
  Set: SetPolyFill,
  WhiteListPolicy: WhiteListPolicy,
  /**
   * Determines if test tracing is enabled
   */
  isTracing: function () {
    return (process.env.TEST_TRACE === 'on');
  },
  trace: function (format) {
    if (!helper.isTracing()) {
      return;
    }
    console.log('\t...' + util.format.apply(null, arguments));
  },

  /**
   * Version dependent it() method for mocha test case
   * @param {String} testVersion Minimum version of Cassandra needed for this test
   * @param {String} testCase Test case name
   * @param {Function} func
   */
  vit: function (testVersion, testCase, func) {
    executeIfVersion(testVersion, it, [testCase, func]);
  },

  /**
   * Version dependent describe() method for mocha test case
   * @param {String} testVersion Minimum version of Cassandra needed for this test
   * @param {String} title Title of the describe section.
   * @param {Function} func
   */
  vdescribe: function (testVersion, title, func) {
    executeIfVersion(testVersion, describe, [title, func]);
  },

  /**
   * Given a {Host} returns the last octet of its ip address.
   * i.e. (127.0.0.247:9042) -> 247.
   *
   * @param {Host|string} host or host address to get ip address of.
   * @returns {string} Last octet of the host address.
   */
  lastOctetOf: function(host) {
    var address = typeof host == "string" ? host : host.address;
    var ipAddress = address.split(':')[0].split('.');
    return ipAddress[ipAddress.length-1];
  },

  /**
   * Given a {Client} and a {Number} returns the host whose last octet
   * ends with the requested number.
   * @param {Client|ControlConnection} client Client to lookup hosts from.
   * @param {Number} number last octet of requested host.
   * @returns {Host}
   */
  findHost: function(client, number) {
    var host = null;
    var self = this;
    client.hosts.forEach(function(h) {
      if(self.lastOctetOf(h) == number) {
        host = h;
      }
    });
    return host;
  },

  /**
   * Waits indefinitely for a specific host to emit an event after a
   * a given function has been called and invokes a callback on completion.
   * @param {Function} f function to call
   * @param {Client|ControlConnection} client client to inspect hosts from.
   * @param {Number} number last octet of requested host.
   * @param {String} event event to wait on, i.e. 'up'.
   * @param {Function} cb function to call when event has been received.
   */
  waitOnHost: function(f, client, number, event, cb) {
    var host = this.findHost(client, number);
    host.once(event, function () {
      cb();
    });
    f();
  },

  /**
   * Returns a function, that when invoked shutdowns the client and callbacks
   * @param {Client} client
   * @param {Function} callback
   * @returns {Function}
   */
  finish: function (client, callback) {
    return (function (err) {
      assert.ifError(err);
      client.shutdown(callback);
    });
  },

  /**
   * Executes a function at regular intervals while the condition is false or the amount of attempts >= maxAttempts.
   * @param {Function} condition
   * @param {Number} delay
   * @param {Number} maxAttempts
   * @param {Function} done
   */
  setIntervalUntil: function (condition, delay, maxAttempts, done) {
    var attempts = 0;
    utils.whilst(
      function whilstCondition() {
        return !condition();
      },
      function whilstItem(next) {
        if (attempts++ >= maxAttempts) {
          return next(new Error(util.format('Condition still false after %d attempts: %s', maxAttempts, condition)));
        }

        setTimeout(next, delay);
      },
      done);
  },
  /**
   * Returns a method that executes a function at regular intervals while the condition is false or the amount of
   * attempts >= maxAttempts.
   * @param {Function} condition
   * @param {Number} delay
   * @param {Number} maxAttempts
   */
  setIntervalUntilTask: function (condition, delay, maxAttempts) {
    var self = this;
    return (function setIntervalUntilHandler(done) {
      self.setIntervalUntil(condition, delay, maxAttempts, done);
    });
  },
  /**
   * Returns a method that delays invoking the callback
   */
  delay: function (delayMs) {
    return (function delayedHandler(next) {
      setTimeout(next, delayMs);
    });
  },
  queries: {
    basic: "SELECT key FROM system.local",
    basicNoResults: "SELECT key from system.local WHERE key = 'not_existent'"
  },
  /**
   * @param {Object} o1
   * @param {Object} o2
   * @param {Array.<string>} props
   * @param {Array.<string>} [except]
   */
  compareProps: function (o1, o2, props, except) {
    assert.ok(o1);
    if (except) {
      props = props.slice(0);
      except.forEach(function (p) {
        var index = props.indexOf(p);
        if (index >= 0) {
          props.splice(index, 1);
        }
      });
    }
    props.forEach(function comparePropItem(p) {
      assert.strictEqual(o1[p], o2[p]);
    });
  }
};

function Ccm() {
  //Use an instance to maintain state
}

/**
 * Removes previous and creates a new cluster (create, populate and start)
 * @param {Number|String} nodeLength number of nodes in the cluster. If multiple dcs, use the notation x:y:z:...
 * @param {{vnodes: Boolean, yaml: Array, jvmArgs: Array, ssl: Boolean, sleep: Number, ipFormat: String}} options
 * @param {Function} callback
 */
Ccm.prototype.startAll = function (nodeLength, options, callback) {
  var self = this;
  options = options || {};
  var version = helper.getCassandraVersion();
  helper.trace('Starting test C* cluster v%s with %s node(s)', version, nodeLength);
  utils.series([
    function (next) {
      //it wont hurt to remove
      self.exec(['remove'], function () {
        //ignore error
        next();
      });
    },
    function (next) {
      var create = ['create', 'test', '-v', version];
      if (process.env.TEST_CASSANDRA_DIR) {
        create = ['create', 'test', '--install-dir=' + process.env.TEST_CASSANDRA_DIR];
        helper.trace('With', create[2]);
      }
      else if (process.env.TEST_CASSANDRA_BRANCH) {
        create = ['create', 'test', '-v', process.env.TEST_CASSANDRA_BRANCH];
        helper.trace('With branch', create[3]);
      }
      if (options.ssl) {
        create.push('--ssl', self.getPath('ssl'));
      }
      self.exec(create, helper.wait(options.sleep, next));
    },
    function (next) {
      if (!options.yaml) {
        return next();
      }
      helper.trace('With conf', options.yaml);
      var i = 0;
      utils.whilst(
        function condition() {
          return i < options.yaml.length
        },
        function iterator(whilstNext) {
          self.exec(['updateconf', options.yaml[i++]], whilstNext);
        },
        next
      );
    },
    function (next) {
      var populate = ['populate', '-n', nodeLength.toString()];
      if (options.vnodes) {
        populate.push('--vnodes');
      }
      if (options.ipFormat) {
        populate.push('--ip-format='+ options.ipFormat);
      }
      self.exec(populate, helper.wait(options.sleep, next));
    },
    function (next) {
      var start = ['start', '--wait-for-binary-proto'];
      if (process.platform.indexOf('win') === 0 && helper.isCassandraGreaterThan('2.2.4')) {
        start.push('--quiet-windows')
      }
      if (util.isArray(options.jvmArgs)) {
        options.jvmArgs.forEach(function (arg) {
          // Windows requires jvm arguments to be quoted, while *nix requires unquoted.
          var jvmArg = process.platform.indexOf('win') === 0 ? '"' + arg + '"' : arg;
          start.push('--jvm_arg', jvmArg);
        }, this);
        helper.trace('With jvm args', options.jvmArgs);
      }
      self.exec(start, helper.wait(options.sleep, next));
    },
    self.waitForUp.bind(self)
  ], function (err) {
    callback(err);
  });
};

Ccm.prototype.exec = function (params, callback) {
  this.spawn('ccm', params, callback);
};

Ccm.prototype.spawn = function (processName, params, callback) {
  if (!callback) {
    callback = function () {};
  }
  params = params || [];
  var originalProcessName = processName;
  var spawn = require('child_process').spawn;
  if (process.platform.indexOf('win') === 0) {
    params = ['-ExecutionPolicy', 'Unrestricted', processName].concat(params);
    processName = 'powershell.exe';
  }
  var p = spawn(processName, params);
  var stdoutArray= [];
  var stderrArray= [];
  var closing = 0;
  p.stdout.setEncoding('utf8');
  p.stderr.setEncoding('utf8');
  p.stdout.on('data', function (data) {
    stdoutArray.push(data);
  });

  p.stderr.on('data', function (data) {
    stderrArray.push(data);
  });

  p.on('close', function (code) {
    if (closing++ > 0) {
      //avoid calling multiple times
      return;
    }
    var info = {code: code, stdout: stdoutArray, stderr: stderrArray};
    var err = null;
    if (code !== 0) {
      err = new Error(
          'Error executing ' + originalProcessName + ':\n' +
          info.stderr.join('\n') +
          info.stdout.join('\n')
      );
      err.info = info;
    }
    callback(err, info);
  });
};

Ccm.prototype.remove = function (callback) {
  this.exec(['remove'], callback);
};

/**
 * Reads the logs to see if the cql protocol is up
 * @param callback
 */
Ccm.prototype.waitForUp = function (callback) {
  var started = false;
  var retryCount = 0;
  var self = this;
  utils.whilst(function () {
    return !started && retryCount < 10;
  }, function iterator (next) {
    self.exec(['node1', 'showlog'], function (err, info) {
      if (err) return next(err);
      var regex = /Starting listening for CQL clients/mi;
      started = regex.test(info.stdout.join(''));
      retryCount++;
      if (!started) {
        //wait 1 sec between retries
        return setTimeout(next, 1000);
      }
      return next();
    });
  }, callback);
};

/**
 * Gets the path of the ccm
 * @param subPath
 */
Ccm.prototype.getPath = function (subPath) {
  var ccmPath = process.env.CCM_PATH;
  if (!ccmPath) {
    ccmPath = (process.platform === 'win32') ? process.env.HOMEPATH : process.env.HOME;
    ccmPath = path.join(ccmPath, 'workspace/tools/ccm');
  }
  return path.join(ccmPath, subPath);
};

/**
 * A polyfill of Map, valid for testing. It does not support update of values
 * @constructor
 */
function MapPolyFill(arr) {
  this.arr = arr || [];
  var self = this;
  Object.defineProperty(this, 'size', {
    get: function() { return self.arr.length; },
    configurable: false
  });
}

MapPolyFill.prototype.set = function (k, v) {
  this.arr.push([k, v]);
};

MapPolyFill.prototype.get = function (k) {
  return this.arr.filter(function (item) {
    return item[0] === k;
  })[0];
};

MapPolyFill.prototype.forEach = function (callback) {
  this.arr.forEach(function (item) {
    //first the value, then the key
    callback(item[1], item[0]);
  });
};

MapPolyFill.prototype.toString = function() {
  return this.arr.toString();
};

function SetPolyFill(arr) {
  this.arr = arr || [];
}

SetPolyFill.prototype.forEach = function (cb, thisArg) {
  this.arr.forEach(cb, thisArg);
};

SetPolyFill.prototype.add = function (x) {
  this.arr.push(x);
};

SetPolyFill.prototype.toString = function() {
  return this.arr.toString();
};

/**
 * A retry policy for testing purposes only, retries for a number of times
 * @param {Number} times
 * @constructor
 */
function RetryMultipleTimes(times) {
  this.times = times;
}

RetryMultipleTimes.prototype.onReadTimeout = function (requestInfo) {
  if (requestInfo.nbRetry > this.times) {
    return this.rethrowResult();
  }
  return this.retryResult();
};

RetryMultipleTimes.prototype.onUnavailable = function (requestInfo) {
  if (requestInfo.nbRetry > this.times) {
    return this.rethrowResult();
  }
  return this.retryResult();
};

RetryMultipleTimes.prototype.onWriteTimeout = function (requestInfo) {
  if (requestInfo.nbRetry > this.times) {
    return this.rethrowResult();
  }
  return this.retryResult();
};

/**
 * For test purposes, filters the child policy by last octet of the ip address
 * @param {Array} list
 * @param [childPolicy]
 * @constructor
 */
function WhiteListPolicy(list, childPolicy) {
  this.list = list;
  this.childPolicy = childPolicy || new policies.loadBalancing.RoundRobinPolicy();
}

util.inherits(WhiteListPolicy, policies.loadBalancing.LoadBalancingPolicy);

WhiteListPolicy.prototype.init = function (client, hosts, callback) {
  this.childPolicy.init(client, hosts, callback);
};

WhiteListPolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var list = this.list;
  this.childPolicy.newQueryPlan(keyspace, queryOptions, function (err, iterator) {
    callback(err, {
      next: function () {
        var item = iterator.next();
        while (!item.done) {
          //noinspection JSCheckFunctionSignatures
          if (list.indexOf(helper.lastOctetOf(item.value)) >= 0) {
            break;
          }
          item = iterator.next();
        }
        return item;
      }
    });
  });
};

/**
 * Conditionally executes func if testVersion is <= the current cassandra version.
 * @param {String} testVersion Minimum version of Cassandra needed.
 * @param {Function} func The function to conditionally execute.
 * @param {Array} args the arguments to apply to the function.
 */
function executeIfVersion (testVersion, func, args) {
  if (helper.isCassandraGreaterThan(testVersion)) {
    func.apply(this, args);
  }
}

module.exports = helper;
module.exports.RetryMultipleTimes = RetryMultipleTimes;
