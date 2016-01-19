var async = require('async');
var util = require('util');
var path = require('path');
var spawn = require('child_process').spawn;
var temp = require('temp').track(true);

//noinspection JSUnusedGlobalSymbols
var helper = {
  getDseVersion: function() {
    var version = process.env['TEST_DSE_VERSION'];
    if (!version) {
      version = '4.5.2';
    }
    return version;
  },
  /**
   * Determines if the current DSE instance version is greater than or equals to the version provided
   * @param {String} version The version in string format, dot separated.
   * @returns {Boolean}
   */
  isDseGreaterThan: function (version) {
    var instanceVersion = this.getDseVersion().split('.').map(function (x) { return parseInt(x, 10);});
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
  queries: {
    basic: "SELECT key FROM system.local",
    basicNoResults: "SELECT key from system.local WHERE key = 'not_existent'"
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
  baseOptions: {
    contactPoints: ['127.0.0.1']
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
  /**
   * Determines if test tracing is enabled
   */
  isTracing: function () {
    return (process.env['TEST_TRACE'] === 'on');
  },
  trace: function (format) {
    if (!helper.isTracing()) {
      return;
    }
    console.log('\t...' + util.format.apply(null, arguments));
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
  extend: function (target) {
    var sources = Array.prototype.slice.call(arguments, 1);
    sources.forEach(function (source) {
      for (var prop in source) {
        if (source.hasOwnProperty(prop)) {
          target[prop] = source[prop];
        }
      }
    });
    return target;
  },
  getOptions: function (options) {
    return helper.extend({}, helper.baseOptions, options);
  },
  ccm: {},
  ads: {}
};

/**
 * Removes previous and creates a new cluster (create, populate and start)
 * @param {Number|String} nodeLength number of nodes in the cluster. If multiple dcs, use the notation x:y:z:...
 * @param {{[vnodes]: Boolean, [yaml]: Array.<String>, [jvmArgs]: Array.<String>, [ssl]: Boolean}|null} options
 * @param {Function} callback
 */
helper.ccm.startAll = function (nodeLength, options, callback) {
  var self = this;
  options = options || {};
  var version = helper.getDseVersion();
  helper.trace('Starting test DSE cluster v%s with %s node(s)', version, nodeLength);
  async.series([
    function (next) {
      //it wont hurt to remove
      self.exec(['remove'], function () {
        //ignore error
        next();
      });
    },
    function (next) {
      var create = ['create', 'test', '--dse', '-v', version];
      if (process.env['TEST_DSE_DIR']) {
        create = ['create', 'test', '--install-dir=' + process.env['TEST_DSE_DIR']];
        helper.trace('With', create[2]);
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
      var i = 0;
      async.whilst(
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
      self.exec(populate, helper.wait(options.sleep, next));
    },
    function (next) {
      var start = ['start', '--wait-for-binary-proto'];
      if (util.isArray(options.jvmArgs)) {
        options.jvmArgs.forEach(function (arg) {
          start.push('--jvm_arg', arg);
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

helper.ccm.exec = function (params, callback) {
  this.spawn('ccm', params, callback);
};

helper.ccm.spawn = function (processName, params, callback) {
  if (!callback) {
    callback = function () {};
  }
  params = params || [];
  var originalProcessName = processName;
  if (process.platform.indexOf('win') === 0) {
    params = ['/c', processName].concat(params);
    processName = 'cmd.exe';
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

helper.ccm.remove = function (callback) {
  this.exec(['remove'], callback);
};

/**
 * Reads the logs to see if the cql protocol is up
 * @param callback
 */
helper.ccm.waitForUp = function (callback) {
  var started = false;
  var retryCount = 0;
  var self = this;
  async.whilst(function () {
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
helper.ccm.getPath = function (subPath) {
  var ccmPath = process.env.CCM_PATH;
  if (!ccmPath) {
    ccmPath = (process.platform === 'win32') ? process.env.HOMEPATH : process.env.HOME;
    ccmPath = path.join(ccmPath, 'workspace/tools/ccm');
  }
  return path.join(ccmPath, subPath);
};
/**
 * Conditionally executes func if testVersion is <= the current cassandra version.
 * @param {String} testVersion Minimum version of Cassandra needed.
 * @param {Function} func The function to conditionally execute.
 * @param {Array} args the arguments to apply to the function.
 */
function executeIfVersion (testVersion, func, args) {
  if (helper.isDseGreaterThan(testVersion)) {
    func.apply(this, args);
  }
}

helper.ads._execute = function(processName, params, cb) {
  var originalProcessName = processName;
  if (process.platform.indexOf('win') === 0) {
    params = ['/c', processName].concat(params);
    processName = 'cmd.exe';
  }
  helper.trace('Executing: ' + processName + ' ' + params.join(" "));

  // If process hasn't completed in 10 seconds.
  var timeout = undefined;
  if(cb) {
    timeout = setTimeout(function() {
      cb("Timed out while waiting for " + processName + " to complete.");
    }, 10000);
  }

  var p = spawn(processName, params, {env:{KRB5_CONFIG: path.join(this.dir, 'krb5.conf')}});
  p.stdout.setEncoding('utf8');
  p.stderr.setEncoding('utf8');
  p.stdout.on('data', function (data) {
    helper.trace("%s_out> %s", originalProcessName, data);
  });

  p.stderr.on('data', function (data) {
    helper.trace("%s_err> %s", originalProcessName, data);
  });

  p.on('close', function (code) {
    helper.trace("%s exited with code %d", originalProcessName, code);
    if(cb) {
      clearTimeout(timeout);
      if (code === 0) {
        cb();
      } else {
        cb(Error("Process exited with non-zero exit code: " + code));
      }
    }
  });

  return p;
};

/**
 * Starts the embedded-ads jar with ldap (port 10389) and kerberos enabled (port 10088).  Depends on ADS_JAR
 * environment variable to resolve the absolute file path of the embedded-ads jar.
 *
 * @param {Function} cb Callback to invoke when server is started and listening.
 */
helper.ads.start = function(cb) {
  var self = this;
  temp.mkdir('ads', function(err, dir) {
    if(err) {
      cb(err);
    }
    self.dir = dir;
    var jarFile = self.getJar();
    var processName = 'java';
    var params = ['-jar', jarFile, '-k', '--confdir', self.dir];
    var initialized = false;

    var timeout = setTimeout(function() {
      cb(new Error("Timed out while waiting for ADS server to start."));
    }, 10000);

    self.process = self._execute(processName, params, function() {
      if(!initialized) {
        cb();
      }
    });
    self.process.stdout.on('data', function (data) {
      // This is a big of a kludge, check for a particular log statement which indicates
      // that all principals have been created before invoking the completion callback.
      if(data.indexOf('Principal Initialization Complete.') != -1) {
        initialized = true;
        clearTimeout(timeout);
        cb();
      }
    });
  });
};

/**
 * Invokes a klist to list the current registered tickets and their expiration if trace is enabled.
 *
 * This is really only useful for debugging.
 *
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.listTickets = function(cb) {
  this._execute('klist', [], cb);
};

/**
 * Acquires a ticket for the given username and its principal.
 * @param {String} username Username to acquire ticket for (i.e. cassandra).
 * @param {String} principal Principal to acquire ticket for (i.e. cassandra@DATASTAX.COM).
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.acquireTicket = function(username, principal, cb) {
  var keytab = this.getKeytabPath(username);

  // Use ktutil on windows, kinit otherwise.
  var processName = 'kinit';
  var params = ['--verbose', '-t', keytab, principal];
  if (process.platform.indexOf('win') === 0) {
    // Not really sure what to do here yet...
  }
  this._execute(processName, params, cb);
};

/**
 * Destroys all tickets for the given principal.
 * @param {String} principal Principal for whom its tickets will be destroyed (i.e. dse/127.0.0.1@DATASTAX.COM).
 * @param {Function} cb Callback to invoke on completion.
 */
helper.ads.destroyTicket = function(principal, cb) {
  if (typeof principal === 'function') {
    cb = principal;
    principal = undefined;
  }

  // Use ktutil on windows, kdestroy otherwise.
  var processName = 'kdestroy';
  var params = principal === undefined ? ['--all'] : ['--principal=' + principal];
  if (process.platform.indexOf('win') === 0) {
    // Not really sure what to do here yet...
  }
  this._execute(processName, params, cb);
};

/**
 * Stops the server process.
 * @param {Function} cb Callback to invoke when server stopped or with an error.
 */
helper.ads.stop = function(cb) {
  if(this.process !== undefined) {
    if(this.process.exitCode) {
      helper.trace("Server already stopped with exit code %d.", this.process.exitCode);
      cb();
    } else {
      this.process.on('close', function () {
        cb();
      });
      this.process.on('error', cb);
      this.process.kill('SIGINT');
    }
  } else {
    cb(Error("Process is not defined."));
  }
};

/**
 * Gets the path of the embedded-ads jar.  Resolved from ADS_JAR environment variable or $HOME/embedded-ads.jar.
 */
helper.ads.getJar = function () {
  var adsJar = process.env.ADS_JAR;
  if (!adsJar) {
    helper.trace("ADS_JAR environment variable not set, using $HOME/embedded-ads.jar");
    adsJar = (process.platform === 'win32') ? process.env.HOMEPATH : process.env.HOME;
    adsJar = path.join(adsJar, 'embedded-ads.jar');
  }
  helper.trace("Using %s for embedded ADS server.", adsJar);
  return adsJar;
};

/**
 * Returns the file path to the keytab for the given user.
 * @param {String} username User to resolve keytab for.
 */
helper.ads.getKeytabPath = function(username) {
  return path.join(this.dir, username + ".keytab");
};

module.exports = helper;
