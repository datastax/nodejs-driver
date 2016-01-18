'use strict';
var util = require('util');
/**
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} user
 * @param {String} host
 * @ignore
 * @constructor
 */
function GssClient(kerberosModule, user, host) {
  this.kerberosModule = kerberosModule;
  this.user = user;
  this.host = host;
}

/**
 * Factory to get the actual implementation of GSSAPI (unix or win)
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} user The primary / service part.
 * @param {String} host The host.
 * @returns GssClient
 */
GssClient.createNew = function (kerberosModule, user, host) {
  return new StandardGssClient(kerberosModule, user, host);
};

/**
 * @abstract
 * @param {Function} callback
 */
GssClient.prototype.init = function (callback) {
  throw new Error('Not implemented');
};

/**
 * @param {Buffer} challenge
 * @param {Function} callback
 * @abstract
 * @returns {Buffer}
 */
GssClient.prototype.evaluateChallenge = function (challenge, callback) {
  throw new Error('Not implemented');
};

/**
 * @abstract
 * @param {Function} [callback]
 */
GssClient.prototype.shutdown = function (callback) {
  throw new Error('Not implemented');
};

/**
 * @ignore
 * @extends GssClient
 * @constructor
 */
function StandardGssClient(kerberosModule, user, host) {
  GssClient.call(this, kerberosModule, user, host);
  this.kerberos = new kerberosModule.Kerberos();
  this.transitionIndex = 0;
}

util.inherits(StandardGssClient, GssClient);

StandardGssClient.prototype.init = function (callback) {
  //service@host
  var uri = this.user;
  if (this.host) {
    uri = util.format("%s@%s", this.user, this.host);
  }
  var self = this;
  this.kerberos.authGSSClientInit(
    uri, //dse@127.0.0.1
    this.kerberosModule.Kerberos.GSS_C_MUTUAL_FLAG, //authenticate itself flag
    function authInitUnixCallback(err, context) {
      if (err) {
        return callback(err);
      }
      /** @type {{response}} */
      self.context = context;
      callback();
    });
};

/** @override */
StandardGssClient.prototype.evaluateChallenge = function (challenge, callback) {
  var self = this;
  this['transition' + this.transitionIndex](challenge, function (err) {
    if (err) {
      return callback(err);
    }
    self.transitionIndex++;
    var result = null;
    if (typeof self.context.response === 'string') {
      result = new Buffer(self.context.response, 'base64');
    }
    else if (!self.context.response) {
      result = new Buffer(0);
    }
    callback(null, result);
  });
};

//noinspection JSUnusedGlobalSymbols
StandardGssClient.prototype.transition0 = function (challenge, callback) {
  this.kerberos.authGSSClientStep(this.context, '', callback);
};

//noinspection JSUnusedGlobalSymbols
StandardGssClient.prototype.transition1 = function (challenge, callback) {
  var charPointerChallenge = challenge.toString('base64');
  this.kerberos.authGSSClientStep(this.context, charPointerChallenge, callback);
};

//noinspection JSUnusedGlobalSymbols
StandardGssClient.prototype.transition2 = function (challenge, callback) {
  var self = this;
  this.kerberos.authGSSClientUnwrap(self.context, challenge.toString('base64'), function(err) {
    if (err) {
      return callback(err, false);
    }
    self.kerberos.authGSSClientWrap(self.context, self.context.response, util.format('%s/%s@DATASTAX.COM', self.user, self.host), function(err) {
      if(err) {
        return callback(err);
      }
      callback();
    });
  });
};

StandardGssClient.prototype.shutdown = function (callback) {
  this.kerberos.authGSSClientClean(this.context, callback);
};

module.exports = GssClient;