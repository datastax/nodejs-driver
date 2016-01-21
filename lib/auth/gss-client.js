'use strict';
var util = require('util');
/**
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} [user]
 * @param {String} [service]
 * @ignore
 * @constructor
 */
function GssClient(kerberosModule, user, service) {
  this.kerberosModule = kerberosModule;
  this.user = user;
  this.service = service !== undefined ? service : 'dse';
}

/**
 * Factory to get the actual implementation of GSSAPI (unix or win)
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} [user] The user principal. (defaults to whatever is in the ticket cache.)
 * @param {String} [service] The service to use. (defaults to 'dse')
 * @returns GssClient
 */
GssClient.createNew = function (kerberosModule, user, service) {
  return new StandardGssClient(kerberosModule, user, service);
};

/**
 * @abstract
 * @param {String} host Host name or ip
 * @param {Function} callback
 */
GssClient.prototype.init = function (host, callback) {
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
function StandardGssClient(kerberosModule, user, service) {
  GssClient.call(this, kerberosModule, user, service);
  this.kerberos = new kerberosModule.Kerberos();
  this.transitionIndex = 0;
}

util.inherits(StandardGssClient, GssClient);

StandardGssClient.prototype.init = function (host, callback) {
  this.host = host;
  var uri = this.service;
  if (this.host) {
    //For the principal    "dse/cassandra1.datastax.com@DATASTAX.COM"
    //the expected uri is: "dse@cassandra1.datastax.com"
    uri = util.format("%s@%s", this.service, this.host);
  }
  var self = this;
  this.kerberos.authGSSClientInit(
    //service@host
    uri,
    this.kerberosModule.Kerberos.GSS_C_MUTUAL_FLAG, //authenticate itself flag
    function authInitStdCallback(err, context) {
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
    var cb = function(err) {
      if(err) {
        return callback(err);
      }
      callback();
    };

    if(self.user !== undefined) {
      self.kerberos.authGSSClientWrap(self.context, self.context.response, self.user, cb);
    } else {
      self.kerberos.authGSSClientWrap(self.context, self.context.response, cb);
    }
  });
};

StandardGssClient.prototype.shutdown = function (callback) {
  this.kerberos.authGSSClientClean(this.context, callback);
};

module.exports = GssClient;