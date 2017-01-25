/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
/**
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} [authorizationId]
 * @param {String} [service]
 * @ignore
 * @constructor
 */
function GssapiClient(kerberosModule, authorizationId, service) {
  this.kerberosModule = kerberosModule;
  this.authorizationId = authorizationId;
  this.service = service !== undefined ? service : 'dse';
}

/**
 * Factory to get the actual implementation of GSSAPI (unix or win)
 * @param {Object} kerberosModule Kerberos client library dependency
 * @param {String} [authorizationId] An identity to act as (for proxy authentication).
 * @param {String} [service] The service to use. (defaults to 'dse')
 * @returns GssapiClient
 */
GssapiClient.createNew = function (kerberosModule, authorizationId, service) {
  return new StandardGssClient(kerberosModule, authorizationId, service);
};

/**
 * @abstract
 * @param {String} host Host name or ip
 * @param {Function} callback
 */
GssapiClient.prototype.init = function (host, callback) {
  throw new Error('Not implemented');
};

/**
 * @param {Buffer} challenge
 * @param {Function} callback
 * @abstract
 * @returns {Buffer}
 */
GssapiClient.prototype.evaluateChallenge = function (challenge, callback) {
  throw new Error('Not implemented');
};

/**
 * @abstract
 * @param {Function} [callback]
 */
GssapiClient.prototype.shutdown = function (callback) {
  throw new Error('Not implemented');
};

/**
 * @ignore
 * @extends GssapiClient
 * @constructor
 */
function StandardGssClient(kerberosModule, authorizationId, service) {
  GssapiClient.call(this, kerberosModule, authorizationId, service);
  this.kerberos = new kerberosModule.Kerberos();
  this.transitionIndex = 0;
}

util.inherits(StandardGssClient, GssapiClient);

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

StandardGssClient.prototype.transition0 = function (challenge, callback) {
  this.kerberos.authGSSClientStep(this.context, '', callback);
};

StandardGssClient.prototype.transition1 = function (challenge, callback) {
  var charPointerChallenge = challenge.toString('base64');
  this.kerberos.authGSSClientStep(this.context, charPointerChallenge, callback);
};

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

    if(self.authorizationId !== undefined) {
      self.kerberos.authGSSClientWrap(self.context, self.context.response, self.authorizationId, cb);
    }
    else {
      self.kerberos.authGSSClientWrap(self.context, self.context.response, cb);
    }
  });
};

StandardGssClient.prototype.shutdown = function (callback) {
  this.kerberos.authGSSClientClean(this.context, callback);
};

module.exports = GssapiClient;