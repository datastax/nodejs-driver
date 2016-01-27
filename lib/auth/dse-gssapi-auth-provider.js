'use strict';
var util = require('util');
var cassandra = require('cassandra-driver');
var BaseDseAuthenticator = require('./base-dse-authenticator');
var GssapiClient = require('./gssapi-client');
var async = require('async');
var dns = require('dns');

var mechanism = new Buffer('GSSAPI');
var initialServerChallenge = 'GSSAPI-START';
var emptyBuffer = new Buffer(0);

/**
 * AuthProvider that provides GSSAPI authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @param {Object} gssOptions GSSAPI authenticator options
 * @param {String} gssOptions.user The user principal. Defaults to whatever is in the ticket cache.
 * @param {String} gssOptions.service The service to use. Defaults to 'dse'.
 * @param {Function} gssOptions.hostNameResolver A method to be used to resolve the name of the Cassandra node based
 * on the IP Address.
 * You can use {@link DseGssapiAuthProvider.reverseDnsResolver} if you use the FQDN for the principal.
 * Defaults to a noop function that returns the same IP address provided, valid for principals using the IP of the host
 * like 'dse/10.10.10.10@MYREALM.COM'.
 * @constructor
 */
function DseGssapiAuthProvider(gssOptions) {
  //load the kerberos at construction time
  try {
    this._kerberos = require('kerberos');
  }
  catch (err) {
    if (err.code === 'MODULE_NOT_FOUND') {
      var newErr = new Error('You must install module "kerberos" to use GSSAPI auth provider: ' +
        'https://www.npmjs.com/package/kerberos');
      newErr.code = err.code;
      throw newErr;
    }
    throw err;
  }
  gssOptions = gssOptions || {};
  this.user = gssOptions.user;
  this.service = gssOptions.service;
  this.hostNameResolver = gssOptions.hostNameResolver || DseGssapiAuthProvider.useIpResolver;
}

util.inherits(DseGssapiAuthProvider, cassandra.auth.AuthProvider);

/** @override */
DseGssapiAuthProvider.prototype.newAuthenticator = function (endpoint, name) {
  var address = endpoint;
  if (endpoint.indexOf(':') > 0) {
    address = endpoint.split(':')[0];
  }
  return new GssapiAuthenticator(this._kerberos, address, name, this.user, this.service, this.hostNameResolver);
};

DseGssapiAuthProvider.reverseDnsResolver = function (ip, callback) {
  dns.reverse(ip, function (err, names) {
    if (err) {
      return callback(err);
    }
    if (!names || !names.length) {
      //fallback to ip
      return callback(null, ip);
    }
    callback(null, names[0]);
  });
};

DseGssapiAuthProvider.useIpResolver = function (ip, callback) {
  callback(null, ip);
};

/**
 * @param {Object} kerberosModule
 * @param {String} address Host address.
 * @param {String} authenticatorName
 * @param {String} user
 * @param {String} service
 * @param {Function} hostNameResolver
 * @extends Authenticator
 * @private
 */
function GssapiAuthenticator(kerberosModule, address, authenticatorName, user, service, hostNameResolver) {
  BaseDseAuthenticator.call(this, authenticatorName);
  this.user = user;
  this.address = address;
  this.client = GssapiClient.createNew(kerberosModule, user, service);
  this.hostNameResolver = hostNameResolver;
}

util.inherits(GssapiAuthenticator, BaseDseAuthenticator);

GssapiAuthenticator.prototype.getMechanism = function () {
  return mechanism;
};

GssapiAuthenticator.prototype.getInitialServerChallenge = function () {
  return new Buffer(initialServerChallenge);
};

//noinspection JSUnusedGlobalSymbols
/**
 * Obtain an initial response token for initializing the SASL handshake.
 * @param {Function} callback
 */
GssapiAuthenticator.prototype.initialResponse = function (callback) {
  var self = this;
  //initialize the GSS client
  var host = this.address;
  async.series([
    function getHostName(next) {
      self.hostNameResolver(self.address, function (err, name) {
        if (!err && name) {
          host = name;
        }
        next();
      });
    },
    function initClient(next) {
      self.client.init(host, function (err) {
        if (err) {
          return next(err);
        }
        if (!self._isDseAuthenticator()) {
          //fallback
          return self.evaluateChallenge(self.getInitialServerChallenge(), next);
        }
        //send the mechanism as a first auth message
        next(null, self.getMechanism());
      });
    }
  ], function seriesFinished(err, results) {
    if (results && results.length > 0) {
      results = results[results.length - 1];
    }
    callback(err, results);
  });
};

/**
 * Evaluates a challenge received from the Server. Generally, this method should callback with
 * no error and no additional params when authentication is complete from the client perspective.
 * @param {Buffer} challenge
 * @param {Function} callback
 * @override
 */
GssapiAuthenticator.prototype.evaluateChallenge = function (challenge, callback) {
  if (!challenge || challenge.toString() === initialServerChallenge) {
    challenge = emptyBuffer;
  }
  this.client.evaluateChallenge(challenge, callback);
};

//noinspection JSUnusedLocalSymbols,JSUnusedGlobalSymbols
/**
 * @override
 */
GssapiAuthenticator.prototype.onAuthenticationSuccess = function (token) {
  this.client.shutdown(function noop() { });
};


module.exports = DseGssapiAuthProvider;