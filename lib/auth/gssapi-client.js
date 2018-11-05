/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

const util = require('util');
const utils = require('../utils');

/**
 * @param {String} [authorizationId]
 * @param {String} [service]
 * @ignore
 * @constructor
 */
function GssapiClient(authorizationId, service) {
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
  if (typeof kerberosModule.initializeClient !== 'function') {
    throw new Error('The driver expects version 1.x of the kerberos library');
  }

  GssapiClient.call(this, authorizationId, service);
  this.kerberos = kerberosModule;
  this.transitionIndex = 0;
}

util.inherits(StandardGssClient, GssapiClient);

StandardGssClient.prototype.init = function (host, callback) {
  this.host = host;
  let uri = this.service;
  if (this.host) {
    //For the principal    "dse/cassandra1.datastax.com@DATASTAX.COM"
    //the expected uri is: "dse@cassandra1.datastax.com"
    uri = util.format("%s@%s", this.service, this.host);
  }

  const options = {
    gssFlags: this.kerberos.GSS_C_MUTUAL_FLAG //authenticate itself flag
  };

  this.kerberos.initializeClient(uri, options, (err, kerberosClient) => {
    if (err) {
      return callback(err);
    }

    this.kerberosClient = kerberosClient;
    callback();
  });
};

/** @override */
StandardGssClient.prototype.evaluateChallenge = function (challenge, callback) {
  this['transition' + this.transitionIndex](challenge, (err, response) => {
    if (err) {
      return callback(err);
    }

    this.transitionIndex++;
    callback(null, response ? utils.allocBufferFromString(response, 'base64') : utils.allocBuffer(0));
  });
};

StandardGssClient.prototype.transition0 = function (challenge, callback) {
  this.kerberosClient.step('', callback);
};

StandardGssClient.prototype.transition1 = function (challenge, callback) {
  const charPointerChallenge = challenge.toString('base64');
  this.kerberosClient.step(charPointerChallenge, callback);
};

StandardGssClient.prototype.transition2 = function (challenge, callback) {
  this.kerberosClient.unwrap(challenge.toString('base64'), (err, response) => {
    if (err) {
      return callback(err, false);
    }

    const cb = function(err, wrapped) {
      if(err) {
        return callback(err);
      }
      callback(null, wrapped);
    };

    if (this.authorizationId !== undefined) {
      this.kerberosClient.wrap(response, { user: this.authorizationId }, cb);
    } else {
      this.kerberosClient.wrap(response, cb);
    }
  });
};

StandardGssClient.prototype.shutdown = function (callback) {
  this.kerberosClient = null;
  callback();
};

module.exports = GssapiClient;