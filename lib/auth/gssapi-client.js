/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const util = require('util');
const utils = require('../utils');

/**
 * GSSAPI Client interface.
 * @ignore
 */
class GssapiClient {
  /**
   * @param {String} [authorizationId]
   * @param {String} [service]
   */
  constructor(authorizationId, service) {
    this.authorizationId = authorizationId;
    this.service = service !== undefined ? service : 'dse';
  }

  /**
   * @abstract
   * @param {String} host Host name or ip
   * @param {Function} callback
   */
  init(host, callback) {
    throw new Error('Not implemented');
  }

  /**
   * @param {Buffer} challenge
   * @param {Function} callback
   * @abstract
   */
  evaluateChallenge(challenge, callback) {
    throw new Error('Not implemented');
  }

  /**
   * @abstract
   * @param {Function} [callback]
   */
  shutdown(callback) {
    throw new Error('Not implemented');
  }

  /**
   * Factory to get the actual implementation of GSSAPI (unix or win)
   * @param {Object} kerberosModule Kerberos client library dependency
   * @param {String} [authorizationId] An identity to act as (for proxy authentication).
   * @param {String} [service] The service to use. (defaults to 'dse')
   * @returns GssapiClient
   */
  static createNew(kerberosModule, authorizationId, service) {
    return new StandardGssClient(kerberosModule, authorizationId, service);
  }
}

/**
 * GSSAPI Client implementation using kerberos module.
 * @ignore
 */
class StandardGssClient extends GssapiClient {
  constructor(kerberosModule, authorizationId, service) {
    if (typeof kerberosModule.initializeClient !== 'function') {
      throw new Error('The driver expects version 1.x of the kerberos library');
    }

    super(authorizationId, service);
    this.kerberos = kerberosModule;
    this.transitionIndex = 0;
  }

  init(host, callback) {
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
  }

  /** @override */
  evaluateChallenge(challenge, callback) {
    this['transition' + this.transitionIndex](challenge, (err, response) => {
      if (err) {
        return callback(err);
      }
      this.transitionIndex++;
      callback(null, response ? utils.allocBufferFromString(response, 'base64') : utils.allocBuffer(0));
    });
  }

  transition0(challenge, callback) {
    this.kerberosClient.step('', callback);
  }

  transition1(challenge, callback) {
    const charPointerChallenge = challenge.toString('base64');
    this.kerberosClient.step(charPointerChallenge, callback);
  }

  transition2(challenge, callback) {
    this.kerberosClient.unwrap(challenge.toString('base64'), (err, response) => {
      if (err) {
        return callback(err, false);
      }
      const cb = function (err, wrapped) {
        if (err) {
          return callback(err);
        }
        callback(null, wrapped);
      };
      if (this.authorizationId !== undefined) {
        this.kerberosClient.wrap(response, { user: this.authorizationId }, cb);
      }
      else {
        this.kerberosClient.wrap(response, null, cb);
      }
    });
  }

  shutdown(callback) {
    this.kerberosClient = null;
    callback();
  }
}

module.exports = GssapiClient;