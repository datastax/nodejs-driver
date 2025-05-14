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

import dns from "dns";
import utils from "../utils";
import BaseDseAuthenticator from './base-dse-authenticator';
import GssapiClient from './gssapi-client';
import { Authenticator, AuthProvider } from "./provider";

const mechanism = utils.allocBufferFromString('GSSAPI');
const initialServerChallenge = 'GSSAPI-START';
const emptyBuffer = utils.allocBuffer(0);

/**
 * @classdesc
 * AuthProvider that provides GSSAPI authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * @example
 * const client = new cassandra.Client({
 *   contactPoints: ['h1', 'h2'],
 *   authProvider: new cassandra.auth.DseGssapiAuthProvider()
 * });
 * @alias module:auth~DseGssapiAuthProvider
 */
class DseGssapiAuthProvider extends AuthProvider {
  private _kerberos: any;
  private authorizationId: string;
  private service: string;
  private hostNameResolver: Function;

  /**
   * Creates a new instance of <code>DseGssapiAuthProvider</code>.
   * @classdesc
   * AuthProvider that provides GSSAPI authenticator instances for clients to connect
   * to DSE clusters secured with the DseAuthenticator.
   * @param {Object} [gssOptions] GSSAPI authenticator options
   * @param {String} [gssOptions.authorizationId] The optional authorization ID. Providing an authorization ID allows the
   * currently authenticated user to act as a different user (a.k.a. proxy authentication).
   * @param {String} [gssOptions.service] The service to use. Defaults to 'dse'.
   * @param {Function} [gssOptions.hostNameResolver] A method to be used to resolve the name of the Cassandra node based
   * on the IP Address.  Defaults to [lookupServiceResolver]{@link module:auth~DseGssapiAuthProvider.lookupServiceResolver}
   * which resolves the FQDN of the provided IP to generate principals in the format of
   * <code>dse/example.com@MYREALM.COM</code>.
   * Alternatively, you can use [reverseDnsResolver]{@link module:auth~DseGssapiAuthProvider.reverseDnsResolver} to do a
   * reverse DNS lookup or [useIpResolver]{@link module:auth~DseGssapiAuthProvider.useIpResolver} to simply use the IP
   * address provided.
   * @param {String} [gssOptions.user] DEPRECATED, it will be removed in future versions. For proxy authentication, use
   * <code>authorizationId</code> instead.
   * @example
   * const client = new cassandra.Client({
   *   contactPoints: ['h1', 'h2'],
   *   authProvider: new cassandra.auth.DseGssapiAuthProvider()
   * });
   * @alias module:auth~DseGssapiAuthProvider
   * @constructor
   */
  constructor(gssOptions: { authorizationId?: string; service?: string; hostNameResolver?: Function; user?: string; }) {
    super();
    // Load the kerberos at construction time
    try {
      // eslint-disable-next-line
      this._kerberos = require('kerberos');
    } catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        const newErr = new Error('You must install module "kerberos" to use GSSAPI auth provider: ' +
          'https://www.npmjs.com/package/kerberos');
        newErr["code"] = err.code;
        throw newErr;
      }
      throw err;
    }
    gssOptions = gssOptions || utils.emptyObject;
    this.authorizationId = gssOptions.authorizationId || gssOptions.user;
    this.service = gssOptions.service;
    this.hostNameResolver = gssOptions.hostNameResolver || DseGssapiAuthProvider.lookupServiceResolver;
  }

  /**
   * Returns an Authenticator instance to be used by the driver when connecting to a host.
   * @param {String} endpoint The IP address and port number in the format ip:port.
   * @param {String} name Authenticator name.
   * @override
   * @returns {Authenticator}
   */
  newAuthenticator(endpoint: string, name: string): Authenticator {
    let address = endpoint;
    if (endpoint.indexOf(':') > 0) {
      address = endpoint.split(':')[0];
    }
    return new GssapiAuthenticator(
      this._kerberos, address, name, this.authorizationId, this.service, this.hostNameResolver);
  }

  /**
   * Performs a lookupService query that resolves an IPv4 or IPv6 address to a hostname.  This ultimately makes a
   * <code>getnameinfo()</code> system call which depends on the OS to do hostname resolution.
   * <p/>
   * <b>Note:</b> Depends on <code>dns.lookupService</code> which was added in 0.12.  For older versions falls back on
   * [reverseDnsResolver]{@link module:auth~DseGssapiAuthProvider.reverseDnsResolver}.
   *
   * @param {String} ip IP address to resolve.
   * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
   */
  private static lookupServiceResolver(ip: string, callback: Function) {
    if (!dns.lookupService) {
      return DseGssapiAuthProvider.reverseDnsResolver(ip, callback);
    }
    dns.lookupService(ip, 0, function (err, hostname) {
      if (err) {
        return callback(err);
      }
      if (!hostname) {
        // Fallback to ip
        return callback(null, ip);
      }
      callback(null, hostname);
    });
  }

  //TODO: reverseDnsResolver and useIpResolver are in JSDoc but we didn't expose them in .d.ts. Should we?
  /**
   * Performs a reverse DNS query that resolves an IPv4 or IPv6 address to a hostname.
   * @param {String} ip IP address to resolve.
   * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
   */
  static reverseDnsResolver(ip: string, callback: Function) {
    dns.reverse(ip, function (err, names) {
      if (err) {
        return callback(err);
      }
      if (!names || !names.length) {
        // Fallback to ip
        return callback(null, ip);
      }
      callback(null, names[0]);
    });
  }

  /**
   * Effectively a no op operation, returns the IP address provided.
   * @param {String} ip IP address to use.
   * @param {Function} callback The callback function with <code>err</code> and <code>hostname</code> arguments.
   */
  static useIpResolver(ip: string, callback: Function) {
    callback(null, ip);
  }
}

/**
 * @param {Object} kerberosModule
 * @param {String} address Host address.
 * @param {String} authenticatorName
 * @param {String} authorizationId
 * @param {String} service
 * @param {Function} hostNameResolver
 * @extends Authenticator
 * @private @internal
 */
class GssapiAuthenticator extends BaseDseAuthenticator {
  authorizationId: any;
  address: any;
  client: GssapiClient;
  hostNameResolver: any;

  /**
   * @param {Object} kerberosModule
   * @param {String} address Host address.
   * @param {String} authenticatorName
   * @param {String} authorizationId
   * @param {String} service
   * @param {Function} hostNameResolver
   * @extends Authenticator
   * @private
   */
  constructor(kerberosModule: object, address: string, authenticatorName: string, authorizationId: string, service: string, hostNameResolver: Function) {
    super(authenticatorName);
    this.authorizationId = authorizationId;
    this.address = address;
    this.client = GssapiClient.createNew(kerberosModule, authorizationId, service);
    this.hostNameResolver = hostNameResolver;
  }

  getMechanism() {
    return mechanism;
  }

  getInitialServerChallenge() {
    return utils.allocBufferFromString(initialServerChallenge);
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   * Obtain an initial response token for initializing the SASL handshake.
   * @param {Function} callback
   */
  initialResponse(callback: Function) {
    const self = this;
    // Initialize the GSS client
    let host = this.address;
    utils.series([
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
            // Fallback
            return self.evaluateChallenge(self.getInitialServerChallenge(), next);
          }
          // Send the mechanism as a first auth message
          next(null, self.getMechanism());
        });
      }
    ], callback);
  }

  /**
   * Evaluates a challenge received from the Server. Generally, this method should callback with
   * no error and no additional params when authentication is complete from the client perspective.
   * @param {Buffer} challenge
   * @param {Function} callback
   * @override
   */
  evaluateChallenge(challenge: Buffer, callback: Function) {
    if (!challenge || challenge.toString() === initialServerChallenge) {
      challenge = emptyBuffer;
    }
    this.client.evaluateChallenge(challenge, callback);
  }

  /**
   * @override
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  onAuthenticationSuccess(token?: Buffer) {
    this.client.shutdown(function noop() { });
  }
}

export default DseGssapiAuthProvider;