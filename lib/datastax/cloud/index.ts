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

const https = require('https');
const fs = require('fs');
const util = require('util');
const AdmZip = require('adm-zip');
const { URL } = require('url');

const errors = require('../../errors');
const utils = require('../../utils');
const { DsePlainTextAuthProvider, NoAuthProvider } = require('../../auth');

// Use the callback-based method fs.readFile() instead of fs.promises as we have to support Node.js 8+
const readFile = util.promisify(fs.readFile);

/**
 * When the user sets the cloud options, it uses the secure bundle or endpoint to access the metadata service and
 * setting the connection options
 * @param {ClientOptions} options
 * @returns {Promise<void>}
 */
async function init(options) {
  if (!options.cloud) {
    return;
  }

  const cloudOptions = new CloudOptions(options);
  await parseZipFile(cloudOptions);
  await getMetadataServiceInfoAsync(cloudOptions);

  if (!cloudOptions.clientOptions.sslOptions.checkServerIdentity) {
    // With SNI enabled, hostname (uuid) and CN will not match
    // Use a custom validation function to validate against the proxy address.
    // Note: this function is only called if the certificate passed all other checks, like CA validation.
    cloudOptions.clientOptions.sslOptions.checkServerIdentity = (_, cert) =>
      checkServerIdentity(cert, cloudOptions.clientOptions.sni.address);
  }
}

class CloudOptions {
  constructor(clientOptions) {
    this.clientOptions = clientOptions;

    if (clientOptions.cloud.secureConnectBundle) {
      this.secureConnectBundle = clientOptions.cloud.secureConnectBundle;
      this.serviceUrl = null;
    } else {
      this.serviceUrl = clientOptions.cloud.endpoint;
    }
    // Include a log emitter to enable logging within the cloud connection logic
    this.logEmitter = clientOptions.logEmitter;

    this.contactPoints = null;
    this.localDataCenter = null;
  }

  /**
   * The sslOptions in the client options from a given map.
   * @param {Map<String, Buffer>} zipEntries
   */
  setSslOptions(zipEntries) {
    this.clientOptions.sslOptions = Object.assign({
      ca: [zipEntries.get('ca.crt') ],
      cert: zipEntries.get('cert'),
      key: zipEntries.get('key'),
      rejectUnauthorized: true
    }, this.clientOptions.sslOptions);
  }

  /**
   *
   * @param username
   * @param password
   */
  setAuthProvider(username, password) {
    if (!username || !password) {
      return;
    }

    if (this.clientOptions.authProvider && !(this.clientOptions.authProvider instanceof NoAuthProvider)) {
      // There is an auth provider set by the user
      return;
    }

    this.clientOptions.authProvider = new DsePlainTextAuthProvider(username, password);
  }
}

/**
 * @param {CloudOptions} cloudOptions
 * @returns {Promise<void>}
 */
async function parseZipFile(cloudOptions) {
  if (cloudOptions.serviceUrl) {
    // Service url already was provided
    return;
  }

  if (!cloudOptions.secureConnectBundle) {
    throw new TypeError('secureConnectBundle must be provided');
  }

  const data = await readFile(cloudOptions.secureConnectBundle);
  const zip = new AdmZip(data);
  const zipEntries = new Map(zip.getEntries().map(e => [e.entryName, e.getData()]));

  if (!zipEntries.get('config.json')) {
    throw new TypeError('Config file must be contained in secure bundle');
  }

  const config = JSON.parse(zipEntries.get('config.json').toString('utf8'));
  if (!config['host'] || !config['port']) {
    throw new TypeError('Config file must include host and port information');
  }

  cloudOptions.serviceUrl = `${config['host']}:${config['port']}/metadata`;
  cloudOptions.setSslOptions(zipEntries);
  cloudOptions.setAuthProvider(config.username, config.password);
}

/**
 * Gets the information retrieved from the metadata service.
 * Invokes the callback with {proxyAddress, localDataCenter, contactPoints} as result
 * @param {CloudOptions} cloudOptions
 * @param {Function} callback
 */
function getMetadataServiceInfo(cloudOptions, callback) {
  const regex = /^(.+?):(\d+)(.*)$/;
  const matches = regex.exec(cloudOptions.serviceUrl);
  callback = utils.callbackOnce(callback);

  if (!matches || matches.length !== 4) {
    throw new TypeError('url should be composed of host, port number and path, without scheme');
  }

  const requestOptions = Object.assign({
    hostname: matches[1],
    port: matches[2],
    path: matches[3] || undefined,
    timeout: cloudOptions.clientOptions.socketOptions.connectTimeout
  }, cloudOptions.clientOptions.sslOptions);

  const req = https.get(requestOptions, res => {
    let data = '';

    utils.log('verbose', `Connected to metadata service with SSL/TLS protocol ${res.socket.getProtocol()}`, {}, cloudOptions);

    res
      .on('data', chunk => data += chunk.toString())
      .on('end', () => {
        if (res.statusCode !== 200) {
          return callback(getServiceRequestError(new Error(`Obtained http status ${res.statusCode}`), requestOptions));
        }

        let message;

        try {
          message = JSON.parse(data);

          if (!message || !message['contact_info']) {
            throw new TypeError('contact_info should be defined in response');
          }

        } catch (err) {
          return callback(getServiceRequestError(err, requestOptions, true));
        }

        const contactInfo = message['contact_info'];

        // Set the connect options
        cloudOptions.clientOptions.contactPoints = contactInfo['contact_points'];
        cloudOptions.clientOptions.localDataCenter = contactInfo['local_dc'];
        cloudOptions.clientOptions.sni = { address: contactInfo['sni_proxy_address'] };

        callback();
      });
  });

  req.on('error', err => callback(getServiceRequestError(err, requestOptions)));

  // We need to both set the timeout in the requestOptions and invoke ClientRequest#setTimeout()
  // to handle all possible scenarios, for some reason... (tested with one OR the other and didn't fully work)
  // Setting the also the timeout handler, aborting will emit 'error' and close
  req.setTimeout(cloudOptions.clientOptions.socketOptions.connectTimeout, () => req.abort());
}

const getMetadataServiceInfoAsync = util.promisify(getMetadataServiceInfo);

/**
 * Returns an Error that wraps the inner error obtained while fetching metadata information.
 * @private
 */
function getServiceRequestError(err, requestOptions, isParsingError) {
  const message = isParsingError
    ? 'There was an error while parsing the metadata service information'
    : 'There was an error fetching the metadata information';

  const url = `${requestOptions.hostname}:${requestOptions.port}${(requestOptions.path) ? requestOptions.path : '/'}`;
  return new errors.NoHostAvailableError({ [url] : err }, message);
}

/**
 * @param {{subject: {CN: string}, subjectaltname: string?}} cert A certificate object as defined by
 * TLS module https://nodejs.org/docs/latest-v12.x/api/tls.html#tls_certificate_object
 * @param {string} sniAddress
 * @returns {Error|undefined} Similar to tls.checkServerIdentity() returns an Error object, populating it with reason,
 * host, and cert on failure. Otherwise, it returns undefined.
 * @internal
 * @ignore
 */
function checkServerIdentity(cert, sniAddress) {
  // Based on logic defined by the Node.js Core module
  // https://github.com/nodejs/node/blob/ff48009fefcecedfee2c6ff1719e5be3f6969049/lib/tls.js#L212-L290

  // SNI address is composed by hostname and port
  const hostName = sniAddress.split(':')[0];
  const altNames = cert.subjectaltname;
  const cn = cert.subject.CN;

  if (hostName === cn) {
    // quick check based on common name
    return undefined;
  }

  const parsedAltNames = [];
  if (altNames) {
    for (const name of altNames.split(', ')) {
      if (name.startsWith('DNS:')) {
        parsedAltNames.push(name.slice(4));
      } else if (name.startsWith('URI:')) {
        parsedAltNames.push(new URL(name.slice(4)).hostname);
      }
    }
  }

  const hostParts = hostName.split('.');
  const wildcard = (pattern) => checkParts(hostParts, pattern);

  let valid;
  if (parsedAltNames.length > 0) {
    valid = parsedAltNames.some(wildcard);
  } else {
    // Use the common name
    valid = wildcard(cn);
  }

  if (!valid) {
    const error = new Error(`Host: ${hostName} is not cert's CN/altnames: ${cn} / ${altNames}`);
    error.reason = error.message;
    error.host = hostName;
    error.cert = cert;
    return error;
  }
}

/**
 * Simplified version of Node.js tls core lib check() function
 * https://github.com/nodejs/node/blob/ff48009fefcecedfee2c6ff1719e5be3f6969049/lib/tls.js#L148-L209
 * @private
 * @returns {boolean}
 */
function checkParts(hostParts, pattern) {
  // Empty strings, null, undefined, etc. never match.
  if (!pattern) {
    return false;
  }

  const patternParts = pattern.split('.');

  if (hostParts.length !== patternParts.length) {
    return false;
  }

  // Check host parts from right to left first.
  for (let i = hostParts.length - 1; i > 0; i -= 1) {
    if (hostParts[i] !== patternParts[i]) {
      return false;
    }
  }

  const hostSubdomain = hostParts[0];
  const patternSubdomain = patternParts[0];
  const patternSubdomainParts = patternSubdomain.split('*');

  // Short-circuit when the subdomain does not contain a wildcard.
  // RFC 6125 does not allow wildcard substitution for components
  // containing IDNA A-labels (Punycode) so match those verbatim.
  if (patternSubdomainParts.length === 1 || patternSubdomain.includes('xn--')) {
    return hostSubdomain === patternSubdomain;
  }

  // More than one wildcard is always wrong.
  if (patternSubdomainParts.length > 2) {
    return false;
  }

  // *.tld wildcards are not allowed.
  if (patternParts.length <= 2) {
    return false;
  }

  const [prefix, suffix] = patternSubdomainParts;

  if (prefix.length + suffix.length > hostSubdomain.length) {
    return false;
  }

  if (!hostSubdomain.startsWith(prefix)) {
    return false;
  }

  if (!hostSubdomain.endsWith(suffix)) {
    return false;
  }

  return true;
}

module.exports = {
  checkServerIdentity,
  init
};