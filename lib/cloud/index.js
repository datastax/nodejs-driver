/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

'use strict';

const https = require('https');
const fs = require('fs');
const AdmZip = require('adm-zip');

const errors = require('../errors');
const utils = require('../utils');
const PlainTextAuthProvider = require('../auth').PlainTextAuthProvider;

/**
 * When the user sets the cloud options, it uses the secure bundle or endpoint to access the metadata service and
 * setting the connection options
 * @param {ClientOptions} options
 * @param {Function} callback
 */
function init(options, callback) {
  if (!options.cloud) {
    return callback();
  }

  const cloudOptions = new CloudOptions(options);

  utils.series([
    next => parseZipFile(cloudOptions, next),
    next => getMetadataServiceInfo(cloudOptions, next),
    next => {
      if (!cloudOptions.clientOptions.sslOptions.checkServerIdentity) {
        // With SNI enabled, hostname and CN will not match
        // Host name and CN was validated as part of the initial HTTPS request
        cloudOptions.clientOptions.sslOptions.checkServerIdentity = () => undefined;
      }
      next();
    }
  ], callback);
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

    if (this.clientOptions.authProvider) {
      // There is an auth provider set by the user
      return;
    }

    this.clientOptions.authProvider = new PlainTextAuthProvider(username, password);
  }
}

/**
 * @param {CloudOptions} cloudOptions
 * @param {Function} callback
 */
function parseZipFile(cloudOptions, callback) {
  if (cloudOptions.serviceUrl) {
    // Service url already was provided
    return callback();
  }

  if (!cloudOptions.secureConnectBundle) {
    return callback(new TypeError('secureConnectBundle must be provided'));
  }

  fs.readFile(cloudOptions.secureConnectBundle, (err, data) => {
    if (err) {
      return callback(err);
    }

    try {

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

    } catch (e) {
      return callback(e);
    }

    callback();
  });
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

exports.init = init;