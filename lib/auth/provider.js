/**
 * @classdesc Provides [Authenticator]{@link module:auth~Authenticator} instances to be used when connecting to a host.
 * @constructor
 * @abstract
 * @alias module:auth~AuthProvider
 */
function AuthProvider() {

}

/**
 * Returns an [Authenticator]{@link module:auth~Authenticator} instance to be used when connecting to a host.
 * @param {String} endpoint The ip address and port number in the format ip:port
 * @param {String} name Authenticator name
 * @abstract
 * @returns {Authenticator}
 */
AuthProvider.prototype.newAuthenticator = function (endpoint, name) {
  throw new Error('This is an abstract class, you must implement newAuthenticator method or ' +
    'use another auth provider that inherits from this class');
};

/**
 * @class
 * @classdesc Handles SASL authentication with Cassandra servers.
 * Each time a new connection is created and the server requires authentication,
 * a new instance of this class will be created by the corresponding.
 * @constructor
 * @alias module:auth~Authenticator
 */
function Authenticator() {

}

/**
 * Obtain an initial response token for initializing the SASL handshake.
 * @param {Function} callback
 */
Authenticator.prototype.initialResponse = function (callback) {
  callback(new Error('Not implemented'));
};

/**
 * Evaluates a challenge received from the Server. Generally, this method should callback with
 * no error and no additional params when authentication is complete from the client perspective.
 * @param {Buffer} challenge
 * @param {Function} callback
 */
Authenticator.prototype.evaluateChallenge = function (challenge, callback) {
  callback(new Error('Not implemented'));
};

/**
 * Called when authentication is successful with the last information
 * optionally sent by the server.
 * @param {Buffer} [token]
 */
Authenticator.prototype.onAuthenticationSuccess = function (token) {

};

exports.AuthProvider = AuthProvider;
exports.Authenticator = Authenticator;