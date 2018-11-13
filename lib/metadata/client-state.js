'use strict';

const util = require('util');
const errors = require('../errors');

/**
 * Creates a new instance of <code>ClientState</code>.
 * @classdesc
 * Represents the state of a {@link Client}.
 * <p>
 * Exposes information on the connections maintained by a Client at a specific time.
 * </p>
 * @alias module:metadata~ClientState
 * @param {Array<Host>} hosts
 * @param {Object.<String, Number>} openConnections
 * @param {Object.<String, Number>} inFlightQueries
 * @constructor
 */
function ClientState(hosts, openConnections, inFlightQueries) {
  this._hosts = hosts;
  this._openConnections = openConnections;
  this._inFlightQueries = inFlightQueries;
}

/**
 * Creates a new instance from the provided client.
 * @param {Client} client
 * @internal
 * @ignore
 */
ClientState.from = function (client) {
  const openConnections = {};
  const inFlightQueries = {};
  const hostArray = [];
  client.hosts.forEach(function each(host) {
    if (host.pool.connections.length === 0) {
      return;
    }

    hostArray.push(host);
    openConnections[host.address] = host.pool.connections.length;
    inFlightQueries[host.address] = host.getInFlight();
  });
  return new ClientState(hostArray, openConnections, inFlightQueries);
};

/**
 * Get an array of hosts to which the client is connected to.
 * @return {Array<Host>}
 */
ClientState.prototype.getConnectedHosts = function () {
  return this._hosts;
};

/**
 * Gets the amount of open connections to a given host.
 * @param {Host} host
 * @return {Number}
 */
ClientState.prototype.getOpenConnections = function (host) {
  if (!host) {
    throw new errors.ArgumentError('Host is not defined');
  }
  return this._openConnections[host.address] || 0;
};

/**
 * Gets the amount of queries that are currently being executed through a given host.
 * <p>
 * This corresponds to the number of queries that have been sent by the Client to server Host on one of its connections
 * but haven't yet obtained a response.
 * </p>
 * @param {Host} host
 * @return {Number}
 */
ClientState.prototype.getInFlightQueries = function (host) {
  if (!host) {
    throw new errors.ArgumentError('Host is not defined');
  }
  return this._inFlightQueries[host.address] || 0;
};

/**
 * Returns the string representation of the instance.
 */
ClientState.prototype.toString = function () {
  return util.format(
    '{"hosts": %j, "openConnections": %j, "inFlightQueries": %j}',
    this._hosts.map(function (h) { return h.address; }),
    this._openConnections,
    this._inFlightQueries
  );
};

module.exports = ClientState;