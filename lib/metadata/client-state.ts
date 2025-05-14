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
import util from "util";
import type Client from "../client";
import errors from "../errors";
import type { Host } from "../host";



/**
 * Represents the state of a {@link Client}.
 * <p>
 * Exposes information on the connections maintained by a Client at a specific time.
 * </p>
 * @alias module:metadata~ClientState
 * @constructor
 */
class ClientState {
  private _hosts: Host[];
  private _openConnections: { [key: string]: number; };
  private _inFlightQueries: { [key: string]: number; };

  /**
   * Creates a new instance of <code>ClientState</code>.
   * @internal
   * @param {Array<Host>} hosts
   * @param {Object.<String, Number>} openConnections
   * @param {Object.<String, Number>} inFlightQueries
   */
  constructor(hosts: Array<Host>, openConnections: {[key: string]: number}, inFlightQueries: {[key: string]: number}) {
    this._hosts = hosts;
    this._openConnections = openConnections;
    this._inFlightQueries = inFlightQueries;
  }

  /**
   * Get an array of hosts to which the client is connected to.
   * @return {Array<Host>}
   */
  getConnectedHosts(): Array<Host> {
    return this._hosts;
  }

  /**
   * Gets the amount of open connections to a given host.
   * @param {Host} host
   * @return {Number}
   */
  getOpenConnections(host: Host): number {
    if (!host) {
      throw new errors.ArgumentError('Host is not defined');
    }

    return this._openConnections[host.address] || 0;
  }

  /**
   * Gets the amount of queries that are currently being executed through a given host.
   * <p>
   * This corresponds to the number of queries that have been sent by the Client to server Host on one of its connections
   * but haven't yet obtained a response.
   * </p>
   * @param {Host} host
   * @return {Number}
   */
  getInFlightQueries(host: Host): number {
    if (!host) {
      throw new errors.ArgumentError('Host is not defined');
    }

    return this._inFlightQueries[host.address] || 0;
  }

  /**
   * Returns the string representation of the instance.
   */
  toString(): string {
    return util.format('{"hosts": %j, "openConnections": %j, "inFlightQueries": %j}',
      this._hosts.map(function (h) { return h.address; }), this._openConnections, this._inFlightQueries);
  }

  /**
   * Creates a new instance from the provided client.
   * @param {Client} client
   * @internal
   * @ignore
   */
  static from(client: Client) {
    const openConnections = {};
    const inFlightQueries = {};
    const hostArray = [];

    client.hosts.forEach(host => {
      if (host.pool.connections.length === 0) {
        return;
      }

      hostArray.push(host);
      openConnections[host.address] = host.pool.connections.length;
      inFlightQueries[host.address] = host.getInFlight();
    });

    return new ClientState(hostArray, openConnections, inFlightQueries);
  }
}

export default ClientState;