"use strict";
var util = require('util');
var async = require('async');
/** @module policies/addressResolution */
/**
 * @class
 * @classdesc
 * Translates IP addresses received from Cassandra nodes into locally queryable
 * addresses.
 * <p>
 * The driver auto-detects new Cassandra nodes added to the cluster through server
 * side pushed notifications and through checking the system tables. For each
 * node, the address received will correspond to the address set as
 * <code>rpc_address</code> in the node yaml file. In most case, this is the correct
 * address to use by the driver and that is what is used by default. However,
 * sometimes the addresses received through this mechanism will either not be
 * reachable directly by the driver or should not be the preferred address to use
 * to reach the node (for instance, the <code>rpc_address</code> set on Cassandra nodes
 * might be a private IP, but some clients  may have to use a public IP, or
 * pass by a router to reach that node). This interface allows to deal with
 * such cases, by allowing to translate an address as sent by a Cassandra node
 * to another address to be used by the driver for connection.
 * <p>
 * Please note that the contact points addresses provided while creating the
 * {@link Client} instance are not "translated", only IP address retrieve from or sent
 * by Cassandra nodes to the driver are.
 * @constructor
 */
function AddressTranslator() {

}

/**
 * Translates a Cassandra <code>rpc_address</code> to another address if necessary.
 * @param {String} address the address of a node as returned by Cassandra.
 * <p>
 * Note that if the <code>rpc_address</code> of a node has been configured to <code>0.0.0.0</code>
 * server side, then the provided address will be the node <code>listen_address</code>,
 * *not* <code>0.0.0.0</code>.
 * </p>
 * @param {Number} port The port number, as specified in the [protocolOptions]{@link ClientOptions} at Client instance creation (9042 by default).
 * @param {Function} callback Callback to invoke with endPoint as first parameter.
 * The endPoint is an string composed of the IP address and the port number in the format <code>ipAddress:port</code>.
 */
AddressTranslator.prototype.translate = function (address, port, callback) {
  callback(address + ':' + port);
};

exports.AddressTranslator = AddressTranslator;