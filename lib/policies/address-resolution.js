"use strict";
var dns = require('dns');
var util = require('util');
var utils = require('../utils');
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
 * @param {Function} callback Callback to invoke with endpoint as first parameter.
 * The endpoint is an string composed of the IP address and the port number in the format <code>ipAddress:port</code>.
 */
AddressTranslator.prototype.translate = function (address, port, callback) {
  callback(address + ':' + port);
};

/**
 * @class
 * @classdesc
 * {@link AddressTranslator} implementation for multi-region EC2 deployments <strong>where clients are also deployed in EC2</strong>.
 * <p>
 * Its distinctive feature is that it translates addresses according to the location of the Cassandra host:
 * </p>
 * <ul>
 *  <li>addresses in different EC2 regions (than the client) are unchanged</li>
 *  <li>addresses in the same EC2 region are <strong>translated to private IPs</strong></li>
 * </ul>
 * <p>
 * This optimizes network costs, because Amazon charges more for communication over public IPs.
 * </p>
 * @constructor
 */
function EC2MultiRegionTranslator() {

}

util.inherits(EC2MultiRegionTranslator, AddressTranslator);

/**
 * Addresses in the same EC2 region are translated to private IPs and addresses in
 * different EC2 regions (than the client) are unchanged
 */
EC2MultiRegionTranslator.prototype.translate = function (address, port, callback) {
  var newAddress = address;
  var self = this;
  var name;
  utils.series([
    function resolve(next) {
      dns.reverse(address, function (err, hostNames) {
        if (err) return next(err);
        if (!hostNames) return next();
        name = hostNames[0];
        next();
      });
    },
    function lookup(next) {
      if (!name) return next();
      dns.lookup(name, function (err, lookupAddress) {
        if (err) return next(err);
        newAddress = lookupAddress;
        next();
      });
    }], function (err) {
    if (err) {
      //there was an issue while doing dns resolution
      self.logError(address, err);
    }
    callback(newAddress + ':' + port);
  });
};

/**
 * Log method called to log errors that occurred while performing dns resolution.
 * You can assign your own method to the class instance to do proper logging.
 * @param {String} address
 * @param {Error} err
 */
EC2MultiRegionTranslator.prototype.logError = function (address, err) {
  //Do nothing by default
};

exports.AddressTranslator = AddressTranslator;
exports.EC2MultiRegionTranslator = EC2MultiRegionTranslator;