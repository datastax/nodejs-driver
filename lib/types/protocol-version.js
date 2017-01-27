'use strict';

/**
 * Contains information for the different protocol versions supported by the driver.
 * @type {Object}
 * @property {Number} v1 Cassandra protocol v1, supported in Apache Cassandra 1.2-->2.2.
 * @property {Number} v2 Cassandra protocol v2, supported in Apache Cassandra 2.0-->2.2.
 * @property {Number} v3 Cassandra protocol v3, supported in Apache Cassandra 2.1-->3.x.
 * @property {Number} v4 Cassandra protocol v4, supported in Apache Cassandra 2.2-->3.x.
 * @property {Number} v5 Cassandra protocol v5, in beta from Apache Cassandra 3.x+. Currently not supported by the
 * driver.
 * @property {Number} maxSupported Returns the higher protocol version that is supported by this driver.
 * @property {Number} minSupported Returns the lower protocol version that is supported by this driver.
 * @property {Function} isSupported A function that returns a boolean determining whether a given protocol version
 * is supported.
 * @alias module:types~protocolVersion
 */
var protocolVersion = {
  // Strict equality operators to compare versions are allowed, other comparison operators are discouraged. Instead,
  // use a function that checks if a functionality is present on a certain version, for maintainability purposes.
  v1: 0x01,
  v2: 0x02,
  v3: 0x03,
  v4: 0x04,
  v5: 0x05,
  maxSupported: 0x04,
  minSupported: 0x01,
  isSupported: function (version) {
    return (version <= 0x04 && version >= 0x01);
  },
  /**
   * Determines whether the protocol supports partition key indexes in the `prepared` RESULT responses.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  supportsPreparedPartitionKey: function (version) {
    return (version >= this.v4);
  },
  /**
   * Determines whether the protocol supports up to 4 strings (ie: change_type, target, keyspace and table) in the
   * schema change responses.
   * @param version
   * @return {boolean}
   * @ignore
   */
  supportsSchemaChangeFullMetadata: function (version) {
    return (version >= this.v3);
  },
  /**
   * Determines whether the protocol supports paging state and serial consistency parameters in QUERY and EXECUTE
   * requests.
   * @param version
   * @return {boolean}
   * @ignore
   */
  supportsPaging: function (version) {
    return (version >= this.v2);
  },
  /**
   * Determines whether the protocol supports timestamps parameters in BATCH, QUERY and EXECUTE requests.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  supportsTimestamp: function (version) {
    return (version >= this.v3);
  },
  /**
   * Determines whether the protocol supports named parameters in QUERY and EXECUTE requests.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  supportsNamedParameters: function (version) {
    return (version >= this.v3);
  },
  /**
   * Determines whether the protocol supports unset parameters.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  supportsUnset: function (version) {
    return (version >= this.v4);
  },
  /**
   * Determines whether the protocol supports timestamp and serial consistency parameters in BATCH requests.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  uses2BytesStreamIds: function (version) {
    return (version >= this.v3);
  },
  /**
   * Determines whether the collection length is encoded using 32 bits.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  uses4BytesCollectionLength: function (version) {
    return (version >= this.v3);
  },
  /**
   * Startup responses using protocol v4+ can be a SERVER_ERROR wrapping a ProtocolException, this method returns true
   * when is possible to receive such error.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  canStartupResponseErrorBeWrapped: function (version) {
    return (version >= this.v4);
  },
  /**
   * Gets the first version number that is supported, lower than the one provided.
   * Returns zero when there isn't a lower supported version.
   * @param {Number} version
   * @return {Number}
   * @ignore
   */
  getLowerSupported: function (version) {
    if (version >= this.v5) {
      return this.v4;
    }
    if (version <= this.v1) {
      return 0;
    }
    return version - 1;
  }
};

module.exports = protocolVersion;