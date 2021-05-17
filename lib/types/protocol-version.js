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

const utils = require('../utils');
const VersionNumber = require('./version-number');
const v200 = VersionNumber.parse('2.0.0');
const v210 = VersionNumber.parse('2.1.0');
const v220 = VersionNumber.parse('2.2.0');
const v300 = VersionNumber.parse('3.0.0');
const v510 = VersionNumber.parse('5.1.0');
const v600 = VersionNumber.parse('6.0.0');

/**
 * Contains information for the different protocol versions supported by the driver.
 * @type {Object}
 * @property {Number} v1 Cassandra protocol v1, supported in Apache Cassandra 1.2-->2.2.
 * @property {Number} v2 Cassandra protocol v2, supported in Apache Cassandra 2.0-->2.2.
 * @property {Number} v3 Cassandra protocol v3, supported in Apache Cassandra 2.1-->3.x.
 * @property {Number} v4 Cassandra protocol v4, supported in Apache Cassandra 2.2-->3.x.
 * @property {Number} v5 Cassandra protocol v5, in beta from Apache Cassandra 3.x+. Currently not supported by the
 * driver.
 * @property {Number} dseV1 DataStax Enterprise protocol v1, DSE 5.1+
 * @property {Number} dseV2 DataStax Enterprise protocol v2, DSE 6.0+
 * @property {Number} maxSupported Returns the higher protocol version that is supported by this driver.
 * @property {Number} minSupported Returns the lower protocol version that is supported by this driver.
 * @property {Function} isSupported A function that returns a boolean determining whether a given protocol version
 * is supported.
 * @alias module:types~protocolVersion
 */
const protocolVersion = {
  // Strict equality operators to compare versions are allowed, other comparison operators are discouraged. Instead,
  // use a function that checks if a functionality is present on a certain version, for maintainability purposes.
  v1: 0x01,
  v2: 0x02,
  v3: 0x03,
  v4: 0x04,
  v5: 0x05,
  v6: 0x06,
  dseV1: 0x41,
  dseV2: 0x42,
  maxSupported: 0x42,
  minSupported: 0x01,

  /**
   * Determines whether the protocol version is a DSE-specific protocol version.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  isDse: function(version) {
    return ((version >= this.dseV1 && version <= this.dseV2));
  },
  /**
   * Returns true if the protocol version represents a version of Cassandra
   * supported by this driver, false otherwise
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  isSupportedCassandra: function(version) {
    return (version <= 0x04 && version >= 0x01);
  },
  /**
   * Determines whether the protocol version is supported by this driver.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  isSupported: function (version) {
    return (this.isDse(version) || this.isSupportedCassandra(version));
  },

  /**
   * Determines whether the protocol includes flags for PREPARE messages.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  supportsPrepareFlags: function (version) {
    return (version === this.dseV2);
  },
  /**
   * Determines whether the protocol supports sending the keyspace as part of PREPARE, QUERY, EXECUTE, and BATCH.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  supportsKeyspaceInRequest: function (version) {
    return (version === this.dseV2);
  },
  /**
   * Determines whether the protocol supports result_metadata_id on `prepared` response and
   * and `execute` request.
   * @param {Number} version
   * @returns {Boolean}
   * @ignore
   */
  supportsResultMetadataId: function (version) {
    return (version === this.dseV2);
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
   * Determines whether the protocol supports continuous paging.
   * @param version
   * @return {boolean}
   * @ignore
   */
  supportsContinuousPaging: function (version) {
    return (this.isDse(version));
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
   * Determines whether the protocol provides a reason map for read and write failure errors.
   * @param version
   * @return {boolean}
   * @ignore
   */
  supportsFailureReasonMap: function (version) {
    return (version >= this.v5);
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
   * Determines whether the QUERY, EXECUTE and BATCH flags are encoded using 32 bits.
   * @param {Number} version
   * @return {boolean}
   * @ignore
   */
  uses4BytesQueryFlags: function (version) {
    return (this.isDse(version));
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
  },

  /**
   * Computes the highest supported protocol version collectively by the given hosts.
   *
   * Considers the cassandra_version of the input hosts to determine what protocol versions
   * are supported and uses the highest common protocol version among them.
   *
   * If hosts >= C* 3.0 are detected, any hosts older than C* 2.1 will not be considered
   * as those cannot be connected to.  In general this will not be a problem as C* does
   * not support clusters with nodes that have versions that are more than one major
   * version away from each other.
   * @param {Connection} connection Connection hosts were discovered from.
   * @param {Array.<Host>} hosts The hosts to determine highest protocol version from.
   * @return {Number} Highest supported protocol version among hosts.
   */
  getHighestCommon: function(connection, hosts) {
    const log = connection.log ? connection.log.bind(connection) : utils.noop;
    let maxVersion = connection.protocolVersion;
    // whether or not protocol v3 is required (nodes detected that don't support < 3).
    let v3Requirement = false;
    // track the common protocol version >= v3 in case we encounter older versions.
    let maxVersionWith3OrMore = maxVersion;
    hosts.forEach(h => {
      let dseVersion = null;
      if (h.dseVersion) {
        // As of DSE 5.1, DSE has it's own specific protocol versions.  If we detect 5.1+
        // consider those protocol versions.
        dseVersion = VersionNumber.parse(h.dseVersion);
        log('verbose', `Encountered host ${h.address} with dse version ${dseVersion}`);
        if (dseVersion.compare(v510) >= 0) {
          v3Requirement = true;
          if (dseVersion.compare(v600) >= 0) {
            maxVersion = Math.min(this.dseV2, maxVersion);
          } else {
            maxVersion = Math.min(this.dseV1, maxVersion);
          }
          maxVersionWith3OrMore = maxVersion;
          return;
        }
        // If DSE < 5.1, we fall back on the cassandra protocol logic.
      }

      if (!h.cassandraVersion || h.cassandraVersion.length === 0) {
        log('warning', 'Encountered host ' + h.address + ' with no cassandra version,' +
          ' skipping as part of protocol version evaluation');
        return;
      }

      try {
        const cassandraVersion = VersionNumber.parse(h.cassandraVersion);
        if (!dseVersion) {
          log('verbose', 'Encountered host ' + h.address + ' with cassandra version ' + cassandraVersion);
        }
        if (cassandraVersion.compare(v300) >= 0) {
          // Anything 3.0.0+ has a max protocol version of V4 and requires at least V3.
          v3Requirement = true;
          maxVersion = Math.min(this.v4, maxVersion);
          maxVersionWith3OrMore = maxVersion;
        } else if (cassandraVersion.compare(v220) >= 0) {
          // Cassandra 2.2.x has a max protocol version of V4.
          maxVersion = Math.min(this.v4, maxVersion);
          maxVersionWith3OrMore = maxVersion;
        } else if (cassandraVersion.compare(v210) >= 0) {
          // Cassandra 2.1.x has a max protocol version of V3.
          maxVersion = Math.min(this.v3, maxVersion);
          maxVersionWith3OrMore = maxVersion;
        } else if (cassandraVersion.compare(v200) >= 0) {
          // Cassandra 2.0.x has a max protocol version of V2.
          maxVersion = Math.min(this.v2, maxVersion);
        } else {
          // Anything else is < 2.x and requires protocol version V1.
          maxVersion = this.v1;
        }
      } catch (e) {
        log('warning', 'Encountered host ' + h.address + ' with unparseable cassandra version ' + h.cassandraVersion
          + ' skipping as part of protocol version evaluation');
      }
    });

    if (v3Requirement && maxVersion < this.v3) {
      const addendum = '. This should not be possible as nodes within a cluster can\'t be separated by more than one major version';
      if (maxVersionWith3OrMore < this.v3) {
        log('error', 'Detected hosts that require at least protocol version 0x3, but currently connected to '
         + connection.address + ':' + connection.port + ' using protocol version 0x' + maxVersionWith3OrMore
         + '. Will not be able to connect to these hosts' + addendum);
      } else {
        log('error', 'Detected hosts with maximum protocol version of 0x' + maxVersion.toString(16)
          + ' but there are some hosts that require at least version 0x3. Will not be able to connect to these older hosts'
          + addendum);
      }
      maxVersion = maxVersionWith3OrMore;
    }

    log('verbose', 'Resolved protocol version 0x' + maxVersion.toString(16) + ' as the highest common protocol version among hosts');
    return maxVersion;
  },

  /**
   * Determines if the protocol is a BETA version of the protocol.
   * @param {Number} version
   * @return {Number}
   */
  isBeta: function (version) {
    return version === this.v5;
  }
};

module.exports = protocolVersion;