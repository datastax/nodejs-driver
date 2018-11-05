"use strict";
const util = require('util');
const events = require('events');
/**
 * Creates a new instance of DataCollection
 * @param {String} name Name of the data object.
 * @classdesc Describes a table or a view
 * @alias module:metadata~DataCollection
 * @constructor
 * @abstract
 */
function DataCollection(name) {
  events.EventEmitter.call(this);
  this.setMaxListeners(0);
  //private
  Object.defineProperty(this, 'loading', { value: false, enumerable: false, writable: true });
  Object.defineProperty(this, 'loaded', { value: false, enumerable: false, writable: true });
  /**
   * Name of the object
   * @type {String}
   */
  this.name = name;
  /**
   * False-positive probability for SSTable Bloom filters.
   * @type {number}
   */
  this.bloomFilterFalsePositiveChance = 0;
  /**
   * Level of caching: all, keys_only, rows_only, none
   * @type {String}
   */
  this.caching = null;
  /**
   * A human readable comment describing the table.
   * @type {String}
   */
  this.comment = null;
  /**
   * Specifies the time to wait before garbage collecting tombstones (deletion markers)
   * @type {number}
   */
  this.gcGraceSeconds = 0;
  /**
   * Compaction strategy class used for the table.
   * @type {String}
   */
  this.compactionClass = null;
  /**
   * Associative-array containing the compaction options keys and values.
   * @type {Object}
   */
  this.compactionOptions = null;
  /**
   * Associative-array containing the compression options.
   * @type {Object}
   */
  this.compression = null;
  /**
   * Specifies the probability of read repairs being invoked over all replicas in the current data center.
   * @type {number}
   */
  this.localReadRepairChance = 0;
  /**
   * Specifies the probability with which read repairs should be invoked on non-quorum reads. The value must be
   * between 0 and 1.
   * @type {number}
   */
  this.readRepairChance = 0;
  /**
   * An associative Array containing extra metadata for the table.
   * <p>
   * For Apache Cassandra versions prior to 3.0.0, this method always returns <code>null</code>.
   * </p>
   * @type {Object}
   */
  this.extensions = null;
  /**
   * When compression is enabled, this option defines the probability
   * with which checksums for compressed blocks are checked during reads.
   * The default value for this options is 1.0 (always check).
   * <p>
   *   For Apache Cassandra versions prior to 3.0.0, this method always returns <code>null</code>.
   * </p>
   * @type {Number|null}
   */
  this.crcCheckChance = null;
  /**
   * Whether the populate I/O cache on flush is set on this table.
   * @type {Boolean}
   */
  this.populateCacheOnFlush = false;
  /**
   * Returns the default TTL for this table.
   * @type {Number}
   */
  this.defaultTtl = 0;
  /**
   * * Returns the speculative retry option for this table.
   * @type {String}
   */
  this.speculativeRetry = 'NONE';
  /**
   * Returns the minimum index interval option for this table.
   * <p>
   *   Note: this option is available in Apache Cassandra 2.1 and above, and will return <code>null</code> for
   *   earlier versions.
   * </p>
   * @type {Number|null}
   */
  this.minIndexInterval = 128;
  /**
   * Returns the maximum index interval option for this table.
   * <p>
   * Note: this option is available in Apache Cassandra 2.1 and above, and will return <code>null</code> for
   * earlier versions.
   * </p>
   * @type {Number|null}
   */
  this.maxIndexInterval = 2048;
  /**
   * Array describing the table columns.
   * @type {Array}
   */
  this.columns = null;
  /**
   * An associative Array of columns by name.
   * @type {Object}
   */
  this.columnsByName = null;
  /**
   * Array describing the columns that are part of the partition key.
   * @type {Array}
   */
  this.partitionKeys = [];
  /**
   * Array describing the columns that form the clustering key.
   * @type {Array}
   */
  this.clusteringKeys = [];
  /**
   * Array describing the clustering order of the columns in the same order as the clusteringKeys.
   * @type {Array}
   */
  this.clusteringOrder = [];
}

util.inherits(DataCollection, events.EventEmitter);

module.exports = DataCollection;