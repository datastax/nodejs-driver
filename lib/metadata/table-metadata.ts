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
import DataCollection from "./data-collection";
import Index from "./schema-index";



/**
 * @classdesc Describes a table
 * @augments {module:metadata~DataCollection}
 * @alias module:metadata~TableMetadata
 */
class TableMetadata extends DataCollection {
  /**
   * Applies only to counter tables.
   * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
   * the client for a write request. For counter tables, this should always be set to true.
   * @type {Boolean}
   */
  replicateOnWrite: boolean;
  /**
   * Returns the memtable flush period (in milliseconds) option for this table.
   * @type {Number}
   */
  memtableFlushPeriod: number;
  /**
   * Returns the index interval option for this table.
   * <p>
   * Note: this option is only available in Apache Cassandra 2.0. It is deprecated in Apache Cassandra 2.1 and
   * above, and will therefore return <code>null</code> for 2.1 nodes.
   * </p>
   * @type {Number|null}
   */
  indexInterval: number | null;
  /**
   * Determines  whether the table uses the COMPACT STORAGE option.
   * @type {Boolean}
   */
  isCompact: boolean;
  /**
   *
   * @type {Array.<Index>}
   */
  indexes: Array<Index>;

  /**
   * Determines whether the Change Data Capture (CDC) flag is set for the table.
   * @type {Boolean|null}
   */
  cdc: boolean | null;

  /**
   * Determines whether the table is a virtual table or not.
   * @type {Boolean}
   */
  virtual: boolean = false;
  /**
   * Creates a new instance of TableMetadata
   * @param {String} name Name of the Table
   * @constructor
   */
  constructor(name: string) {
    super(name);
    /**
     * Applies only to counter tables.
     * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by
     * the client for a write request. For counter tables, this should always be set to true.
     * @type {Boolean}
     */
    this.replicateOnWrite = true;
    /**
     * Returns the memtable flush period (in milliseconds) option for this table.
     * @type {Number}
     */
    this.memtableFlushPeriod = 0;
    /**
     * Returns the index interval option for this table.
     * <p>
     * Note: this option is only available in Apache Cassandra 2.0. It is deprecated in Apache Cassandra 2.1 and
     * above, and will therefore return <code>null</code> for 2.1 nodes.
     * </p>
     * @type {Number|null}
     */
    this.indexInterval = null;
    /**
     * Determines  whether the table uses the COMPACT STORAGE option.
     * @type {Boolean}
     */
    this.isCompact = false;
    /**
     *
     * @type {Array.<Index>}
     */
    this.indexes = null;

    /**
     * Determines whether the Change Data Capture (CDC) flag is set for the table.
     * @type {Boolean|null}
     */
    this.cdc = null;

    /**
     * Determines whether the table is a virtual table or not.
     * @type {Boolean}
     */
    this.virtual = false;
  }
}

export default TableMetadata;