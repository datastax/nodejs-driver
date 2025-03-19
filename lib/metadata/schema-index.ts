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
import utils from "../utils";
import types, { Row } from "../types/index";


/** @private */
const kind = {
  custom: 0,
  keys: 1,
  composites: 2
} as const;
/**
 * @classdesc Describes a CQL index.
 * @alias module:metadata~Index
 */
class Index {
  /**
   * Name of the index.
   * @type {String}
   */
  name: string;
  /**
   * Target of the index.
   * @type {String}
   */
  target: string;
  /**
   * A numeric value representing index kind (0: custom, 1: keys, 2: composite);
   * @type {Number}
   */
  kind: number;
  /**
   * An associative array containing the index options
   * @type {Object}
   */
  options: object;
  /**
   * Creates a new Index instance.
   * @classdesc Describes a CQL index.
   * @param {String} name
   * @param {String} target
   * @param {Number|String} kind
   * @param {Object} options
   * @constructor
   */
  constructor(name: string, target: string, kind: number | string, options: object) {
    /**
     * Name of the index.
     * @type {String}
     */
    this.name = name;
    /**
     * Target of the index.
     * @type {String}
     */
    this.target = target;
    /**
     * A numeric value representing index kind (0: custom, 1: keys, 2: composite);
     * @type {Number}
     */
    this.kind = typeof kind === 'string' ? getKindByName(kind) : kind;
    /**
     * An associative array containing the index options
     * @type {Object}
     */
    this.options = options;
  }
  /**
   * Parses Index information from rows in the 'system_schema.indexes' table
   * @deprecated It will be removed in the next major version.
   * @param {Array.<Row>} indexRows
   * @returns {Array.<Index>}
   */
  static fromRows(indexRows: Array<Row>): Array<Index> {
    if (!indexRows || indexRows.length === 0) {
      return utils.emptyArray as Array<Index>;
    }
    return indexRows.map(function (row) {
      const options = row['options'];
      return new Index(row['index_name'], options['target'], getKindByName(row['kind']), options);
    });
  }
  /**
   * Parses Index information from rows in the legacy 'system.schema_columns' table.
   * @deprecated It will be removed in the next major version.
   * @param {Array.<Row>} columnRows
   * @param {Object.<String, {name, type}>} columnsByName
   * @returns {Array.<Index>}
   */
  static fromColumnRows(columnRows: Array<Row>, columnsByName: {[key: string]: {name; type}}): Array<Index> {
    const result = [];
    for (let i = 0; i < columnRows.length; i++) {
      const row = columnRows[i];
      const indexName = row['index_name'];
      if (!indexName) {
        continue;
      }
      const c = columnsByName[row['column_name']];
      let target;
      const options = JSON.parse(row['index_options']);
      if (options !== null && options['index_keys'] !== undefined) {
        target = util.format("keys(%s)", c.name);
      }
      else if (options !== null && options['index_keys_and_values'] !== undefined) {
        target = util.format("entries(%s)", c.name);
      }
      else if (c.type.options.frozen && (c.type.code === types.dataTypes.map || c.type.code === types.dataTypes.list ||
        c.type.code === types.dataTypes.set)) {
        target = util.format("full(%s)", c.name);
      }
      else {
        target = c.name;
      }
      result.push(new Index(indexName, target, getKindByName(row['index_type']), options));
    }
    return result;
  }
  /**
   * Determines if the index is of composites kind
   * @returns {Boolean}
   */
  isCompositesKind(): boolean {
    return this.kind === kind.composites;
  }
  /**
   * Determines if the index is of keys kind
   * @returns {Boolean}
   */
  isKeysKind(): boolean {
    return this.kind === kind.keys;
  }
  /**
   * Determines if the index is of custom kind
   * @returns {Boolean}
   */
  isCustomKind(): boolean {
    return this.kind === kind.custom;
  }
}

/**
 * Gets the number representing the kind based on the name
 * @param {String} name
 * @returns {Number}
 * @private
 */
function getKindByName(name: string): number {
  if (!name) {
    return kind.custom;
  }
  return kind[name.toLowerCase()];
}

export default Index;