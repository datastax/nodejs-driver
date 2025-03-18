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
import { ResultSet, Uuid } from "../../types";
import utils from "../../utils";

/**
 * Represents the result set of a [graph query execution]{@link Client#executeGraph} containing vertices, edges, or
 * scalar values depending on the query.
 * <p>
 * It allows iteration of the items using <code>for..of</code> statements under ES2015 and exposes
 * <code>forEach()</code>, <code>first()</code>, and <code>toArray()</code> to access the underlying items.
 * </p>
 * @example
 * for (let vertex of result) { ... }
 * @example
 * const arr = result.toArray();
 * @example
 * const vertex = result.first();
 * @alias module:datastax/graph~GraphResultSet
 */
class GraphResultSet {
  info: typeof ResultSet.prototype.info;
  length: number;
  pageState: string;
  private rows: any[];
  private rowParser: Function;

  /**
   * @param {ResultSet} result The result set from the query execution.
   * @param {Function} [rowParser] Optional row parser function.
   * @constructor
   */
  constructor(result: ResultSet, rowParser: Function = parsePlainJsonRow) {
    this.info = result.info;
    this.rows = result.rows;
    this.rowParser = rowParser;

    /**
     * This property has been deprecated because it may return a lower value than the actual length of the results.
     * Use <code>toArray()</code> instead.
     * <p>Gets the length of the result.</p>
     * @deprecated Use <code>toArray()</code> instead. This property will be removed in the following major version.
     */
    this.length = result.rowLength;

    /**
     * A string token representing the current page state of the query. It can be used in the following executions to
     * continue paging and retrieve the remainder of the result for the query.
     */
    this.pageState = result.pageState;
  }

  /**
   * Returns the first element of the result or null if the result is empty.
   * @returns {Object}
   */
  first(): object | null {
    const iterator = this.values();
    const item = iterator.next();
    if (item.done) {
      return null;
    }

    return item.value;
  }

  /**
   * Executes a provided function once per result element.
   * @param {Function} callback Function to execute for each element, taking two arguments: currentValue and index.
   * @param {Object} [thisArg] Value to use as <code>this</code> when executing callback.
   */
  forEach(callback: Function, thisArg?: object): void {
    if (!this.rows.length) {
      return;
    }
    const iterator = this.values();
    let item = iterator.next();
    let index = 0;
    while (!item.done) {
      callback.call(thisArg || this, item.value, index++);
      item = iterator.next();
    }
  }

  /**
   * Returns an Array of graph result elements (vertex, edge, scalar).
   * @returns {Array}
   */
  toArray(): Array<any> {
    if (!this.rows.length) {
      return utils.emptyArray as any[];
    }
    return utils.iteratorToArray(this.values());
  }

  /**
   * Returns a new Iterator object that contains the values for each index in the result.
   * @returns {Iterator}
   */
  *values(): Iterator<any> {
    for (const traverser of this.getTraversers()) {
      const bulk = traverser.bulk || 1;

      for (let j = 0; j < bulk; j++) {
        yield traverser.object;
      }
    }
  }

  /**
   * Gets the traversers contained in the result set.
   * @returns {IterableIterator}
   */
  *getTraversers(): IterableIterator<any> {
    for (const row of this.rows) {
      yield this.rowParser(row);
    }
  }

  /**
   * Makes the result set iterable using `for..of`.
   * @returns {Iterator}
   */
  [Symbol.iterator]() {
    return this.values();
  }
}

/**
 * Parses a row into a traverser object.
 * @param {Row} row The row to parse.
 * @returns {Object} The parsed traverser object.
 * @private
 */
function parsePlainJsonRow(row: any): { object: any; bulk: number } {
  const parsed = JSON.parse(row['gremlin']);
  return { object: parsed.result, bulk: parsed.bulk || 1 };
}

export default GraphResultSet;