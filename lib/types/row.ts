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

/** @module types */
/**
 * Represents a result row
 * @param {Array} columns
 * @constructor
 */
class Row {
  private readonly __columns: Array<any>;
  [key: string]: any;

  /** @internal */
  constructor(columns: Array<any>) {
    if (!columns) {
      throw new Error('Columns not defined');
    }
    //Private non-enumerable properties, with double underscore to avoid interfering with column names
    Object.defineProperty(this, '__columns', { value: columns, enumerable: false, writable: false});
  }

  /**
   * Returns the cell value.
   * @param {String|Number} columnName Name or index of the column
   */
  get(columnName: string | number): any {
    if (typeof columnName === 'number') {
      //its an index
      return this[this.__columns[columnName].name];
    }
    return this[columnName];
  }

  /**
   * Returns an array of the values of the row
   * @returns {Array}
   */
  values(): Array<any> {
    const valuesArray = [];
    this.forEach(function (val) {
      valuesArray.push(val);
    });
    return valuesArray;
  }

  /**
   * Returns an array of the column names of the row
   * @returns {Array}
   */
  keys(): string[] {
    const keysArray = [];
    this.forEach(function (val, key) {
      keysArray.push(key);
    });
    return keysArray;
  }

  //TODO: was exposed as forEach(callback: (row: Row) => void): void;
  /**
   * Executes the callback for each field in the row, containing the value as first parameter followed by the columnName
   * @param {Function} callback
   */
  forEach(callback: (val: any, key: string) => void): void {
    for (const columnName in this) {
      if (!this.hasOwnProperty(columnName)) {
        continue;
      }
      callback(this[columnName], columnName);
    }
  }
}

export default Row;