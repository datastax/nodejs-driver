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



/**
 * @classdesc Describes a CQL materialized view.
 * @alias module:metadata~MaterializedView
 * @augments {module:metadata~DataCollection}
 * @constructor
 */
class MaterializedView extends DataCollection {
  /**
   * Name of the table.
   * @type {String}
   */
  tableName: string;
  /**
   * View where clause.
   * @type {String}
   */
  whereClause: string;
  /**
   * Determines if all the table columns where are included in the view.
   * @type {boolean}
   */
  includeAllColumns: boolean;
  /**
   * Creates a new MaterializedView.
   * @internal
   * @param {String} name Name of the View.
   * @augments {module:metadata~DataCollection}
   * @constructor
   */
  constructor(name: string) {
    super(name);

    this.tableName = null;
    this.whereClause = null;
    this.includeAllColumns = false;
  }
}

export default MaterializedView;