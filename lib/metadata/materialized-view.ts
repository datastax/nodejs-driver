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
const util = require('util');
const DataCollection = require('./data-collection');
/**
 * Creates a new MaterializedView.
 * @param {String} name Name of the View.
 * @classdesc Describes a CQL materialized view.
 * @alias module:metadata~MaterializedView
 * @augments {module:metadata~DataCollection}
 * @constructor
 */
function MaterializedView(name) {
  DataCollection.call(this, name);
  /**
   * Name of the table.
   * @type {String}
   */
  this.tableName = null;
  /**
   * View where clause.
   * @type {String}
   */
  this.whereClause = null;
  /**
   * Determines if all the table columns where are included in the view.
   * @type {boolean}
   */
  this.includeAllColumns = false;
}

util.inherits(MaterializedView, DataCollection);

module.exports = MaterializedView;