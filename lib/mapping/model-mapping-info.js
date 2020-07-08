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

const tableMappingsModule = require('./table-mappings');
const TableMappings = tableMappingsModule.TableMappings;
const DefaultTableMappings = tableMappingsModule.DefaultTableMappings;

/**
 * Represents the parsed user information of the table mappings of a model.
 * @ignore
 */
class ModelMappingInfo {
  /**
   * @param {String} keyspace
   * @param {Array<{name, isView}>} tables
   * @param {TableMappings} mappings
   * @param {Map<String,ModelColumnInfo>} columns
   */
  constructor(keyspace, tables, mappings, columns) {
    this.keyspace = keyspace;
    this.tables = tables;
    this._mappings = mappings;
    this._columns = columns;

    // Define a map of column information per property name
    /** @type {Map<String, ModelColumnInfo>} */
    this._documentProperties = new Map();
    for (const modelColumnInfo of columns.values()) {
      this._documentProperties.set(modelColumnInfo.propertyName, modelColumnInfo);
    }
  }

  getColumnName(propName) {
    const modelColumnInfo = this._documentProperties.get(propName);
    if (modelColumnInfo !== undefined) {
      // There is an specific name transformation between the column name and the property name
      return modelColumnInfo.columnName;
    }
    // Rely on the TableMappings (i.e. maybe there is a convention defined for this property)
    return this._mappings.getColumnName(propName);
  }

  getPropertyName(columnName) {
    const modelColumnInfo = this._columns.get(columnName);
    if (modelColumnInfo !== undefined) {
      // There is an specific name transformation between the column name and the property name
      return modelColumnInfo.propertyName;
    }
    // Rely on the TableMappings (i.e. maybe there is a convention defined for this column)
    return this._mappings.getPropertyName(columnName);
  }

  getFromModelFn(propName) {
    const modelColumnInfo = this._documentProperties.get(propName);
    return modelColumnInfo !== undefined ? modelColumnInfo.fromModel : null;
  }

  getToModelFn(columnName) {
    const modelColumnInfo = this._columns.get(columnName);
    return modelColumnInfo !== undefined ? modelColumnInfo.toModel : null;
  }

  newInstance() {
    return this._mappings.newObjectInstance();
  }

  /**
   * Parses the user options into a map of model names and ModelMappingInfo.
   * @param {MappingOptions} options
   * @param {String} currentKeyspace
   * @returns {Map<String, ModelMappingInfo>}
   */
  static parse(options, currentKeyspace) {
    const result = new Map();
    if (!options || !options.models) {
      return result;
    }

    Object.keys(options.models).forEach(modelName => {
      const modelOptions = options.models[modelName];
      result.set(modelName, ModelMappingInfo._create(modelName, currentKeyspace, modelOptions));
    });

    return result;
  }

  static _create(modelName, currentKeyspace, modelOptions) {
    if (!currentKeyspace && (!modelOptions || !modelOptions.keyspace)) {
      throw new Error(
        'You should specify the keyspace of the model in the MappingOptions when the Client is not using a keyspace');
    }

    if (!modelOptions) {
      return ModelMappingInfo.createDefault(modelName, currentKeyspace);
    }

    let tables;

    if (modelOptions.tables && modelOptions.tables.length > 0) {
      tables = modelOptions.tables.map(item => {
        const table = { name: null, isView: false };
        if (typeof item === 'string') {
          table.name = item;
        } else if (item) {
          table.name = item.name;
          table.isView = !!item.isView;
        }

        if (!table.name) {
          throw new Error(`Table name not specified for model '${modelName}'`);
        }

        return table;
      });
    } else {
      tables = [ { name: modelName, isView: false }];
    }

    if (modelOptions.mappings && !(modelOptions.mappings instanceof TableMappings)) {
      throw new Error('mappings should be an instance of TableMappings');
    }

    const columns = new Map();
    if (modelOptions.columns !== null && typeof modelOptions.columns === 'object') {
      Object.keys(modelOptions.columns).forEach(columnName => {
        columns.set(columnName, ModelColumnInfo.parse(columnName, modelOptions.columns[columnName]));
      });
    }

    return new ModelMappingInfo(
      modelOptions.keyspace || currentKeyspace,
      tables,
      modelOptions.mappings || new DefaultTableMappings(),
      columns
    );
  }

  static createDefault(modelName, currentKeyspace) {
    return new ModelMappingInfo(
      currentKeyspace,
      [ { name: modelName, isView: false }],
      new DefaultTableMappings(),
      new Map());
  }
}

class ModelColumnInfo {
  constructor(columnName, propertyName, toModel, fromModel) {
    this.columnName = columnName;
    this.propertyName = propertyName;

    if (toModel && typeof toModel !== 'function') {
      throw new TypeError(`toModel type for property '${propertyName}' should be a function (obtained ${
        typeof toModel})`);
    }

    if (fromModel && typeof fromModel !== 'function') {
      throw new TypeError(`fromModel type for property '${propertyName}' should be a function (obtained ${
        typeof fromModel})`);
    }

    this.toModel = toModel;
    this.fromModel = fromModel;
  }

  static parse(columnName, value) {
    if (!value) {
      return new ModelColumnInfo(columnName, columnName);
    }

    if (typeof value === 'string') {
      return new ModelColumnInfo(columnName, value);
    }

    return new ModelColumnInfo(columnName, value.name || columnName, value.toModel, value.fromModel);
  }
}

module.exports = ModelMappingInfo;