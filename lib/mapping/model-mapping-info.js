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
   * @param {Map<String,String>} columns
   */
  constructor(keyspace, tables, mappings, columns) {
    this.keyspace = keyspace;
    this.tables = tables;
    this._mappings = mappings;
    this._columns = columns;
    this._documentProperties = new Map();
    columns.forEach((propName, columnName) => this._documentProperties.set(propName, columnName));
  }

  getColumnName(propName) {
    const columnName = this._documentProperties.get(propName);
    if (columnName !== undefined) {
      // There is an specific name transformation between the column name and the property name
      return columnName;
    }
    // Rely on the TableMappings (i.e. maybe there is a convention defined for this property)
    return this._mappings.getColumnName(propName);
  }

  getPropertyName(columnName) {
    const propName = this._columns.get(columnName);
    if (propName !== undefined) {
      // There is an specific name transformation between the column name and the property name
      return propName;
    }
    // Rely on the TableMappings (i.e. maybe there is a convention defined for this column)
    return this._mappings.getPropertyName(columnName);
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
        columns.set(columnName, modelOptions.columns[columnName]);
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

module.exports = ModelMappingInfo;