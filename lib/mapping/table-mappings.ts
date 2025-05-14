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



/**
 * Contains a set of methods to represent a row into a document and a document into a row.
 * @alias module:mapping~TableMappings
 * @interface
 */
class TableMappings {
  /**
   * Method that is called by the mapper to create the instance of the document.
   * @return {Object}
   */
  newObjectInstance(): object {
    return {};
  }

  /**
   * Gets the name of the column based on the document property name.
   * @param {String} propName The name of the property.
   * @returns {String}
   */
  getColumnName(propName: string): string {
    return propName;
  }

  /**
   * Gets the name of the document property based on the column name.
   * @param {String} columnName The name of the column.
   * @returns {String}
   */
  getPropertyName(columnName: string): string {
    return columnName;
  }
}

/**
 * A [TableMappings]{@link module:mapping~TableMappings} implementation that converts CQL column names in all-lowercase
 * identifiers with underscores (snake case) to camel case (initial lowercase letter) property names.
 * <p>
 *   The conversion is performed without any checks for the source format, you should make sure that the source
 *   format is snake case for CQL identifiers and camel case for properties.
 * </p>
 * @alias module:mapping~UnderscoreCqlToCamelCaseMappings
 * @implements {module:mapping~TableMappings}
 */
class UnderscoreCqlToCamelCaseMappings extends TableMappings {
  /**
   * Creates a new instance of {@link UnderscoreCqlToCamelCaseMappings}
   */
  constructor() {
    super();
  }

  /**
   * Converts a property name in camel case to snake case.
   * @param {String} propName Name of the property to convert to snake case.
   * @return {String}
   */
  getColumnName(propName: string): string {
    return propName.replace(/[a-z][A-Z]/g, (match, _offset) => match.charAt(0) + '_' + match.charAt(1)).toLowerCase();
  }

  /**
   * Converts a column name in snake case to camel case.
   * @param {String} columnName The column name to convert to camel case.
   * @return {String}
   */
  getPropertyName(columnName: string): string {
    return columnName.replace(/_[a-z]/g, (match, offset) => ((offset === 0) ? match : match.substr(1).toUpperCase()));
  }
}

/**
 * Default implementation of [TableMappings]{@link module:mapping~TableMappings} that doesn't perform any conversion.
 * @alias module:mapping~DefaultTableMappings
 * @implements {module:mapping~TableMappings}
 */
class DefaultTableMappings extends TableMappings {
  /**
   * Creates a new instance of {@link DefaultTableMappings}.
   */
  constructor() {
    super();
  }

  /**  @override */
  getColumnName(propName: string): string {
    return super.getColumnName(propName);
  }

  /** @override */
  getPropertyName(columnName: string): string {
    return super.getPropertyName(columnName);
  }

  /**
   * Creates a new object instance, using object initializer.
   */
  newObjectInstance(): object {
    return super.newObjectInstance();
  }
}

export default {
  TableMappings,
  UnderscoreCqlToCamelCaseMappings,
  DefaultTableMappings
};

export {
  DefaultTableMappings, TableMappings,
  UnderscoreCqlToCamelCaseMappings
};
