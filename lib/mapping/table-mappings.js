'use strict';

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
  newObjectInstance() {
    return {};
  }

  /**
   * Gets the name of the column based on the document property name.
   * @param {String} propName The name of the property.
   * @returns {String}
   */
  getColumnName(propName) {
    return propName;
  }

  /**
   * Gets the name of the document property based on the column name.
   * @param {String} columnName The name of the column.
   * @returns {String}
   */
  getPropertyName(columnName) {
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
  getColumnName(propName) {
    return propName.replace(/[a-z][A-Z]/g, (match, offset) => match.charAt(0) + '_' + match.charAt(1)).toLowerCase();
  }

  /**
   * Converts a column name in snake case to camel case.
   * @param {String} columnName The column name to convert to camel case.
   * @return {String}
   */
  getPropertyName(columnName) {
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
  getColumnName(propName) {
    return super.getColumnName(propName);
  }

  /** @override */
  getPropertyName(columnName) {
    return super.getPropertyName(columnName);
  }

  /**
   * Creates a new object instance, using object initializer.
   */
  newObjectInstance() {
    return super.newObjectInstance();
  }
}

exports.TableMappings = TableMappings;
exports.UnderscoreCqlToCamelCaseMappings = UnderscoreCqlToCamelCaseMappings;
exports.DefaultTableMappings = DefaultTableMappings;