'use strict';

const format = require('util').format;

/**
 * Provides utility methods to choose the correct tables and views that should be included in a statement.
 */
class ObjectSelector {
  /**
   * Gets the table/view that should be used to execute the SELECT query.
   * @param {Client} client
   * @param {TableMappingInfo} info
   * @param {Array} columnKeys
   * @param {Array<String>} fieldColumns
   * @param {Array<Array<String>>} orderByColumns
   * @return {Promise<String>} A promise that resolves to a table names.
   */
  static getForSelect(client, info, columnKeys, fieldColumns, orderByColumns) {
    if (info.tables.length === 1) {
      return Promise.resolve(info.tables[0].name);
    }

    return Promise.all(
      info.tables.map(t => {
        if (t.isView) {
          return client.metadata.getMaterializedView(info.keyspace, t.name);
        }
        return client.metadata.getTable(info.keyspace, t.name);
      }))
      .then(tables => {
        for (let i = 0; i < tables.length; i++) {
          const table = tables[i];
          if (table === null) {
            throw new Error(format('Table %s could not be retrieved', info.tables[i].name));
          }

          let allPartitionKeysAreIncluded = true;

          const partitionKeys = new Map(table.partitionKeys.map(k => {
            allPartitionKeysAreIncluded = allPartitionKeysAreIncluded && columnKeys.indexOf(k.name) >= 0;
            return [ k.name, true ];
          }));

          if (!allPartitionKeysAreIncluded) {
            // Not all the partition keys are covered
            continue;
          }

          if (columnKeys.length > table.partitionKeys.length) {
            // Validate that all keys are clustered keys
            const notCoveredByClusteringKeys = columnKeys
              .filter(name => !partitionKeys.get(name) && table.clusteringKeys.indexOf(name) === -1).length > 0;

            if (notCoveredByClusteringKeys) {
              continue;
            }
          }

          // All fields must be contained
          const containsAllFields = fieldColumns
            .reduce((acc, columnName) => acc && table.columnsByName[columnName] !== undefined, true);

          if (!containsAllFields) {
            continue;
          }

          // ORDER BY fields must be part of the clustering keys
          // On the mapper we only validate that are part of the table
          const containsAllOrderByColumns = orderByColumns
            .reduce((acc, order) => acc && table.columnsByName[order[0]] !== undefined, true);

          if (!containsAllOrderByColumns) {
            continue;
          }

          return table.name;
        }

        throw new Error(format('No configured table matches the columns: %j', columnKeys));
      });
  }

  /**
   * Gets the tables that should be used to execute the INSERT query.
   * @param {Client} client
   * @param {TableMappingInfo} info
   * @param {Array} propertiesInfo
   * @return {Promise<Array<TableMetadata>>} A promise that resolves to an Array of tables.
   */
  static getForInsert(client, info, propertiesInfo) {
    if (info.tables.length === 1) {
      return Promise.resolve(info.tables[0].name);
    }

    return Promise.all(info.tables.filter(t => !t.isView).map(t => client.metadata.getTable(info.keyspace, t.name)))
      .then(tables => {
        const filteredTables = tables
          .filter((table, i) => {
            if (table === null) {
              throw new Error(format('Table %s could not be retrieved', info.tables[i].name));
            }

            // All partition and clustering keys from the table should be included in the document
            const keyNames = table.partitionKeys.concat(table.clusteringKeys).map(k => k.name);
            const columns = propertiesInfo.map(m => m.columnName);

            for (let i = 0; i < keyNames.length; i++) {
              if (columns.indexOf(keyNames[i]) === -1) {
                return false;
              }
            }

            return true;
          });

        if (filteredTables.length === 0) {
          throw new Error('No table matches the document information: all primary keys should be specified');
        }

        return filteredTables;
      });
  }
}

module.exports = ObjectSelector;