'use strict';

/**
 * Provides utility methods to choose the correct tables and views that should be included in a statement.
 * @ignore
 */
class ObjectSelector {
  /**
   * Gets the table/view that should be used to execute the SELECT query.
   * @param {Client} client
   * @param {ModelMappingInfo} info
   * @param {Boolean} allPKsDefined
   * @param {Array} propertiesInfo
   * @param {Array} fieldsInfo
   * @param {Array<Array<String>>} orderByColumns
   * @return {Promise<String>} A promise that resolves to a table names.
   */
  static getForSelect(client, info, allPKsDefined, propertiesInfo, fieldsInfo, orderByColumns) {
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
            throw new Error(`Table "${info.tables[i].name}" could not be retrieved`);
          }

          const allPartitionKeysAreIncluded = table.partitionKeys
            .reduce((acc, c) => acc && contains(propertiesInfo, p => p.columnName === c.name), true);

          if (!allPartitionKeysAreIncluded) {
            // Not all the partition keys are covered
            continue;
          }

          if (allPKsDefined) {
            const allClusteringKeysAreIncluded = table.clusteringKeys
              .reduce((acc, c) => acc && contains(propertiesInfo, p => p.columnName === c.name), true);
            if (!allClusteringKeysAreIncluded) {
              // All clustering keys should be included as allPKsDefined flag is set
              continue;
            }
          }

          if (propertiesInfo.length > table.partitionKeys.length) {
            // Check that the Where clause is composed by partition and clustering keys
            const allPropertiesArePrimaryKeys = propertiesInfo
              .reduce(
                (acc, p) => acc && (
                  contains(table.partitionKeys, c => c.name === p.columnName) ||
                  contains(table.clusteringKeys, c => c.name === p.columnName)
                ),
                true);

            if (!allPropertiesArePrimaryKeys) {
              continue;
            }
          }

          // All fields must be contained
          const containsAllFields = fieldsInfo
            .reduce((acc, p) => acc && table.columnsByName[p.columnName] !== undefined, true);

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

        let message = `No table matches the filter (${allPKsDefined ? 'all PKs have to be specified' : 'PKs'}): [${
          propertiesInfo.map(p => p.columnName)}]`;

        if (fieldsInfo.length > 0) {
          message += `; fields: [${fieldsInfo.map(p => p.columnName)}]`;
        }
        if (orderByColumns.length > 0) {
          message += `; orderBy: [${orderByColumns.map(item => item[0])}]`;
        }

        throw new Error(message);
      });
  }

  /** Returns the name of the first table */
  static getForSelectAll(info) {
    return info.tables[0].name;
  }

  /**
   * Gets the tables that should be used to execute the INSERT query.
   * @param {Client} client
   * @param {ModelMappingInfo} info
   * @param {Array} propertiesInfo
   * @return {Promise<Array<TableMetadata>>} A promise that resolves to an Array of tables.
   */
  static getForInsert(client, info, propertiesInfo) {
    return Promise.all(info.tables.filter(t => !t.isView).map(t => client.metadata.getTable(info.keyspace, t.name)))
      .then(tables => {
        const filteredTables = tables
          .filter((table, i) => {
            if (table === null) {
              throw new Error(`Table "${info.tables[i].name}" could not be retrieved`);
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
          throw new Error(`No table matches (all PKs have to be specified) fields: [${
            propertiesInfo.map(p => p.columnName)}]`);
        }

        return filteredTables;
      });
  }

  /**
   * Gets the tables that should be used to execute the UPDATE query.
   * @param {Client} client
   * @param {ModelMappingInfo} info
   * @param {Array} propertiesInfo
   * @param {Array} when
   * @return {Promise<Array<TableMetadata>>} A promise that resolves to an Array of tables.
   */
  static getForUpdate(client, info, propertiesInfo, when) {
    return Promise.all(info.tables.filter(t => !t.isView).map(t => client.metadata.getTable(info.keyspace, t.name)))
      .then(tables => {
        const filteredTables = tables
          .filter((table, i) => {
            if (table === null) {
              throw new Error(`Table "${info.tables[i].name}" could not be retrieved`);
            }

            // All partition and clustering keys from the table should be included in the document
            const keyNames = table.partitionKeys.concat(table.clusteringKeys).map(k => k.name);
            const columns = new Set(propertiesInfo.map(p => p.columnName));
            const allPrimaryKeysAreContained = keyNames.reduce((acc, key) => acc && columns.has(key), true);

            if (!allPrimaryKeysAreContained) {
              return false;
            }

            const applicableColumns = propertiesInfo
              .reduce((acc, p) => acc + (table.columnsByName[p.columnName] !== undefined ? 1 : 0), 0);

            if (applicableColumns <= keyNames.length) {
              // No SET columns are defined
              return false;
            }

            // "when" conditions should be contained in the table
            return when.reduce((acc, p) => acc && table.columnsByName[p.columnName] !== undefined, true);
          });

        if (filteredTables.length === 0) {
          let message = `No table matches (all PKs and columns to set have to be specified) fields: [${
            propertiesInfo.map(p => p.columnName)}]`;

          if (when.length > 0) {
            message += `; condition: [${when.map(p => p.columnName)}]`;
          }

          throw new Error(message);
        }

        return filteredTables;
      });
  }

  /**
   * Gets the tables that should be used to execute the DELETE query.
   * @param {Client} client
   * @param {ModelMappingInfo} info
   * @param {Array} propertiesInfo
   * @param {Array} when
   * @return {Promise<Array<TableMetadata>>} A promise that resolves to an Array of tables.
   */
  static getForDelete(client, info, propertiesInfo, when) {
    return Promise.all(info.tables.filter(t => !t.isView).map(t => client.metadata.getTable(info.keyspace, t.name)))
      .then(tables => {
        const filteredTables = tables
          .filter((table, i) => {
            if (table === null) {
              throw new Error(`Table "${info.tables[i].name}" could not be retrieved`);
            }

            // All partition and clustering keys from the table should be included in the document
            const keyNames = table.partitionKeys.concat(table.clusteringKeys).map(k => k.name);
            const columns = propertiesInfo.map(p => p.columnName);

            for (let i = 0; i < keyNames.length; i++) {
              if (columns.indexOf(keyNames[i]) === -1) {
                return false;
              }
            }

            // "when" conditions should be contained in the table
            return when.reduce((acc, p) => acc && table.columnsByName[p.columnName] !== undefined, true);
          });

        if (filteredTables.length === 0) {
          let message = `No table matches (all PKs have to be specified) fields: [${
            propertiesInfo.map(p => p.columnName)}]`;

          if (when.length > 0) {
            message += `; condition: [${when.map(p => p.columnName)}]`;
          }

          throw new Error(message);
        }

        return filteredTables;
      });
  }
}

function contains(arr, fn) {
  return arr.filter(fn).length > 0;
}

module.exports = ObjectSelector;