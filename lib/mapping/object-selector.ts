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

const keyMatches = {
  all: 1,
  none: 0,
  some: -1
};

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

          if (keysAreIncluded(table.partitionKeys, propertiesInfo) !== keyMatches.all) {
            // Not all the partition keys are covered
            continue;
          }


          if (allPKsDefined) {
            if (keysAreIncluded(table.clusteringKeys, propertiesInfo) !== keyMatches.all) {
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

          // CQL:
          // - "ORDER BY" is currently only supported on the clustered columns of the PRIMARY KEY
          // - "ORDER BY" currently only support the ordering of columns following their declared order in
          //   the PRIMARY KEY
          //
          // In the mapper, we validate that the ORDER BY columns appear in the same order as in the clustering keys
          const containsAllOrderByColumns = orderByColumns
            .reduce((acc, order, index) => {
              if (!acc) {
                return false;
              }

              const ck = table.clusteringKeys[index];

              return ck && ck.name === order[0];
            }, true);

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

            if (keysAreIncluded(table.partitionKeys, propertiesInfo) !== keyMatches.all) {
              // Not all the partition keys are covered
              return false;
            }

            const clusteringKeyMatches = keysAreIncluded(table.clusteringKeys, propertiesInfo);

            // All clustering keys should be included or it can be inserting a static column value
            if (clusteringKeyMatches === keyMatches.all) {
              return true;
            }

            if (clusteringKeyMatches === keyMatches.some) {
              return false;
            }

            const staticColumns = staticColumnCount(table);
            return propertiesInfo.length === table.partitionKeys.length + staticColumns && staticColumns > 0;
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

            if (keysAreIncluded(table.partitionKeys, propertiesInfo) !== keyMatches.all) {
              // Not all the partition keys are covered
              return false;
            }

            const clusteringKeyMatches = keysAreIncluded(table.clusteringKeys, propertiesInfo);

            // All clustering keys should be included or it can be updating a static column value
            if (clusteringKeyMatches === keyMatches.some) {
              return false;
            }

            if (clusteringKeyMatches === keyMatches.none && !hasStaticColumn(table)) {
              return false;
            }

            const applicableColumns = propertiesInfo
              .reduce((acc, p) => acc + (table.columnsByName[p.columnName] !== undefined ? 1 : 0), 0);

            if (applicableColumns <= table.partitionKeys.length + table.clusteringKeys.length) {
              if (!hasStaticColumn(table) || applicableColumns <= table.partitionKeys.length) {
                // UPDATE statement does not contain columns to SET
                return false;
              }
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

/**
 * Returns the amount of matches for a given key
 * @private
 * @param {Array} keys
 * @param {Array} propertiesInfo
 */
function keysAreIncluded(keys, propertiesInfo) {
  if (keys.length === 0) {
    return keyMatches.all;
  }

  // Filtering by name might look slow / ineffective to using hash maps
  // but we expect `keys` and `propertiesInfo` to contain only few items
  const matches = propertiesInfo.reduce((acc, p) => acc + (contains(keys, k => p.columnName === k.name) ? 1 : 0), 0);
  if (matches === 0) {
    return keyMatches.none;
  }

  return matches === keys.length ? keyMatches.all : keyMatches.some;
}

function hasStaticColumn(table) {
  return staticColumnCount(table) > 0;
}

function staticColumnCount(table) {
  return table.columns.reduce((acc, column) => acc + (column.isStatic ? 1 : 0), 0);
}

module.exports = ObjectSelector;
