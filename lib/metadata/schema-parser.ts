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
const events = require('events');
const types = require('../types');
const utils = require('../utils');
const errors = require('../errors');
const promiseUtils = require('../promise-utils');
const TableMetadata = require('./table-metadata');
const Aggregate = require('./aggregate');
const SchemaFunction = require('./schema-function');
const Index = require('./schema-index');
const MaterializedView = require('./materialized-view');
const { format } = util;

/**
 * @module metadata/schemaParser
 * @ignore
 */

const _selectAllKeyspacesV1 = "SELECT * FROM system.schema_keyspaces";
const _selectSingleKeyspaceV1 = "SELECT * FROM system.schema_keyspaces where keyspace_name = '%s'";
const _selectAllKeyspacesV2 = "SELECT * FROM system_schema.keyspaces";
const _selectSingleKeyspaceV2 = "SELECT * FROM system_schema.keyspaces where keyspace_name = '%s'";
const _selectTableV1 = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'";
const _selectTableV2 = "SELECT * FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s'";
const _selectColumnsV1 = "SELECT * FROM system.schema_columns WHERE keyspace_name='%s' AND columnfamily_name='%s'";
const _selectColumnsV2 = "SELECT * FROM system_schema.columns WHERE keyspace_name='%s' AND table_name='%s'";
const _selectIndexesV2 = "SELECT * FROM system_schema.indexes WHERE keyspace_name='%s' AND table_name='%s'";
const _selectUdtV1 = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='%s' AND type_name='%s'";
const _selectUdtV2 = "SELECT * FROM system_schema.types WHERE keyspace_name='%s' AND type_name='%s'";
const _selectFunctionsV1 = "SELECT * FROM system.schema_functions WHERE keyspace_name = '%s' AND function_name = '%s'";
const _selectFunctionsV2 = "SELECT * FROM system_schema.functions WHERE keyspace_name = '%s' AND function_name = '%s'";
const _selectAggregatesV1 = "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";
const _selectAggregatesV2 = "SELECT * FROM system_schema.aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";
const _selectMaterializedViewV2 = "SELECT * FROM system_schema.views WHERE keyspace_name = '%s' AND view_name = '%s'";

const _selectAllVirtualKeyspaces = "SELECT * FROM system_virtual_schema.keyspaces";
const _selectSingleVirtualKeyspace = "SELECT * FROM system_virtual_schema.keyspaces where keyspace_name = '%s'";
const _selectVirtualTable = "SELECT * FROM system_virtual_schema.tables where keyspace_name = '%s' and table_name='%s'";
const _selectVirtualColumns = "SELECT * FROM system_virtual_schema.columns where keyspace_name = '%s' and table_name='%s'";


/**
 * @abstract
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc
 * @constructor
 * @ignore
 */
class SchemaParser {
  constructor(options, cc) {
    this.cc = cc;
    this.encodingOptions = options.encoding;
    this.selectTable = null;
    this.selectColumns = null;
    this.selectIndexes = null;
    this.selectUdt = null;
    this.selectAggregates = null;
    this.selectFunctions = null;
    this.supportsVirtual = false;
  }

  /**
   * @param name
   * @param durableWrites
   * @param strategy
   * @param strategyOptions
   * @param virtual
   * @returns {{name, durableWrites, strategy, strategyOptions, tokenToReplica, udts, tables, functions, aggregates}}
   * @protected
   */
  _createKeyspace(name, durableWrites, strategy, strategyOptions, virtual) {
    return {
      name,
      durableWrites,
      strategy,
      strategyOptions,
      virtual: virtual === true,
      udts: {},
      tables: {},
      functions: {},
      aggregates: {},
      views: {},
      tokenToReplica: getTokenToReplicaMapper(strategy, strategyOptions),
      graphEngine: undefined
    };
  }

  /**
   * @abstract
   * @param {String} name
   * @returns {Promise<Object>}
   */
  getKeyspace(name) {
  }

  /**
   * @abstract
   * @param {Boolean} waitReconnect
   * @returns {Promise<Object<string, Object>>}
   */
  getKeyspaces(waitReconnect) {
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @param {Object} cache
   * @param {Boolean} virtual
   * @returns {Promise<TableMetadata|null>}
   */
  async getTable(keyspaceName, name, cache, virtual) {
    let tableInfo = cache && cache[name];
    if (!tableInfo) {
      tableInfo = new TableMetadata(name);
      if (cache) {
        cache[name] = tableInfo;
      }
    }
    if (tableInfo.loaded) {
      return tableInfo;
    }
    if (tableInfo.loading) {
      // Wait for it to emit
      return promiseUtils.fromEvent(tableInfo, 'load');
    }
    try {
      // its not cached and not being retrieved
      tableInfo.loading = true;
      let indexRows;
      let virtualTable = virtual;
      const selectTable = virtualTable ? _selectVirtualTable : this.selectTable;
      const query = util.format(selectTable, keyspaceName, name);
      let tableRow = await this._getFirstRow(query);
      // if we weren't sure if table was virtual or not, query virtual schema.
      if (!tableRow && this.supportsVirtual && virtualTable === undefined) {
        const query = util.format(_selectVirtualTable, keyspaceName, name);
        try {
          tableRow = await this._getFirstRow(query);
        }
        catch (err) {
          // we can't error here as we can't be sure if the node
          // supports virtual tables, in this case it is adequate
          // to act as if there was no matching table.
        }
        if (tableRow) {
          // We are fetching a virtual table
          virtualTable = true;
        }
      }
      if (!tableRow) {
        tableInfo.loading = false;
        tableInfo.emit('load', null, null);
        return null;
      }
      const selectColumns = virtualTable ? _selectVirtualColumns : this.selectColumns;
      const columnRows = await this._getRows(util.format(selectColumns, keyspaceName, name));
      if (this.selectIndexes && !virtualTable) {
        indexRows = await this._getRows(util.format(this.selectIndexes, keyspaceName, name));
      }
      await this._parseTableOrView(tableInfo, tableRow, columnRows, indexRows, virtualTable);
      tableInfo.loaded = true;
      tableInfo.emit('load', null, tableInfo);
      return tableInfo;
    }
    catch (err) {
      tableInfo.emit('load', err, null);
      throw err;
    }
    finally {
      tableInfo.loading = false;
    }
  }

  async _getFirstRow(query) {
    const rows = await this._getRows(query);
    return rows[0];
  }

  async _getRows(query) {
    const response = await this.cc.query(query);
    return response.rows;
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @param {Object} cache
   * @returns {Promise<Object|null>}
   */
  async getUdt(keyspaceName, name, cache) {
    let udtInfo = cache && cache[name];
    if (!udtInfo) {
      udtInfo = new events.EventEmitter();
      if (cache) {
        cache[name] = udtInfo;
      }
      udtInfo.setMaxListeners(0);
      udtInfo.loading = false;
      udtInfo.name = name;
      udtInfo.keyspace = keyspaceName;
      udtInfo.fields = null;
    }
    if (udtInfo.fields) {
      return udtInfo;
    }
    if (udtInfo.loading) {
      return promiseUtils.fromEvent(udtInfo, 'load');
    }
    udtInfo.loading = true;
    const query = format(this.selectUdt, keyspaceName, name);
    try {
      const row = await this._getFirstRow(query);
      if (!row) {
        udtInfo.loading = false;
        udtInfo.emit('load', null, null);
        return null;
      }
      await this._parseUdt(udtInfo, row);
      udtInfo.emit('load', null, udtInfo);
      return udtInfo;
    }
    catch (err) {
      udtInfo.emit('load', err);
      throw err;
    }
    finally {
      udtInfo.loading = false;
    }
  }

  /**
   * Parses the udt information from the row
   * @param udtInfo
   * @param {Row} row
   * @returns {Promise<void>}
   * @abstract
   */
  _parseUdt(udtInfo, row) {
  }

  /**
   * Builds the metadata based on the table and column rows
   * @abstract
   * @param {module:metadata~TableMetadata} tableInfo
   * @param {Row} tableRow
   * @param {Array.<Row>} columnRows
   * @param {Array.<Row>} indexRows
   * @param {Boolean} virtual
   * @returns {Promise<void>}
   * @throws {Error}
   */
  async _parseTableOrView(tableInfo, tableRow, columnRows, indexRows, virtual) {
  }

  /**
   * @abstract
   * @param {String} keyspaceName
   * @param {String} name
   * @param {Object} cache
   * @returns {Promise<MaterializedView|null>}
   */
  getMaterializedView(keyspaceName, name, cache) {
  }

  /**
   * @param {String} keyspaceName
   * @param {String} name
   * @param {Boolean} aggregate
   * @param {Object} cache
   * @returns {Promise<Map>}
   */
  async getFunctions(keyspaceName, name, aggregate, cache) {
    /** @type {String} */
    let query = this.selectFunctions;
    let parser = row => this._parseFunction(row);
    if (aggregate) {
      query = this.selectAggregates;
      parser = row => this._parseAggregate(row);
    }
    // if it's not already loaded, get all functions with that name
    // cache it by name and, within name, by signature
    let functionsInfo = cache && cache[name];
    if (!functionsInfo) {
      functionsInfo = new events.EventEmitter();
      if (cache) {
        cache[name] = functionsInfo;
      }
      functionsInfo.setMaxListeners(0);
    }
    if (functionsInfo.values) {
      return functionsInfo.values;
    }
    if (functionsInfo.loading) {
      return promiseUtils.fromEvent(functionsInfo, 'load');
    }
    functionsInfo.loading = true;
    try {
      const rows = await this._getRows(format(query, keyspaceName, name));
      const funcs = await Promise.all(rows.map(parser));
      const result = new Map();
      if (rows.length > 0) {
        // Cache positive hits
        functionsInfo.values = result;
      }

      funcs.forEach(f => functionsInfo.values.set(f.signature.join(','), f));
      functionsInfo.emit('load', null, result);
      return result;
    }
    catch (err) {
      functionsInfo.emit('load', err);
      throw err;
    }
    finally {
      functionsInfo.loading = false;
    }
  }

  /**
   * @abstract
   * @param {Row} row
   * @returns {Promise}
   */
  _parseAggregate(row) {
  }

  /**
   * @abstract
   * @param {Row} row
   * @returns {Promise}
   */
  _parseFunction(row) {
  }

  /** @returns {Map} */
  _asMap(obj) {
    if (!obj) {
      return new Map();
    }
    if (this.encodingOptions.map && obj instanceof this.encodingOptions.map) {
      // Its already a Map or a polyfill of a Map
      return obj;
    }
    return new Map(Object.keys(obj).map(k => [k, obj[k]]));
  }

  _mapAsObject(map) {
    if (!map) {
      return map;
    }
    if (this.encodingOptions.map && map instanceof this.encodingOptions.map) {
      const result = {};
      map.forEach((value, key) => result[key] = value);
      return result;
    }
    return map;
  }
}

/**
 * Used to parse schema information for Cassandra versions 1.2.x, and 2.x
 * @ignore
 */
class SchemaParserV1 extends SchemaParser {

  /**
   * @param {ClientOptions} options
   * @param {ControlConnection} cc
   */
  constructor(options, cc) {
    super(options, cc);
    this.selectTable = _selectTableV1;
    this.selectColumns = _selectColumnsV1;
    this.selectUdt = _selectUdtV1;
    this.selectAggregates = _selectAggregatesV1;
    this.selectFunctions = _selectFunctionsV1;
  }

  async getKeyspaces(waitReconnect) {
    const keyspaces = {};
    const result = await this.cc.query(_selectAllKeyspacesV1, waitReconnect);
    for (let i = 0; i < result.rows.length; i++) {
      const row = result.rows[i];
      const ksInfo = this._createKeyspace(row['keyspace_name'], row['durable_writes'], row['strategy_class'], JSON.parse(row['strategy_options'] || null));
      keyspaces[ksInfo.name] = ksInfo;
    }
    return keyspaces;
  }

  async getKeyspace(name) {
    const row = await this._getFirstRow(format(_selectSingleKeyspaceV1, name));
    if (!row) {
      return null;
    }
    return this._createKeyspace(row['keyspace_name'], row['durable_writes'], row['strategy_class'], JSON.parse(row['strategy_options']));
  }

  // eslint-disable-next-line require-await
  async _parseTableOrView(tableInfo, tableRow, columnRows, indexRows, virtual) {
    // All the tableInfo parsing in V1 is sync, it uses a async function because the super class defines one
    // to support other versions.
    let c, name, types;
    const encoder = this.cc.getEncoder();
    const columnsKeyed = {};
    let partitionKeys = [];
    let clusteringKeys = [];
    tableInfo.bloomFilterFalsePositiveChance = tableRow['bloom_filter_fp_chance'];
    tableInfo.caching = tableRow['caching'];
    tableInfo.comment = tableRow['comment'];
    tableInfo.compactionClass = tableRow['compaction_strategy_class'];
    tableInfo.compactionOptions = JSON.parse(tableRow['compaction_strategy_options']);
    tableInfo.compression = JSON.parse(tableRow['compression_parameters']);
    tableInfo.gcGraceSeconds = tableRow['gc_grace_seconds'];
    tableInfo.localReadRepairChance = tableRow['local_read_repair_chance'];
    tableInfo.readRepairChance = tableRow['read_repair_chance'];
    tableInfo.populateCacheOnFlush = tableRow['populate_io_cache_on_flush'] || tableInfo.populateCacheOnFlush;
    tableInfo.memtableFlushPeriod = tableRow['memtable_flush_period_in_ms'] || tableInfo.memtableFlushPeriod;
    tableInfo.defaultTtl = tableRow['default_time_to_live'] || tableInfo.defaultTtl;
    tableInfo.speculativeRetry = tableRow['speculative_retry'] || tableInfo.speculativeRetry;
    tableInfo.indexInterval = tableRow['index_interval'] || tableInfo.indexInterval;
    if (typeof tableRow['min_index_interval'] !== 'undefined') {
      //Cassandra 2.1+
      tableInfo.minIndexInterval = tableRow['min_index_interval'] || tableInfo.minIndexInterval;
      tableInfo.maxIndexInterval = tableRow['max_index_interval'] || tableInfo.maxIndexInterval;
    }
    else {
      //set to null
      tableInfo.minIndexInterval = null;
      tableInfo.maxIndexInterval = null;
    }
    if (typeof tableRow['replicate_on_write'] !== 'undefined') {
      //leave the default otherwise
      tableInfo.replicateOnWrite = tableRow['replicate_on_write'];
    }
    tableInfo.columns = [];
    for (let i = 0; i < columnRows.length; i++) {
      const row = columnRows[i];
      const type = encoder.parseFqTypeName(row['validator']);
      c = {
        name: row['column_name'],
        type: type,
        isStatic: false
      };
      tableInfo.columns.push(c);
      columnsKeyed[c.name] = c;
      switch (row['type']) {
        case 'partition_key':
          partitionKeys.push({ c: c, index: (row['component_index'] || 0) });
          break;
        case 'clustering_key':
          clusteringKeys.push({
            c: c,
            index: (row['component_index'] || 0),
            order: c.type.options.reversed ? 'DESC' : 'ASC'
          });
          break;
        case 'static':
          // C* 2.0.6+ supports static columns
          c.isStatic = true;
          break;
      }
    }
    if (partitionKeys.length > 0) {
      tableInfo.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(item => item.c);
      clusteringKeys.sort(utils.propCompare('index'));
      tableInfo.clusteringKeys = clusteringKeys.map(item => item.c);
      tableInfo.clusteringOrder = clusteringKeys.map(item => item.order);
    }
    // In C* 1.2, keys are not stored on the schema_columns table
    const keysStoredInTableRow = (tableInfo.partitionKeys.length === 0);
    if (keysStoredInTableRow && tableRow['key_aliases']) {
      //In C* 1.2, keys are not stored on the schema_columns table
      partitionKeys = JSON.parse(tableRow['key_aliases']);
      types = encoder.parseKeyTypes(tableRow['key_validator']).types;
      for (let i = 0; i < partitionKeys.length; i++) {
        name = partitionKeys[i];
        c = columnsKeyed[name];
        if (!c) {
          c = {
            name: name,
            type: types[i]
          };
          tableInfo.columns.push(c);
        }
        tableInfo.partitionKeys.push(c);
      }
    }
    const comparator = encoder.parseKeyTypes(tableRow['comparator']);
    if (keysStoredInTableRow && tableRow['column_aliases']) {
      clusteringKeys = JSON.parse(tableRow['column_aliases']);
      for (let i = 0; i < clusteringKeys.length; i++) {
        name = clusteringKeys[i];
        c = columnsKeyed[name];
        if (!c) {
          c = {
            name: name,
            type: comparator.types[i]
          };
          tableInfo.columns.push(c);
        }
        tableInfo.clusteringKeys.push(c);
        tableInfo.clusteringOrder.push(c.type.options.reversed ? 'DESC' : 'ASC');
      }
    }
    tableInfo.isCompact = !!tableRow['is_dense'];
    if (!tableInfo.isCompact) {
      //is_dense column does not exist in previous versions of Cassandra
      //also, compact pk, ck and val appear as is_dense false
      // clusteringKeys != comparator types - 1
      // or not composite (comparator)
      tableInfo.isCompact = (
        //clustering keys are not marked as composite
        !comparator.isComposite ||
        //only 1 column not part of the partition or clustering keys
        (!comparator.hasCollections && tableInfo.clusteringKeys.length !== comparator.types.length - 1));
    }
    name = tableRow['value_alias'];
    if (tableInfo.isCompact && name && !columnsKeyed[name]) {
      //additional column in C* 1.2 as value_alias
      c = {
        name: name,
        type: encoder.parseFqTypeName(tableRow['default_validator'])
      };
      tableInfo.columns.push(c);
      columnsKeyed[name] = c;
    }
    tableInfo.columnsByName = columnsKeyed;
    tableInfo.indexes = Index.fromColumnRows(columnRows, tableInfo.columnsByName);
  }

  getMaterializedView(keyspaceName, name, cache) {
    return Promise.reject(new errors.NotSupportedError('Materialized views are not supported on Cassandra versions below 3.0'));
  }

  // eslint-disable-next-line require-await
  async _parseAggregate(row) {
    const encoder = this.cc.getEncoder();
    const aggregate = new Aggregate();
    aggregate.name = row['aggregate_name'];
    aggregate.keyspaceName = row['keyspace_name'];
    aggregate.signature = row['signature'] || utils.emptyArray;
    aggregate.stateFunction = row['state_func'];
    aggregate.finalFunction = row['final_func'];
    aggregate.initConditionRaw = row['initcond'];
    aggregate.argumentTypes = (row['argument_types'] || utils.emptyArray).map(name => encoder.parseFqTypeName(name));
    aggregate.stateType = encoder.parseFqTypeName(row['state_type']);
    const initConditionValue = encoder.decode(aggregate.initConditionRaw, aggregate.stateType);
    if (initConditionValue !== null && typeof initConditionValue !== 'undefined') {
      aggregate.initCondition = initConditionValue.toString();
    }
    aggregate.returnType = encoder.parseFqTypeName(row['return_type']);
    return aggregate;
  }

  // eslint-disable-next-line require-await
  async _parseFunction(row) {
    const encoder = this.cc.getEncoder();
    const func = new SchemaFunction();
    func.name = row['function_name'];
    func.keyspaceName = row['keyspace_name'];
    func.signature = row['signature'] || utils.emptyArray;
    func.argumentNames = row['argument_names'] || utils.emptyArray;
    func.body = row['body'];
    func.calledOnNullInput = row['called_on_null_input'];
    func.language = row['language'];
    func.argumentTypes = (row['argument_types'] || utils.emptyArray).map(name => encoder.parseFqTypeName(name));
    func.returnType = encoder.parseFqTypeName(row['return_type']);
    return func;
  }

  // eslint-disable-next-line require-await
  async _parseUdt(udtInfo, row) {
    const encoder = this.cc.getEncoder();
    const fieldNames = row['field_names'];
    const fieldTypes = row['field_types'];
    const fields = new Array(fieldNames.length);
    for (let i = 0; i < fieldNames.length; i++) {
      fields[i] = {
        name: fieldNames[i],
        type: encoder.parseFqTypeName(fieldTypes[i])
      };
    }
    udtInfo.fields = fields;
    return udtInfo;
  }
}


/**
 * Used to parse schema information for Cassandra versions 3.x and above
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @ignore
 */
class SchemaParserV2 extends SchemaParser {

  /**
   * @param {ClientOptions} options The client options
   * @param {ControlConnection} cc The control connection to be used
   * @param {Function} udtResolver The function to be used to retrieve the udts.
   */
  constructor(options, cc, udtResolver) {
    super(options, cc);
    this.udtResolver = udtResolver;
    this.selectTable = _selectTableV2;
    this.selectColumns = _selectColumnsV2;
    this.selectUdt = _selectUdtV2;
    this.selectAggregates = _selectAggregatesV2;
    this.selectFunctions = _selectFunctionsV2;
    this.selectIndexes = _selectIndexesV2;
  }

  async getKeyspaces(waitReconnect) {
    const keyspaces = {};
    const result = await this.cc.query(_selectAllKeyspacesV2, waitReconnect);
    for (let i = 0; i < result.rows.length; i++) {
      const ksInfo = this._parseKeyspace(result.rows[i]);
      keyspaces[ksInfo.name] = ksInfo;
    }
    return keyspaces;
  }

  async getKeyspace(name) {
    const row = await this._getFirstRow(format(_selectSingleKeyspaceV2, name));
    if (!row) {
      return null;
    }
    return this._parseKeyspace(row);
  }

  async getMaterializedView(keyspaceName, name, cache) {
    let viewInfo = cache && cache[name];
    if (!viewInfo) {
      viewInfo = new MaterializedView(name);
      if (cache) {
        cache[name] = viewInfo;
      }
    }
    if (viewInfo.loaded) {
      return viewInfo;
    }
    if (viewInfo.loading) {
      return promiseUtils.fromEvent(viewInfo, 'load');
    }
    viewInfo.loading = true;
    try {
      const tableRow = await this._getFirstRow(format(_selectMaterializedViewV2, keyspaceName, name));
      if (!tableRow) {
        viewInfo.emit('load', null, null);
        viewInfo.loading = false;
        return null;
      }
      const columnRows = await this._getRows(format(this.selectColumns, keyspaceName, name));
      await this._parseTableOrView(viewInfo, tableRow, columnRows, null, false);
      viewInfo.loaded = true;
      viewInfo.emit('load', null, viewInfo);
      return viewInfo;
    }
    catch (err) {
      viewInfo.emit('load', err);
      throw err;
    }
    finally {
      viewInfo.loading = false;
    }
  }

  _parseKeyspace(row, virtual) {
    const replication = row['replication'];
    let strategy;
    let strategyOptions;
    if (replication) {
      strategy = replication['class'];
      strategyOptions = {};
      for (const key in replication) {
        if (!replication.hasOwnProperty(key) || key === 'class') {
          continue;
        }
        strategyOptions[key] = replication[key];
      }
    }

    const ks = this._createKeyspace(row['keyspace_name'], row['durable_writes'], strategy, strategyOptions, virtual);
    ks.graphEngine = row['graph_engine'];
    return ks;
  }

  async _parseTableOrView(tableInfo, tableRow, columnRows, indexRows, virtual) {
    const encoder = this.cc.getEncoder();
    const columnsKeyed = {};
    const partitionKeys = [];
    const clusteringKeys = [];
    tableInfo.columns = await Promise.all(columnRows.map(async (row) => {
      const type = await encoder.parseTypeName(tableRow['keyspace_name'], row['type'], 0, null, this.udtResolver);
      const c = {
        name: row['column_name'],
        type: type,
        isStatic: false
      };
      columnsKeyed[c.name] = c;
      switch (row['kind']) {
        case 'partition_key':
          partitionKeys.push({ c, index: (row['position'] || 0) });
          break;
        case 'clustering':
          clusteringKeys.push({
            c, index: (row['position'] || 0), order: row['clustering_order'] === 'desc' ? 'DESC' : 'ASC'
          });
          break;
        case 'static':
          c.isStatic = true;
          break;
      }
      return c;
    }));
    tableInfo.columnsByName = columnsKeyed;
    tableInfo.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(item => item.c);
    clusteringKeys.sort(utils.propCompare('index'));
    tableInfo.clusteringKeys = clusteringKeys.map(item => item.c);
    tableInfo.clusteringOrder = clusteringKeys.map(item => item.order);
    if (virtual) {
      // When table is virtual, the only relevant information to parse are the columns
      // as the table itself has no configuration
      tableInfo.virtual = true;
      return;
    }
    const isView = tableInfo instanceof MaterializedView;
    tableInfo.bloomFilterFalsePositiveChance = tableRow['bloom_filter_fp_chance'];
    tableInfo.caching = JSON.stringify(tableRow['caching']);
    tableInfo.comment = tableRow['comment'];
    // Regardless of the encoding options, use always an Object to represent an associative Array
    const compaction = this._asMap(tableRow['compaction']);
    if (compaction) {
      // compactionOptions as an Object<String, String>
      tableInfo.compactionOptions = {};
      tableInfo.compactionClass = compaction.get('class');
      compaction.forEach((value, key) => {
        if (key === 'class') {
          return;
        }
        tableInfo.compactionOptions[key] = compaction.get(key);
      });
    }
    // Convert compression to an Object<String, String>
    tableInfo.compression = this._mapAsObject(tableRow['compression']);
    tableInfo.gcGraceSeconds = tableRow['gc_grace_seconds'];
    tableInfo.localReadRepairChance = tableRow['dclocal_read_repair_chance'];
    tableInfo.readRepairChance = tableRow['read_repair_chance'];
    tableInfo.extensions = this._mapAsObject(tableRow['extensions']);
    tableInfo.crcCheckChance = tableRow['crc_check_chance'];
    tableInfo.memtableFlushPeriod = tableRow['memtable_flush_period_in_ms'] || tableInfo.memtableFlushPeriod;
    tableInfo.defaultTtl = tableRow['default_time_to_live'] || tableInfo.defaultTtl;
    tableInfo.speculativeRetry = tableRow['speculative_retry'] || tableInfo.speculativeRetry;
    tableInfo.minIndexInterval = tableRow['min_index_interval'] || tableInfo.minIndexInterval;
    tableInfo.maxIndexInterval = tableRow['max_index_interval'] || tableInfo.maxIndexInterval;
    tableInfo.nodesync = tableRow['nodesync'] || tableInfo.nodesync;
    if (!isView) {
      const cdc = tableRow['cdc'];
      if (cdc !== undefined) {
        tableInfo.cdc = cdc;
      }
    }
    if (isView) {
      tableInfo.tableName = tableRow['base_table_name'];
      tableInfo.whereClause = tableRow['where_clause'];
      tableInfo.includeAllColumns = tableRow['include_all_columns'];
      return;
    }
    tableInfo.indexes = this._getIndexes(indexRows);
    // flags can be an instance of Array or Set (real or polyfill)
    let flags = tableRow['flags'];
    if (Array.isArray(flags)) {
      flags = new Set(flags);
    }
    const isDense = flags.has('dense');
    const isSuper = flags.has('super');
    const isCompound = flags.has('compound');
    tableInfo.isCompact = isSuper || isDense || !isCompound;
    // Remove the columns related to Thrift
    const isStaticCompact = !isSuper && !isDense && !isCompound;
    if (isStaticCompact) {
      pruneStaticCompactTableColumns(tableInfo);
    }
    else if (isDense) {
      pruneDenseTableColumns(tableInfo);
    }
  }

  _getIndexes(indexRows) {
    if (!indexRows || indexRows.length === 0) {
      return utils.emptyArray;
    }
    return indexRows.map((row) => {
      const options = this._mapAsObject(row['options']);
      return new Index(row['index_name'], options['target'], row['kind'], options);
    });
  }

  async _parseAggregate(row) {
    const encoder = this.cc.getEncoder();
    const aggregate = new Aggregate();
    aggregate.name = row['aggregate_name'];
    aggregate.keyspaceName = row['keyspace_name'];
    aggregate.signature = row['argument_types'] || utils.emptyArray;
    aggregate.stateFunction = row['state_func'];
    aggregate.finalFunction = row['final_func'];
    aggregate.initConditionRaw = row['initcond'];
    aggregate.initCondition = aggregate.initConditionRaw;
    aggregate.deterministic = row['deterministic'] || false;
    aggregate.argumentTypes = await Promise.all(aggregate.signature.map(name => encoder.parseTypeName(row['keyspace_name'], name, 0, null, this.udtResolver)));
    aggregate.stateType = await encoder.parseTypeName(row['keyspace_name'], row['state_type'], 0, null, this.udtResolver);
    aggregate.returnType = await encoder.parseTypeName(row['keyspace_name'], row['return_type'], 0, null, this.udtResolver);
    return aggregate;
  }

  async _parseFunction(row) {
    const encoder = this.cc.getEncoder();
    const func = new SchemaFunction();
    func.name = row['function_name'];
    func.keyspaceName = row['keyspace_name'];
    func.signature = row['argument_types'] || utils.emptyArray;
    func.argumentNames = row['argument_names'] || utils.emptyArray;
    func.body = row['body'];
    func.calledOnNullInput = row['called_on_null_input'];
    func.language = row['language'];
    func.deterministic = row['deterministic'] || false;
    func.monotonic = row['monotonic'] || false;
    func.monotonicOn = row['monotonic_on'] || utils.emptyArray;
    func.argumentTypes = await Promise.all(func.signature.map(name => encoder.parseTypeName(row['keyspace_name'], name, 0, null, this.udtResolver)));
    func.returnType = await encoder.parseTypeName(row['keyspace_name'], row['return_type'], 0, null, this.udtResolver);
    return func;
  }

  async _parseUdt(udtInfo, row) {
    const encoder = this.cc.getEncoder();
    const fieldTypes = row['field_types'];
    const keyspace = row['keyspace_name'];
    udtInfo.fields = await Promise.all(row['field_names'].map(async (name, i) => {
      const type = await encoder.parseTypeName(keyspace, fieldTypes[i], 0, null, this.udtResolver);
      return { name, type };
    }));
    return udtInfo;
  }
}

/**
 * Used to parse schema information for Cassandra versions 4.x and above.
 *
 * This parser similar to [SchemaParserV2] expect it also parses virtual
 * keyspaces.
 * @ignore
 */
class SchemaParserV3 extends SchemaParserV2 {
  /**
   * @param {ClientOptions} options The client options
   * @param {ControlConnection} cc The control connection to be used
   * @param {Function} udtResolver The function to be used to retrieve the udts.
   */
  constructor(options, cc, udtResolver) {
    super(options, cc, udtResolver);
    this.supportsVirtual = true;
  }

  async getKeyspaces(waitReconnect) {
    const keyspaces = {};
    const queries = [
      { query: _selectAllKeyspacesV2, virtual: false },
      { query: _selectAllVirtualKeyspaces, virtual: true }
    ];

    await Promise.all(queries.map(async (q) => {
      let result = null;
      try {
        result = await this.cc.query(q.query, waitReconnect);
      }
      catch (err) {
        if (q.virtual) {
          // Only throw error for non-virtual query as
          // server reporting C* 4.0 may not actually implement
          // virtual tables.
          return;
        }
        throw err;
      }
      for (let i = 0; i < result.rows.length; i++) {
        const ksInfo = this._parseKeyspace(result.rows[i], q.virtual);
        keyspaces[ksInfo.name] = ksInfo;
      }
    }));
    return keyspaces;
  }

  async getKeyspace(name) {
    const ks = await this._getKeyspace(_selectSingleKeyspaceV2, name, false);
    if (!ks) {
      // if not found, attempt to retrieve as virtual keyspace.
      return this._getKeyspace(_selectSingleVirtualKeyspace, name, true);
    }
    return ks;
  }

  async _getKeyspace(query, name, virtual) {
    try {
      const row = await this._getFirstRow(format(query, name));

      if (!row) {
        return null;
      }

      return this._parseKeyspace(row, virtual);
    }
    catch (err) {
      if (virtual) {
        // only throw error for non-virtual query as
        // server reporting C* 4.0 may not actually implement
        // virtual tables.
        return null;
      }
      throw err;
    }
  }
}

/**
 * Upon migration from thrift to CQL, we internally create a pair of surrogate clustering/regular columns
 * for compact static tables. These columns shouldn't be exposed to the user but are currently returned by C*.
 * We also need to remove the static keyword for all other columns in the table.
 * @param {module:metadata~TableMetadata} tableInfo
*/
function pruneStaticCompactTableColumns(tableInfo) {
  let i;
  let c;
  //remove "column1 text" clustering column
  for (i = 0; i < tableInfo.clusteringKeys.length; i++) {
    c = tableInfo.clusteringKeys[i];
    const index = tableInfo.columns.indexOf(c);
    tableInfo.columns.splice(index, 1);
    delete tableInfo.columnsByName[c.name];
  }
  tableInfo.clusteringKeys = utils.emptyArray;
  tableInfo.clusteringOrder = utils.emptyArray;
  //remove regular columns and set the static columns to non-static
  i = tableInfo.columns.length;
  while (i--) {
    c = tableInfo.columns[i];
    if (!c.isStatic && tableInfo.partitionKeys.indexOf(c) === -1) {
      // remove "value blob" regular column
      tableInfo.columns.splice(i, 1);
      delete tableInfo.columnsByName[c.name];
      continue;
    }
    c.isStatic = false;
  }
}

/**
 * Upon migration from thrift to CQL, we internally create a surrogate column "value" of type custom.
 * This column shouldn't be exposed to the user but is currently returned by C*.
 * @param {module:metadata~TableMetadata} tableInfo
 */
function pruneDenseTableColumns(tableInfo) {
  let i = tableInfo.columns.length;
  while (i--) {
    const c = tableInfo.columns[i];
    if (!c.isStatic && c.type.code === types.dataTypes.custom && c.type.info === 'empty') {
      // remove "value blob" regular column
      tableInfo.columns.splice(i, 1);
      delete tableInfo.columnsByName[c.name];
      continue;
    }
    c.isStatic = false;
  }
}

function getTokenToReplicaMapper(strategy, strategyOptions) {
  if (/SimpleStrategy$/.test(strategy)) {
    const rf = parseInt(strategyOptions['replication_factor'], 10);
    if (rf > 1) {
      return getTokenToReplicaSimpleMapper(rf);
    }
  }
  if (/NetworkTopologyStrategy$/.test(strategy)) {
    return getTokenToReplicaNetworkMapper(strategyOptions);
  }
  //default, wrap in an Array
  return (function noStrategy(tokenizer, ring, primaryReplicas) {
    const replicas = {};
    for (const key in primaryReplicas) {
      if (!primaryReplicas.hasOwnProperty(key)) {
        continue;
      }
      replicas[key] = [primaryReplicas[key]];
    }
    return replicas;
  });
}

/**
 * @param {Number} replicationFactor
 * @returns {function}
 */
function getTokenToReplicaSimpleMapper(replicationFactor) {
  return (function tokenSimpleStrategy(tokenizer, ringTokensAsStrings, primaryReplicas) {
    const ringLength = ringTokensAsStrings.length;
    const rf = Math.min(replicationFactor, ringLength);
    const replicas = {};
    for (let i = 0; i < ringLength; i++) {
      const key = ringTokensAsStrings[i];
      const tokenReplicas = [primaryReplicas[key]];
      for (let j = 1; j < ringLength && tokenReplicas.length < rf; j++) {
        let nextReplicaIndex = i + j;
        if (nextReplicaIndex >= ringLength) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % ringLength;
        }
        const nextReplica = primaryReplicas[ringTokensAsStrings[nextReplicaIndex]];
        // In the case of vnodes, consecutive sections of the ring can be assigned to the same host.
        if (tokenReplicas.indexOf(nextReplica) === -1) {
          tokenReplicas.push(nextReplica);
        }
      }
      replicas[key] = tokenReplicas;
    }
    return replicas;
  });
}

/**
 * @param {Object} replicationFactors
 * @returns {Function}
 * @private
 */
function getTokenToReplicaNetworkMapper(replicationFactors) {
  //                A(DC1)
  //
  //           H         B(DC2)
  //                |
  //      G       --+--       C(DC1)
  //                |
  //           F         D(DC2)
  //
  //                E(DC1)
  return (function tokenNetworkStrategy(tokenizer, ringTokensAsStrings, primaryReplicas, datacenters) {
    const replicas = {};
    const ringLength = ringTokensAsStrings.length;

    for (let i = 0; i < ringLength; i++) {
      const key = ringTokensAsStrings[i];
      const tokenReplicas = [];
      const replicasByDc = {};
      const racksPlaced = {};
      const skippedHosts = [];
      for (let j = 0; j < ringLength; j++) {
        let nextReplicaIndex = i + j;
        if (nextReplicaIndex >= ringLength) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % ringLength;
        }
        const h = primaryReplicas[ringTokensAsStrings[nextReplicaIndex]];
        // In the case of vnodes, consecutive sections of the ring can be assigned to the same host.
        if (tokenReplicas.indexOf(h) !== -1) {
          continue;
        }
        const dc = h.datacenter;
        //Check if the next replica belongs to one of the targeted dcs
        let dcRf = parseInt(replicationFactors[dc], 10);
        if (!dcRf) {
          continue;
        }
        dcRf = Math.min(dcRf, datacenters[dc].hostLength);
        let dcReplicas = replicasByDc[dc] || 0;
        //Amount of replicas per dc is greater than rf or the amount of host in the datacenter
        if (dcReplicas >= dcRf) {
          continue;
        }
        let racksPlacedInDc = racksPlaced[dc];
        if (!racksPlacedInDc) {
          racksPlacedInDc = racksPlaced[dc] = new utils.HashSet();
        }
        if (h.rack &&
            racksPlacedInDc.contains(h.rack) &&
            racksPlacedInDc.length < datacenters[dc].racks.length) {
          // We already selected a replica for this rack
          // Skip until replicas in other racks are added
          if (skippedHosts.length < dcRf - dcReplicas) {
            skippedHosts.push(h);
          }
          continue;
        }
        replicasByDc[h.datacenter] = ++dcReplicas;
        tokenReplicas.push(h);
        if (h.rack && racksPlacedInDc.add(h.rack) && racksPlacedInDc.length === datacenters[dc].racks.length) {
          // We finished placing all replicas for all racks in this dc
          // Add the skipped hosts
          replicasByDc[dc] += addSkippedHosts(dcRf, dcReplicas, tokenReplicas, skippedHosts);
        }
        if (isDoneForToken(replicationFactors, datacenters, replicasByDc)) {
          break;
        }
      }
      replicas[key] = tokenReplicas;
    }
    return replicas;
  });
}

/**
 * @returns {Number} The number of skipped hosts added.
 */
function addSkippedHosts(dcRf, dcReplicas, tokenReplicas, skippedHosts) {
  let i;
  for (i = 0; i < dcRf - dcReplicas && i < skippedHosts.length; i++) {
    tokenReplicas.push(skippedHosts[i]);
  }
  return i;
}

function isDoneForToken(replicationFactors, datacenters, replicasByDc) {
  const keys = Object.keys(replicationFactors);
  for (let i = 0; i < keys.length; i++) {
    const dcName = keys[i];
    const dc = datacenters[dcName];
    if (!dc) {
      // A DC is included in the RF but the DC does not exist in the topology
      continue;
    }
    const rf = Math.min(parseInt(replicationFactors[dcName], 10), dc.hostLength);
    if (rf > 0 && (!replicasByDc[dcName] || replicasByDc[dcName] < rf)) {
      return false;
    }
  }
  return true;
}

/**
 * Creates a new instance if the currentInstance is not valid for the
 * provided Cassandra version
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @param {Array.<Number>} [version] The cassandra version
 * @param {SchemaParser} [currentInstance] The current instance
 * @returns {SchemaParser}
 */
function getByVersion(options, cc, udtResolver, version, currentInstance) {
  let parserConstructor = SchemaParserV1;
  if (version && version[0] === 3) {
    parserConstructor = SchemaParserV2;
  } else if (version && version[0] >= 4) {
    parserConstructor = SchemaParserV3;
  }
  if (!currentInstance || !(currentInstance instanceof parserConstructor)){
    return new parserConstructor(options, cc, udtResolver);
  }
  return currentInstance;
}

exports.getByVersion = getByVersion;
exports.isDoneForToken = isDoneForToken;