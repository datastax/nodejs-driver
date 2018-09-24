"use strict";
const util = require('util');
const events = require('events');
const types = require('../types');
const utils = require('../utils');
const errors = require('../errors');
const TableMetadata = require('./table-metadata');
const Aggregate = require('./aggregate');
const SchemaFunction = require('./schema-function');
const Index = require('./schema-index');
const MaterializedView = require('./materialized-view');
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
function SchemaParser(options, cc) {
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
 * @returns {{name, durableWrites, strategy, strategyOptions, tokenToReplica, udts, tables, functions, aggregates}|null}
 * @protected
 */
SchemaParser.prototype._createKeyspace = function (name, durableWrites, strategy, strategyOptions, virtual) {
  const ksInfo = {
    name: name,
    durableWrites: durableWrites,
    strategy: strategy,
    strategyOptions: strategyOptions,
    tokenToReplica: null,
    virtual: virtual === true,
    udts: {},
    tables: {},
    functions: {},
    aggregates: {},
    views: {}
  };
  ksInfo.tokenToReplica = getTokenToReplicaMapper(strategy, strategyOptions);
  return ksInfo;
};

/**
 * @abstract
 * @param {String} name
 * @param {Function} callback
 */
SchemaParser.prototype.getKeyspace = function (name, callback) {
};

/**
 * @abstract
 * @param {Boolean} waitReconnect
 * @param {Function} callback
 */
SchemaParser.prototype.getKeyspaces = function (waitReconnect, callback) {
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Object} cache
 * @param {Boolean} virtual
 * @param {Function} callback
 */
SchemaParser.prototype.getTable = function (keyspaceName, name, cache, virtual, callback) {
  let tableInfo = cache && cache[name];
  if (!tableInfo) {
    tableInfo = new TableMetadata(name);
    if (cache) {
      cache[name] = tableInfo;
    }
  }
  if (tableInfo.loaded) {
    return callback(null, tableInfo);
  }
  tableInfo.once('load', callback);
  if (tableInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  // its not cached and not being retrieved
  tableInfo.loading = true;
  let tableRow, columnRows, indexRows;
  const self = this;
  let virtualTable = virtual;
  utils.series([
    function getTableRow(next) {
      const selectTable = virtualTable ? _selectVirtualTable : self.selectTable; 
      const query = util.format(selectTable, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        tableRow = response.rows[0];
        next();
      });
    },
    function getVirtualTableRow(next) {
      // if we weren't sure if table was virtual or not, query virtual schema.
      if (!tableRow && self.supportsVirtual && virtualTable === undefined) {
        const query = util.format(_selectVirtualTable, keyspaceName, name);
        self.cc.query(query, function (err, response) {
          if (err) {
            // we can't error here as we can't be sure if the node
            // supports virtual tables, in this case it is adequate
            // to act as if there was no matching table.
            return next();
          }
          tableRow = response.rows[0];
          // if we got a result, this is a virtual table
          if (tableRow) {
            virtualTable = true;
          }
          next();
        });
      } else {
        return next();
      }
    },
    function getColumnRows (next) {
      if (!tableRow) {
        return next();
      }
      const selectColumns = virtualTable ? _selectVirtualColumns : self.selectColumns;
      const query = util.format(selectColumns, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        columnRows = response.rows;
        next();
      });
    },
    function getIndexes(next) {
      if (!tableRow || !self.selectIndexes || virtualTable) {
        //either the table does not exists or it does not support indexes schema table
        return next();
      }
      const query = util.format(self.selectIndexes, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        indexRows = response.rows;
        next();
      });
    }
  ], function afterQuery (err) {
    if (err || !tableRow) {
      tableInfo.loading = false;
      return tableInfo.emit('load', err, null);
    }
    self._parseTableOrView(tableInfo, tableRow, columnRows, indexRows, virtualTable, function (err) {
      tableInfo.loading = false;
      tableInfo.loaded = !err;
      tableInfo.emit('load', err, tableInfo);
    });
  });
};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Object} cache
 * @param {Function} callback
 */
SchemaParser.prototype.getUdt = function (keyspaceName, name, cache, callback) {
  let udtInfo = cache && cache[name];
  if (!udtInfo) {
    udtInfo = new events.EventEmitter();
    if (cache) {
      cache[name] = udtInfo;
    }
    udtInfo.setMaxListeners(0);
    udtInfo.loading = false;
    udtInfo.name = name;
    udtInfo.fields = null;
  }
  if (udtInfo.fields) {
    return callback(null, udtInfo);
  }
  udtInfo.once('load', callback);
  if (udtInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  udtInfo.loading = true;
  //it is not cached, try to query for it
  const query = util.format(this.selectUdt, keyspaceName, name);
  const self = this;
  this.cc.query(query, function (err, response) {
    if (err) {
      return udtInfo.emit('load', err);
    }
    const row = response.rows[0];
    if (!row) {
      udtInfo.loading = false;
      return udtInfo.emit('load', null, null);
    }
    self._parseUdt(udtInfo, row, function (err) {
      udtInfo.loading = false;
      if (err) {
        return udtInfo.emit('load', err);
      }
      return udtInfo.emit('load', null, udtInfo);
    });
  });
};

/**
 * Parses the udt information from the row
 * @param udtInfo
 * @param {Row} row
 * @param {Function} callback Callback to be invoked with the err and {{fields: Array}}|null
 * @abstract
 */
SchemaParser.prototype._parseUdt = function (udtInfo, row, callback) {
};

/**
 * Builds the metadata based on the table and column rows
 * @abstract
 * @param {module:metadata~TableMetadata} tableInfo
 * @param {Row} tableRow
 * @param {Array.<Row>} columnRows
 * @param {Array.<Row>} indexRows
 * @param {Boolean} virtual
 * @param {Function} callback
 * @throws {Error}
 */
SchemaParser.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, virtual, callback) {
};


/**
 * @abstract
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Object} cache
 * @param {Function} callback
 */
SchemaParser.prototype.getMaterializedView = function (keyspaceName, name, cache, callback) {

};

/**
 * @param {String} keyspaceName
 * @param {String} name
 * @param {Boolean} aggregate
 * @param {Object} cache
 * @param {Function} callback
 */
SchemaParser.prototype.getFunctions = function (keyspaceName, name, aggregate, cache, callback) {
  /** @type {String} */
  let query = this.selectFunctions;
  let parser = this._parseFunction.bind(this);
  if (aggregate) {
    query = this.selectAggregates;
    parser = this._parseAggregate.bind(this);
  }
  //if not already loaded
  //get all functions with that name
  //cache it by name and, within name, by signature
  let functionsInfo = cache && cache[name];
  if (!functionsInfo) {
    functionsInfo = new events.EventEmitter();
    if (cache) {
      cache[name] = functionsInfo;
    }
    functionsInfo.setMaxListeners(0);
  }
  if (functionsInfo.values) {
    return callback(null, functionsInfo.values);
  }
  functionsInfo.once('load', callback);
  if (functionsInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  functionsInfo.loading = true;
  //it is not cached, try to query for it
  query = util.format(query, keyspaceName, name);
  this.cc.query(query, function (err, response) {
    functionsInfo.loading = false;
    if (err || response.rows.length === 0) {
      return functionsInfo.emit('load', err, null);
    }
    if (response.rows.length > 0) {
      functionsInfo.values = {};
    }
    utils.each(response.rows, function (row, next) {
      parser(row, function (err, func) {
        if (err) {
          return next(err);
        }
        functionsInfo.values['(' + func.signature.join(',') + ')'] = func;
        next();
      });
    }, function (err) {
      if (err) {
        return functionsInfo.emit('load', err);
      }
      functionsInfo.emit('load', null, functionsInfo.values);
    });
  });
};

/**
 * @abstract
 * @param {Row} row
 * @param {Function} callback
 */
SchemaParser.prototype._parseAggregate = function (row, callback) {
};

/**
 * @abstract
 * @param {Row} row
 * @param {Function} callback
 */
SchemaParser.prototype._parseFunction = function (row, callback) {
};

/** @returns {Map} */
SchemaParser.prototype._asMap = function (obj) {
  if (!obj) {
    return new Map();
  }

  if (this.encodingOptions.map && obj instanceof this.encodingOptions.map) {
    // Its already a Map or a polyfill of a Map
    return obj;
  }

  return new Map(Object.keys(obj).map(k => [ k, obj[k]]));
};

SchemaParser.prototype._mapAsObject = function (map) {
  if (!map) {
    return map;
  }

  if (this.encodingOptions.map && map instanceof this.encodingOptions.map) {
    const result = {};
    map.forEach((value, key) => result[key] = value);
    return result;
  }

  return map;
};

/**
 * Used to parse schema information for Cassandra versions 1.2.x, and 2.x
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc
 * @constructor
 * @ignore
 */
function SchemaParserV1(options, cc) {
  SchemaParser.call(this, options, cc);
  this.selectTable = _selectTableV1;
  this.selectColumns = _selectColumnsV1;
  this.selectUdt = _selectUdtV1;
  this.selectAggregates = _selectAggregatesV1;
  this.selectFunctions = _selectFunctionsV1;
}

util.inherits(SchemaParserV1, SchemaParser);

/** @override */
SchemaParserV1.prototype.getKeyspaces = function (waitReconnect, callback) {
  const self = this;
  const keyspaces = {};
  this.cc.query(_selectAllKeyspacesV1, waitReconnect, function (err, result) {
    if (err) {
      return callback(err);
    }
    for (let i = 0; i < result.rows.length; i++) {
      const row = result.rows[i];
      const ksInfo = self._createKeyspace(
        row['keyspace_name'],
        row['durable_writes'],
        row['strategy_class'],
        JSON.parse(row['strategy_options'] || null));
      keyspaces[ksInfo.name] = ksInfo;
    }
    callback(null, keyspaces);
  });
};

/** @override */
SchemaParserV1.prototype.getKeyspace = function (name, callback) {
  const self = this;
  this.cc.query(util.format(_selectSingleKeyspaceV1, name), function (err, result) {
    if (err) {
      return callback(err);
    }
    const row = result.rows[0];
    if (!row) {
      return callback(null, null);
    }
    callback(null, self._createKeyspace(
      row['keyspace_name'],
      row['durable_writes'],
      row['strategy_class'],
      JSON.parse(row['strategy_options'])));
  });
};

/** @override */
SchemaParserV1.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, virtual, callback) {
  let i, c, name, types;
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
  try {
    (function parseColumns() {
      //function context
      for (i = 0; i < columnRows.length; i++) {
        const row = columnRows[i];
        const type = encoder.parseFqTypeName(row['validator']);
        c = {
          name: row['column_name'],
          type: type
        };
        tableInfo.columns.push(c);
        columnsKeyed[c.name] = c;
        switch (row['type']) {
          case 'partition_key':
            partitionKeys.push({c: c, index: (row['component_index'] || 0)});
            break;
          case 'clustering_key':
            clusteringKeys.push({
              c: c,
              index: (row['component_index'] || 0),
              order: c.type.options.reversed ? 'DESC' : 'ASC'
            });
            break;
        }
      }
    })();
    if (partitionKeys.length > 0) {
      tableInfo.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(function (item) {
        return item.c;
      });
      clusteringKeys.sort(utils.propCompare('index'));
      tableInfo.clusteringKeys = clusteringKeys.map(function (item) {
        return item.c;
      });
      tableInfo.clusteringOrder = clusteringKeys.map(function (item) {
        return item.order;
      });
    }
    //In C* 1.2, keys are not stored on the schema_columns table
    const keysStoredInTableRow = (tableInfo.partitionKeys.length === 0);
    if (keysStoredInTableRow && tableRow['key_aliases']) {
      //In C* 1.2, keys are not stored on the schema_columns table
      partitionKeys = JSON.parse(tableRow['key_aliases']);
      types = encoder.parseKeyTypes(tableRow['key_validator']).types;
      for (i = 0; i < partitionKeys.length; i++) {
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
      for (i = 0; i < clusteringKeys.length; i++) {
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
        (!comparator.hasCollections && tableInfo.clusteringKeys.length !== comparator.types.length - 1)
      );
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
  catch (err) {
    return callback(err);
  }
  //All the tableInfo parsing in V1 is sync, it uses a callback because the super defines one
  //to support other versions.
  callback();
};

/** @override */
SchemaParserV1.prototype.getMaterializedView = function (keyspaceName, name, cache, callback) {
  callback(new errors.NotSupportedError('Materialized views are not supported on Cassandra versions below 3.0'));
};

/** @override */
SchemaParserV1.prototype._parseAggregate = function (row, callback) {
  const encoder = this.cc.getEncoder();
  const aggregate = new Aggregate();
  aggregate.name = row['aggregate_name'];
  aggregate.keyspaceName = row['keyspace_name'];
  aggregate.signature = row['signature'] || utils.emptyArray;
  aggregate.stateFunction = row['state_func'];
  aggregate.finalFunction = row['final_func'];
  aggregate.initConditionRaw = row['initcond'];
  try {
    aggregate.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
      return encoder.parseFqTypeName(name);
    });
    aggregate.stateType = encoder.parseFqTypeName(row['state_type']);
    const initConditionValue = encoder.decode(aggregate.initConditionRaw, aggregate.stateType);
    if (initConditionValue !== null && typeof initConditionValue !== 'undefined') {
      aggregate.initCondition = initConditionValue.toString();
    }
    aggregate.returnType = encoder.parseFqTypeName(row['return_type']);
  }
  catch (err) {
    return callback(err);
  }
  callback(null, aggregate);
};

/** @override */
SchemaParserV1.prototype._parseFunction = function (row, callback) {
  const encoder = this.cc.getEncoder();
  const func = new SchemaFunction();
  func.name = row['function_name'];
  func.keyspaceName = row['keyspace_name'];
  func.signature = row['signature'] || utils.emptyArray;
  func.argumentNames = row['argument_names'] || utils.emptyArray;
  func.body = row['body'];
  func.calledOnNullInput = row['called_on_null_input'];
  func.language = row['language'];
  try {
    func.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
      return encoder.parseFqTypeName(name);
    });
    func.returnType = encoder.parseFqTypeName(row['return_type']);
  }
  catch (err) {
    return callback(err);
  }
  callback(null, func);
};

/** @override */
SchemaParserV1.prototype._parseUdt = function (udtInfo, row, callback) {
  const encoder = this.cc.getEncoder();
  const fieldNames = row['field_names'];
  const fieldTypes = row['field_types'];
  const fields = new Array(fieldNames.length);
  try {
    for (let i = 0; i < fieldNames.length; i++) {
      fields[i] = {
        name: fieldNames[i],
        type: encoder.parseFqTypeName(fieldTypes[i])
      };
    }
  }
  catch (err) {
    return callback(err);
  }
  udtInfo.fields = fields;
  callback(null, udtInfo);
};

/**
 * Used to parse schema information for Cassandra versions 3.x and above
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @constructor
 * @ignore
 */
function SchemaParserV2(options, cc, udtResolver) {
  SchemaParser.call(this, options, cc);
  this.udtResolver = udtResolver;
  this.selectTable = _selectTableV2;
  this.selectColumns = _selectColumnsV2;
  this.selectUdt = _selectUdtV2;
  this.selectAggregates = _selectAggregatesV2;
  this.selectFunctions = _selectFunctionsV2;
  this.selectIndexes = _selectIndexesV2;
}

util.inherits(SchemaParserV2, SchemaParser);

/** @override */
SchemaParserV2.prototype.getKeyspaces = function (waitReconnect, callback) {
  const self = this;
  const keyspaces = {};
  this.cc.query(_selectAllKeyspacesV2, waitReconnect, function (err, result) {
    if (err) {
      return callback(err);
    }
    for (let i = 0; i < result.rows.length; i++) {
      const ksInfo = self._parseKeyspace(result.rows[i]);
      keyspaces[ksInfo.name] = ksInfo;
    }
    callback(null, keyspaces);
  });
};

/** @override */
SchemaParserV2.prototype.getKeyspace = function (name, callback) {
  const self = this;
  this.cc.query(util.format(_selectSingleKeyspaceV2, name), function (err, result) {
    if (err) {
      return callback(err);
    }
    const row = result.rows[0];
    if (!row) {
      return callback(null, null);
    }
    callback(null, self._parseKeyspace(row));
  });
};

/** @override */
SchemaParserV2.prototype.getMaterializedView = function (keyspaceName, name, cache, callback) {
  let viewInfo = cache && cache[name];
  if (!viewInfo) {
    viewInfo = new MaterializedView(name);
    if (cache) {
      cache[name] = viewInfo;
    }
  }
  if (viewInfo.loaded) {
    return callback(null, viewInfo);
  }
  viewInfo.once('load', callback);
  if (viewInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  viewInfo.loading = true;
  let tableRow, columnRows;
  //it is not cached, try to query for it
  const self = this;
  utils.series([
    function getTableRow(next) {
      const query = util.format(_selectMaterializedViewV2, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        tableRow = response.rows[0];
        next();
      });
    },
    function getColumnRows (next) {
      if (!tableRow) {
        return next();
      }
      const query = util.format(self.selectColumns, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        columnRows = response.rows;
        next();
      });
    }
  ], function afterQuery (err) {
    viewInfo.loading = false;
    if (err || !tableRow) {
      return viewInfo.emit('load', err, null);
    }
    self._parseTableOrView(viewInfo, tableRow, columnRows, null, false, function (err) {
      viewInfo.loading = false;
      viewInfo.loaded = !err;
      viewInfo.emit('load', err, viewInfo);
    });
  });

};

SchemaParserV2.prototype._parseKeyspace = function (row, virtual) {
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
  return this._createKeyspace(
    row['keyspace_name'],
    row['durable_writes'],
    strategy,
    strategyOptions,
    virtual);
};

/** @override */
SchemaParserV2.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, virtual, callback) {
  const encoder = this.cc.getEncoder();
  const columnsKeyed = {};
  const partitionKeys = [];
  const clusteringKeys = [];
  const self = this;

  // maps column rows to columnInfo and also populates columnsKeyed
  const columnRowMapper = (row, next) => {
    encoder.parseTypeName(tableRow['keyspace_name'], row['type'], 0, null, self.udtResolver, function (err, type) {
      if (err) {
        return next(err);
      }
      const c = {
        name: row['column_name'],
        type: type,
        isStatic: false
      };
      columnsKeyed[c.name] = c;
      switch (row['kind']) {
        case 'partition_key':
          partitionKeys.push({ c: c, index: (row['position'] || 0)});
          break;
        case 'clustering':
          clusteringKeys.push({ c: c, index: (row['position'] || 0), order: row['clustering_order'] === 'desc' ? 'DESC' : 'ASC'});
          break;
        case 'static':
          c.isStatic = true;
          break;
      }
      next(null, c);
    });
  };

  // is table is virtual, the only relevant information to parse is the columns as the table itself has no configuration.
  if (virtual) {
    tableInfo.virtual = true;
    utils.map(columnRows, columnRowMapper, function (err, columns) {
      if (err) {
        return callback(err);
      }
      tableInfo.columns = columns;
      tableInfo.columnsByName = columnsKeyed;
      tableInfo.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(function (item) {
        return item.c;
      });
      clusteringKeys.sort(utils.propCompare('index'));
      tableInfo.clusteringKeys = clusteringKeys.map(function (item) {
        return item.c;
      });
      tableInfo.clusteringOrder = clusteringKeys.map(function (item) {
        return item.order;
      });
      callback();
    });
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
  if (!isView) {
    const cdc = tableRow['cdc'];
    if (cdc !== undefined) {
      tableInfo.cdc = cdc;
    }
  }

  // Map all columns asynchronously
  utils.map(columnRows, columnRowMapper, (err, columns) => {
    if (err) {
      return callback(err);
    }
    tableInfo.columns = columns;
    tableInfo.columnsByName = columnsKeyed;
    tableInfo.partitionKeys = partitionKeys.sort(utils.propCompare('index')).map(function (item) {
      return item.c;
    });
    clusteringKeys.sort(utils.propCompare('index'));
    tableInfo.clusteringKeys = clusteringKeys.map(function (item) {
      return item.c;
    });
    tableInfo.clusteringOrder = clusteringKeys.map(function (item) {
      return item.order;
    });

    if (isView) {
      tableInfo.tableName = tableRow['base_table_name'];
      tableInfo.whereClause = tableRow['where_clause'];
      tableInfo.includeAllColumns = tableRow['include_all_columns'];
      return callback();
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
    //remove the columns related to Thrift
    const isStaticCompact = !isSuper && !isDense && !isCompound;
    if(isStaticCompact) {
      pruneStaticCompactTableColumns(tableInfo);
    }
    else if (isDense) {
      pruneDenseTableColumns(tableInfo);
    }
    callback();
  });
};

SchemaParserV2.prototype._getIndexes = function (indexRows) {
  if (!indexRows || indexRows.length === 0) {
    return utils.emptyArray;
  }

  return indexRows.map((row) => {
    const options = this._mapAsObject(row['options']);
    return new Index(row['index_name'], options['target'], row['kind'], options);
  });
};

/** @override */
SchemaParserV2.prototype._parseAggregate = function (row, callback) {
  const encoder = this.cc.getEncoder();
  const aggregate = new Aggregate();
  aggregate.name = row['aggregate_name'];
  aggregate.keyspaceName = row['keyspace_name'];
  aggregate.signature = row['argument_types'] || utils.emptyArray;
  aggregate.stateFunction = row['state_func'];
  aggregate.finalFunction = row['final_func'];
  aggregate.initConditionRaw = row['initcond'];
  aggregate.initCondition = aggregate.initConditionRaw;
  const self = this;
  utils.series([
    function parseArguments(next) {
      utils.map(row['argument_types'] || utils.emptyArray, function (name, mapNext) {
        encoder.parseTypeName(row['keyspace_name'], name, 0, null, self.udtResolver, mapNext);
      }, function (err, result) {
        aggregate.argumentTypes = result;
        next(err);
      });
    },
    function parseStateType(next) {
      encoder.parseTypeName(row['keyspace_name'], row['state_type'], 0, null, self.udtResolver, function (err, type) {
        aggregate.stateType = type;
        next(err);
      });
    },
    function parseReturnType(next) {
      encoder.parseTypeName(row['keyspace_name'], row['return_type'], 0, null, self.udtResolver, function (err, type) {
        aggregate.returnType = type;
        next(err);
      });
    }
  ], function (err) {
    if (err) {
      return callback(err);
    }
    callback(null, aggregate);
  });
};

/** @override */
SchemaParserV2.prototype._parseFunction = function (row, callback) {
  const encoder = this.cc.getEncoder();
  const func = new SchemaFunction();
  func.name = row['function_name'];
  func.keyspaceName = row['keyspace_name'];
  func.signature = row['argument_types'] || utils.emptyArray;
  func.argumentNames = row['argument_names'] || utils.emptyArray;
  func.body = row['body'];
  func.calledOnNullInput = row['called_on_null_input'];
  func.language = row['language'];
  const self = this;
  utils.series([
    function parseArguments(next) {
      utils.map(row['argument_types'] || utils.emptyArray, function (name, mapNext) {
        encoder.parseTypeName(row['keyspace_name'], name, 0, null, self.udtResolver, mapNext);
      }, function (err, result) {
        func.argumentTypes = result;
        next(err);
      });
    },
    function parseReturnType(next) {
      encoder.parseTypeName(row['keyspace_name'], row['return_type'], 0, null, self.udtResolver, function (err, type) {
        func.returnType = type;
        next(err);
      });
    }
  ], function (err) {
    if (err) {
      return callback(err);
    }
    callback(null, func);
  });
};

/** @override */
SchemaParserV2.prototype._parseUdt = function (udtInfo, row, callback) {
  const encoder = this.cc.getEncoder();
  const fieldTypes = row['field_types'];
  const keyspace = row['keyspace_name'];
  const fields = new Array(fieldTypes.length);
  const self = this;
  utils.forEachOf(row['field_names'], function (name, i, next) {
    encoder.parseTypeName(keyspace, fieldTypes[i], 0, null, self.udtResolver, function (err, type) {
      if (err) {
        return next(err);
      }
      fields[i] = {
        name: name,
        type: type
      };
      next();
    });
  }, function (err) {
    if (err) {
      return callback(err);
    }
    udtInfo.fields = fields;
    callback(null, udtInfo);
  });
};

/**
 * Used to parse schema information for Cassandra versions 4.x and above.
 *
 * This parser similar to [SchemaParserV2] expect it also parses virtual
 * keyspaces.
 *
 * @param {ClientOptions} options The client options
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @constructor
 * @ignore
 */
function SchemaParserV3(options, cc, udtResolver) {
  SchemaParserV2.call(this, options, cc, udtResolver);
  this.supportsVirtual = true;
}

util.inherits(SchemaParserV3, SchemaParserV2);

/** @override */
SchemaParserV3.prototype.getKeyspaces = function (waitReconnect, callback) {
  const self = this;
  const keyspaces = {};
  const queries = [
    { query: _selectAllKeyspacesV2, virtual: false },
    { query: _selectAllVirtualKeyspaces, virtual: true }
  ];
  utils.each(queries, (q, cb) => {
    self.cc.query(q.query, waitReconnect, (err, result) => {
      if (err) {
        // only callback in error for non-virtual query as
        // server reporting C* 4.0 may not actually implement
        // virtual tables.
        if (!q.virtual) {
          return cb(err);
        }
        return cb();
      }
      for (let i = 0; i < result.rows.length; i++) {
        const ksInfo = self._parseKeyspace(result.rows[i], q.virtual);
        keyspaces[ksInfo.name] = ksInfo;
      }
      cb();
    });
  }, (err) => {
    callback(err, keyspaces);
  });
};

/** @override */
SchemaParserV3.prototype.getKeyspace = function (name, callback) {
  this._getKeyspace(_selectSingleKeyspaceV2, name, false, (err, ks) => {
    if (err) {
      return callback(err);
    }
    if (!ks) {
      // if not found, attempt to retrieve as virtual keyspace.
      return this._getKeyspace(_selectSingleVirtualKeyspace, name, true, callback);
    }
    return callback(null, ks);
  });
};

SchemaParserV3.prototype._getKeyspace = function (query, name, virtual, callback) {
  this.cc.query(util.format(query, name), (err, result) => {
    if (err) {
      // only callback in error for non-virtual query as
      // server reporting C* 4.0 may not actually implement
      // virtual tables.
      if (!virtual) {
        return callback(err);
      }
      return callback(null, null);
    }
    const row = result.rows[0];
    if (!row) {
      return callback(null, null);
    }
    callback(null, this._parseKeyspace(row, virtual));
  });
};

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
