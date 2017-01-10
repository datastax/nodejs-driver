"use strict";
var util = require('util');
var events = require('events');
var types = require('../types');
var utils = require('../utils');
var errors = require('../errors');
var TableMetadata = require('./table-metadata');
var Aggregate = require('./aggregate');
var SchemaFunction = require('./schema-function');
var Index = require('./schema-index');
var MaterializedView = require('./materialized-view');
/**
 * @module metadata/schemaParser
 * @ignore
 */

var _selectAllKeyspacesV1 = "SELECT * FROM system.schema_keyspaces";
var _selectSingleKeyspaceV1 = "SELECT * FROM system.schema_keyspaces where keyspace_name = '%s'";
var _selectAllKeyspacesV2 = "SELECT * FROM system_schema.keyspaces";
var _selectSingleKeyspaceV2 = "SELECT * FROM system_schema.keyspaces where keyspace_name = '%s'";
var _selectTableV1 = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'";
var _selectTableV2 = "SELECT * FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s'";
var _selectColumnsV1 = "SELECT * FROM system.schema_columns WHERE keyspace_name='%s' AND columnfamily_name='%s'";
var _selectColumnsV2 = "SELECT * FROM system_schema.columns WHERE keyspace_name='%s' AND table_name='%s'";
var _selectIndexesV2 = "SELECT * FROM system_schema.indexes WHERE keyspace_name='%s' AND table_name='%s'";
var _selectUdtV1 = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='%s' AND type_name='%s'";
var _selectUdtV2 = "SELECT * FROM system_schema.types WHERE keyspace_name='%s' AND type_name='%s'";
var _selectFunctionsV1 = "SELECT * FROM system.schema_functions WHERE keyspace_name = '%s' AND function_name = '%s'";
var _selectFunctionsV2 = "SELECT * FROM system_schema.functions WHERE keyspace_name = '%s' AND function_name = '%s'";
var _selectAggregatesV1 = "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";
var _selectAggregatesV2 = "SELECT * FROM system_schema.aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";
var _selectMaterializedViewV2 = "SELECT * FROM system_schema.views WHERE keyspace_name = '%s' AND view_name = '%s'";

/**
 * @abstract
 * @param {ControlConnection} cc
 * @constructor
 * @ignore
 */
function SchemaParser(cc) {
  this.cc = cc;
  this.selectTable = null;
  this.selectColumns = null;
  this.selectIndexes = null;
  this.selectUdt = null;
  this.selectAggregates = null;
  this.selectFunctions = null;
}

/**
 * @param name
 * @param durableWrites
 * @param strategy
 * @param strategyOptions
 * @returns {{name, durableWrites, strategy, strategyOptions, tokenToReplica, udts, tables, functions, aggregates}|null}
 * @protected
 */
SchemaParser.prototype._createKeyspace = function (name, durableWrites, strategy, strategyOptions) {
  var ksInfo = {
    name: name,
    durableWrites: durableWrites,
    strategy: strategy,
    strategyOptions: strategyOptions,
    tokenToReplica: null,
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
 * @param {Function} callback
 */
SchemaParser.prototype.getTable = function (keyspaceName, name, cache, callback) {
  var tableInfo = cache && cache[name];
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
  var tableRow, columnRows, indexRows;
  var self = this;
  utils.series([
    function getTableRow(next) {
      var query = util.format(self.selectTable, keyspaceName, name);
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
        return next(null, null, null);
      }
      var query = util.format(self.selectColumns, keyspaceName, name);
      self.cc.query(query, function (err, response) {
        if (err) {
          return next(err);
        }
        columnRows = response.rows;
        next();
      });
    },
    function getIndexes(next) {
      if (!tableRow || !self.selectIndexes) {
        //either the table does not exists or it does not support indexes schema table
        return next();
      }
      var query = util.format(self.selectIndexes, keyspaceName, name);
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
    self._parseTableOrView(tableInfo, tableRow, columnRows, indexRows, function (err) {
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
  var udtInfo = cache && cache[name];
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
  var query = util.format(this.selectUdt, keyspaceName, name);
  var self = this;
  this.cc.query(query, function (err, response) {
    if (err) {
      return udtInfo.emit('load', err);
    }
    var row = response.rows[0];
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

//noinspection JSValidateJSDoc
/**
 * Builds the metadata based on the table and column rows
 * @abstract
 * @param {module:metadata~TableMetadata} tableInfo
 * @param {Row} tableRow
 * @param {Array.<Row>} columnRows
 * @param {Array.<Row>} indexRows
 * @param {Function} callback
 * @throws {Error}
 */
SchemaParser.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, callback) {
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
  var query = this.selectFunctions;
  var parser = this._parseFunction.bind(this);
  if (aggregate) {
    query = this.selectAggregates;
    parser = this._parseAggregate.bind(this);
  }
  //if not already loaded
  //get all functions with that name
  //cache it by name and, within name, by signature
  var functionsInfo = cache && cache[name];
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

/**
 * Used to parse schema information for Cassandra versions 1.2.x, and 2.x
 * @param {ControlConnection} cc
 * @constructor
 * @ignore
 */
function SchemaParserV1(cc) {
  SchemaParser.call(this, cc);
  this.selectTable = _selectTableV1;
  this.selectColumns = _selectColumnsV1;
  this.selectUdt = _selectUdtV1;
  this.selectAggregates = _selectAggregatesV1;
  this.selectFunctions = _selectFunctionsV1;
}

util.inherits(SchemaParserV1, SchemaParser);

/** @override */
SchemaParserV1.prototype.getKeyspaces = function (waitReconnect, callback) {
  var self = this;
  var keyspaces = {};
  this.cc.query(_selectAllKeyspacesV1, waitReconnect, function (err, result) {
    if (err) {
      return callback(err);
    }
    for (var i = 0; i < result.rows.length; i++) {
      var row = result.rows[i];
      var ksInfo = self._createKeyspace(
        row['keyspace_name'],
        row['durable_writes'],
        row['strategy_class'],
        JSON.parse(row['strategy_options']));
      keyspaces[ksInfo.name] = ksInfo;
    }
    callback(null, keyspaces);
  });
};

/** @override */
SchemaParserV1.prototype.getKeyspace = function (name, callback) {
  var self = this;
  this.cc.query(util.format(_selectSingleKeyspaceV1, name), function (err, result) {
    if (err) {
      return callback(err);
    }
    var row = result.rows[0];
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

//noinspection JSUnusedLocalSymbols
/** @override */
SchemaParserV1.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, callback) {
  var i, c, name, types;
  var encoder = this.cc.getEncoder();
  var columnsKeyed = {};
  var partitionKeys = [];
  var clusteringKeys = [];
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
        var row = columnRows[i];
        var type = encoder.parseFqTypeName(row['validator']);
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
    var keysStoredInTableRow = (tableInfo.partitionKeys.length === 0);
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
    var comparator = encoder.parseKeyTypes(tableRow['comparator']);
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
  var encoder = this.cc.getEncoder();
  var aggregate = new Aggregate();
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
    var initConditionValue = encoder.decode(aggregate.initConditionRaw, aggregate.stateType);
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
  var encoder = this.cc.getEncoder();
  var func = new SchemaFunction();
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
  var encoder = this.cc.getEncoder();
  var fieldNames = row['field_names'];
  var fieldTypes = row['field_types'];
  var fields = new Array(fieldNames.length);
  try {
    for (var i = 0; i < fieldNames.length; i++) {
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
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @constructor
 * @ignore
 */
function SchemaParserV2(cc, udtResolver) {
  SchemaParser.call(this, cc);
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
  var self = this;
  var keyspaces = {};
  this.cc.query(_selectAllKeyspacesV2, waitReconnect, function (err, result) {
    if (err) {
      return callback(err);
    }
    for (var i = 0; i < result.rows.length; i++) {
      var ksInfo = self._parseKeyspace(result.rows[i]);
      keyspaces[ksInfo.name] = ksInfo;
    }
    callback(null, keyspaces);
  });
};

/** @override */
SchemaParserV2.prototype.getKeyspace = function (name, callback) {
  var self = this;
  this.cc.query(util.format(_selectSingleKeyspaceV2, name), function (err, result) {
    if (err) {
      return callback(err);
    }
    var row = result.rows[0];
    if (!row) {
      return callback(null, null);
    }
    callback(null, self._parseKeyspace(row));
  });
};

/** @override */
SchemaParserV2.prototype.getMaterializedView = function (keyspaceName, name, cache, callback) {
  var viewInfo = cache && cache[name];
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
  var tableRow, columnRows;
  //it is not cached, try to query for it
  var self = this;
  utils.series([
    function getTableRow(next) {
      var query = util.format(_selectMaterializedViewV2, keyspaceName, name);
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
      var query = util.format(self.selectColumns, keyspaceName, name);
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
    self._parseTableOrView(viewInfo, tableRow, columnRows, null, function (err) {
      viewInfo.loading = false;
      viewInfo.loaded = !err;
      viewInfo.emit('load', err, viewInfo);
    });
  });

};

SchemaParserV2.prototype._parseKeyspace = function (row) {
  var replication = row['replication'];
  var strategy;
  var strategyOptions;
  if (replication) {
    strategy = replication['class'];
    strategyOptions = {};
    for (var key in replication) {
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
    strategyOptions);
};

/** @override */
SchemaParserV2.prototype._parseTableOrView = function (tableInfo, tableRow, columnRows, indexRows, callback) {
  var encoder = this.cc.getEncoder();
  var columnsKeyed = {};
  var partitionKeys = [];
  var clusteringKeys = [];
  var isView = tableInfo instanceof MaterializedView;
  tableInfo.bloomFilterFalsePositiveChance = tableRow['bloom_filter_fp_chance'];
  tableInfo.caching = JSON.stringify(tableRow['caching']);
  tableInfo.comment = tableRow['comment'];
  var compaction = tableRow['compaction'];
  if (compaction) {
    tableInfo.compactionOptions = {};
    tableInfo.compactionClass = compaction['class'];
    for (var key in compaction) {
      if (!compaction.hasOwnProperty(key) || key === 'class') {
        continue;
      }
      tableInfo.compactionOptions[key] = compaction[key];
    }
  }
  tableInfo.compression = tableRow['compression'];
  tableInfo.gcGraceSeconds = tableRow['gc_grace_seconds'];
  tableInfo.localReadRepairChance = tableRow['dclocal_read_repair_chance'];
  tableInfo.readRepairChance = tableRow['read_repair_chance'];
  tableInfo.extensions = tableRow['extensions'];
  tableInfo.crcCheckChance = tableRow['crc_check_chance'];
  tableInfo.memtableFlushPeriod = tableRow['memtable_flush_period_in_ms'] || tableInfo.memtableFlushPeriod;
  tableInfo.defaultTtl = tableRow['default_time_to_live'] || tableInfo.defaultTtl;
  tableInfo.speculativeRetry = tableRow['speculative_retry'] || tableInfo.speculativeRetry;
  tableInfo.minIndexInterval = tableRow['min_index_interval'] || tableInfo.minIndexInterval;
  tableInfo.maxIndexInterval = tableRow['max_index_interval'] || tableInfo.maxIndexInterval;
  var self = this;
  utils.map(columnRows, function (row, next) {
    encoder.parseTypeName(tableRow['keyspace_name'], row['type'], 0, null, self.udtResolver, function (err, type) {
      if (err) {
        return next(err);
      }
      var c = {
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
  }, function (err, columns) {
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
    tableInfo.indexes = Index.fromRows(indexRows);
    var flags = tableRow['flags'];
    var isDense = flags.indexOf('dense') >= 0;
    var isSuper = flags.indexOf('super') >= 0;
    var isCompound = flags.indexOf('compound') >= 0;
    tableInfo.isCompact = isSuper || isDense || !isCompound;
    //remove the columns related to Thrift
    var isStaticCompact = !isSuper && !isDense && !isCompound;
    if(isStaticCompact) {
      pruneStaticCompactTableColumns(tableInfo);
    }
    else if (isDense) {
      pruneDenseTableColumns(tableInfo);
    }
    callback();
  });
};

/** @override */
SchemaParserV2.prototype._parseAggregate = function (row, callback) {
  var encoder = this.cc.getEncoder();
  var aggregate = new Aggregate();
  aggregate.name = row['aggregate_name'];
  aggregate.keyspaceName = row['keyspace_name'];
  aggregate.signature = row['argument_types'] || utils.emptyArray;
  aggregate.stateFunction = row['state_func'];
  aggregate.finalFunction = row['final_func'];
  aggregate.initConditionRaw = row['initcond'];
  aggregate.initCondition = aggregate.initConditionRaw;
  var self = this;
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
  var encoder = this.cc.getEncoder();
  var func = new SchemaFunction();
  func.name = row['function_name'];
  func.keyspaceName = row['keyspace_name'];
  func.signature = row['argument_types'] || utils.emptyArray;
  func.argumentNames = row['argument_names'] || utils.emptyArray;
  func.body = row['body'];
  func.calledOnNullInput = row['called_on_null_input'];
  func.language = row['language'];
  var self = this;
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
  var encoder = this.cc.getEncoder();
  var fieldTypes = row['field_types'];
  var keyspace = row['keyspace_name'];
  var fields = new Array(fieldTypes.length);
  var self = this;
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

//noinspection JSValidateJSDoc
/**
 * Upon migration from thrift to CQL, we internally create a pair of surrogate clustering/regular columns
 * for compact static tables. These columns shouldn't be exposed to the user but are currently returned by C*.
 * We also need to remove the static keyword for all other columns in the table.
 * @param {module:metadata~TableMetadata} tableInfo
*/
function pruneStaticCompactTableColumns(tableInfo) {
  var i;
  var c;
  //remove "column1 text" clustering column
  for (i = 0; i < tableInfo.clusteringKeys.length; i++) {
    c = tableInfo.clusteringKeys[i];
    var index = tableInfo.columns.indexOf(c);
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

//noinspection JSValidateJSDoc
/**
 * Upon migration from thrift to CQL, we internally create a surrogate column "value" of type custom.
 * This column shouldn't be exposed to the user but is currently returned by C*.
 * @param {module:metadata~TableMetadata} tableInfo
 */
function pruneDenseTableColumns(tableInfo) {
  var i = tableInfo.columns.length;
  while (i--) {
    var c = tableInfo.columns[i];
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
    var rf = parseInt(strategyOptions['replication_factor'], 10);
    if (rf > 1) {
      return getTokenToReplicaSimpleMapper(rf);
    }
  }
  if (/NetworkTopologyStrategy$/.test(strategy)) {
    //noinspection JSUnresolvedVariable
    return getTokenToReplicaNetworkMapper(strategyOptions);
  }
  //default, wrap in an Array
  return (function noStrategy(tokenizer, ring, primaryReplicas) {
    var replicas = {};
    for (var key in primaryReplicas) {
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
  return (function tokenSimpleStrategy(tokenizer, ring, primaryReplicas) {
    var rf = Math.min(replicationFactor, ring.length);
    var replicas = {};
    for (var i = 0; i < ring.length; i++) {
      var token = ring[i];
      var key = tokenizer.stringify(token);
      var tokenReplicas = [primaryReplicas[key]];
      for (var j = 1; j < rf; j++) {
        var nextReplicaIndex = i + j;
        if (nextReplicaIndex >= ring.length) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % ring.length;
        }
        var nextReplica = primaryReplicas[tokenizer.stringify(ring[nextReplicaIndex])];
        tokenReplicas.push(nextReplica);
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
  return (function tokenNetworkStrategy(tokenizer, ring, primaryReplicas, datacenters) {
    var replicas = {};
    for (var i = 0; i < ring.length; i++) {
      var token = ring[i];
      var key = tokenizer.stringify(token);
      var tokenReplicas = [];
      var replicasByDc = {};
      var racksPlaced = {};
      var skippedHosts = [];
      for (var j = 0; j < ring.length; j++) {
        var nextReplicaIndex = i + j;
        if (nextReplicaIndex >= ring.length) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % ring.length;
        }
        var h = primaryReplicas[tokenizer.stringify(ring[nextReplicaIndex])];
        var dc = h.datacenter;
        //Check if the next replica belongs to one of the targeted dcs
        var dcRf = parseInt(replicationFactors[dc], 10);
        if (!dcRf) {
          continue;
        }
        dcRf = Math.min(dcRf, datacenters[dc].hostLength);
        var dcReplicas = replicasByDc[dc] || 0;
        //Amount of replicas per dc is greater than rf or the amount of host in the datacenter
        if (dcReplicas >= dcRf) {
          continue;
        }
        var racksPlacedInDc = racksPlaced[dc];
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
  var i;
  for (i = 0; i < dcRf - dcReplicas && i < skippedHosts.length; i++) {
    tokenReplicas.push(skippedHosts[i]);
  }
  return i;
}

function isDoneForToken(replicationFactors, datacenters, replicasByDc) {
  var keys = Object.keys(replicationFactors);
  for (var i = 0; i < keys.length; i++) {
    var dcName = keys[i];
    var dc = datacenters[dcName];
    if (!dc) {
      // A DC is included in the RF but the DC does not exist in the topology
      continue;
    }
    var rf = Math.min(parseInt(replicationFactors[dcName], 10), dc.hostLength);
    if (rf > 0 && (!replicasByDc[dcName] || replicasByDc[dcName] < rf)) {
      return false;
    }
  }
  return true;
}
/**
 * Creates a new instance if the currentInstance is not valid for the
 * provided Cassandra version
 * @param {ControlConnection} cc The control connection to be used
 * @param {Function} udtResolver The function to be used to retrieve the udts.
 * @param {Array.<Number>} [version] The cassandra version
 * @param {SchemaParser} [currentInstance] The current instance
 * @returns {SchemaParser}
 */
function getByVersion(cc, udtResolver, version, currentInstance) {
  var parserConstructor = SchemaParserV1;
  if (version && version[0] >= 3) {
    parserConstructor = SchemaParserV2;
  }
  if (!currentInstance || !(currentInstance instanceof parserConstructor)){
    return new parserConstructor(cc, udtResolver);
  }
  return currentInstance;
}

exports.getByVersion = getByVersion;