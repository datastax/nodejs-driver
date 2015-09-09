"use strict";
var util = require('util');
var async = require('async');
var events = require('events');
var utils = require('../utils');
var TableMetadata = require('./table-metadata');
var Aggregate = require('./aggregate');
var SchemaFunction = require('./schema-function');
/**
 * @module metadata/schemaParser
 * @ignore
 */

var _compositeTypeName = 'org.apache.cassandra.db.marshal.CompositeType';
var _selectAllKeyspaces = "SELECT * FROM system.schema_keyspaces";
var _selectSingleKeyspace = "SELECT * FROM system.schema_keyspaces where keyspace_name = '%s'";
var _selectTable = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'";
var _selectColumns = "SELECT * FROM system.schema_columns WHERE keyspace_name='%s' AND columnfamily_name='%s'";
var _selectUdt = "SELECT * FROM system.schema_usertypes WHERE keyspace_name='%s' AND type_name='%s'";
var _selectFunctions = "SELECT * FROM system.schema_functions WHERE keyspace_name = '%s' AND function_name = '%s'";
var _selectAggregates = "SELECT * FROM system.schema_aggregates WHERE keyspace_name = '%s' AND aggregate_name = '%s'";

function SchemaParser() {

}

/**
 * @param row
 * @returns {{name, durableWrites, strategy, strategyOptions, tokenToReplica, udts, tables, functions, aggregates}|null}
 * @protected
 */
SchemaParser.prototype._parseKeyspace = function (row) {
  if (!row) {
    return null;
  }
  var ksInfo = {
    name: row['keyspace_name'],
    durableWrites: row['durable_writes'],
    strategy: row['strategy_class'],
    strategyOptions: JSON.parse(row['strategy_options']),
    tokenToReplica: null,
    udts: {},
    tables: {},
    functions: {},
    aggregates: {}
  };
  ksInfo.tokenToReplica = getTokenToReplicaMapper(ksInfo.strategy, ksInfo.strategyOptions);
  return ksInfo;
};

/**
 * @param {ControlConnection} cc
 * @param {String} name
 * @param {Function} callback
 */
SchemaParser.prototype.getKeyspace = function (cc, name, callback) {
  var self = this;
  cc.query(_selectAllKeyspaces, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, self._parseKeyspace(result.rows[0]));
  });
};

/**
 * @param {ControlConnection} cc
 * @param {Function} callback
 */
SchemaParser.prototype.getKeyspaces = function (cc, callback) {
  var self = this;
  var keyspaces = {};
  cc.query(_selectAllKeyspaces, function (err, result) {
    if (err) return callback(err);
    for (var i = 0; i < result.rows.length; i++) {
      var ksInfo = self._parseKeyspace(result.rows[i]);
      keyspaces[ksInfo.name] = ksInfo;
    }
    callback(null, keyspaces);
  });
};

/**
 * @abstract
 * @param {ControlConnection} cc
 * @param keyspace
 * @param {String} name
 * @param {Function} callback
 */
SchemaParser.prototype.getTable = function (cc, keyspace, name, callback) {
};

/**
 * @abstract
 * @param {ControlConnection} cc
 * @param keyspace
 * @param {String} name
 * @param {Function} callback
 */
SchemaParser.prototype.getUdt = function (cc, keyspace, name, callback) {
};

/**
 * @abstract
 * @param {ControlConnection} cc
 * @param keyspace
 * @param {String} name
 * @param {Boolean} aggregate
 * @param {Function} callback
 */
SchemaParser.prototype.getFunctions = function (cc, keyspace, name, aggregate, callback) {
  
};

/**
 * Used to parse schema information for Cassandra versions 1.2.x, and 2.x
 * @ignore
 * @constructor
 */
function SchemaParserV1() {

}

util.inherits(SchemaParserV1, SchemaParser);

/** @override */
SchemaParserV1.prototype.getTable = function (cc, keyspace, name, callback) {
  var tableInfo = keyspace.tables[name];
  if (!tableInfo) {
    keyspace.tables[name] = tableInfo = new TableMetadata(name);
  }
  if (tableInfo.loaded) {
    return callback(null, tableInfo);
  }
  tableInfo.once('load', callback);
  if (tableInfo.loading) {
    //It' already queued, it will be emitted
    return;
  }
  tableInfo.loading = true;
  //it is not cached, try to query for it
  var self = this;
  async.waterfall([
    function getTableRow(next) {
      var query = util.format(_selectTable, keyspace.name, name);
      cc.query(query, function (err, response) {
        if (err) return next(err);
        next(null, response.rows[0]);
      });
    },
    function getColumnRows (tableRow, next) {
      if (!tableRow) return next();
      var query = util.format(_selectColumns, keyspace.name, name);
      cc.query(query, function (err, response) {
        if (err) return next(err);
        next(null, tableRow, response.rows);
      });
    }
  ], function afterQuery (err, tableRow, columnRows) {
    tableInfo.loading = false;
    if (err || !tableRow) {
      return tableInfo.emit('load', err, null);
    }
    try {
      self._parseTable(tableInfo, cc.getEncoder(), tableRow, columnRows);
    }
    catch (parseError) {
      err = parseError;
    }
    tableInfo.emit('load', err, tableInfo);
  });
};

/** @override */
SchemaParserV1.prototype.getUdt = function (cc, keyspace, name, callback) {
  var udtInfo = keyspace.udts[name];
  if (!udtInfo) {
    keyspace.udts[name] = udtInfo = new events.EventEmitter();
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
  var query = util.format(_selectUdt, keyspace.name, name);
  var self = this;
  cc.query(query, function (err, response) {
    udtInfo.loading = false;
    if (err) {
      return udtInfo.emit('load', err);
    }
    var row = response.rows[0];
    udtInfo.emit('load', null, self._parseUdt(udtInfo, cc.getEncoder(), row));
  });
};

/** @override */
SchemaParserV1.prototype.getFunctions = function (cc, keyspace, name, aggregate, callback) {
  var cache = keyspace.functions;
  var query = _selectFunctions;
  var Constructor = SchemaFunction;
  var parser = this._parseFunction;
  if (aggregate) {
    cache = keyspace.aggregates;
    query = _selectAggregates;
    Constructor = Aggregate;
    parser = this._parseAggregate;
  }
  //if not already loaded
  //get all functions with that name
  //cache it by name and, within name, by signature
  var functionsInfo = cache[name];
  if (!functionsInfo) {
    cache[name] = functionsInfo = new events.EventEmitter();
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
  query = util.format(query, keyspace.name, name);
  cc.query(query, function (err, response) {
    functionsInfo.loading = false;
    if (err || response.rows.length === 0) {
      return functionsInfo.emit('load', err, null);
    }
    if (response.rows.length > 0) {
      functionsInfo.values = {};
    }
    try {
      response.rows.forEach(function (row) {
        var func = new Constructor();
        parser(cc.getEncoder(), func, row);
        functionsInfo.values['(' + func.signature.join(',') + ')'] = func;
      });
    }
    catch (buildErr) {
      err = buildErr;
    }
    if (err) {
      functionsInfo.values = null;
    }
    functionsInfo.emit('load', err, functionsInfo.values);
  });
};
/**
 * Builds the metadata based on the table and column rows
 * @param {TableMetadata} tableInfo
 * @param {Encoder} encoder
 * @param {Row} tableRow
 * @param {Array.<Row>} columnRows
 * @throws {Error}
 */
SchemaParserV1.prototype._parseTable = function (tableInfo, encoder, tableRow, columnRows) {
  var i, c, name, types;
  var columnsKeyed = {};
  var partitionKeys = [];
  var clusteringKeys = [];
  tableInfo.loaded = true;
  tableInfo.bloomFilterFalsePositiveChance = tableRow['bloom_filter_fp_chance'];
  tableInfo.caching = tableRow['caching'];
  tableInfo.comment = tableRow['comment'];
  tableInfo.compactionClass = tableRow['compaction_strategy_class'];
  tableInfo.compactionOptions = JSON.parse(tableRow['compaction_strategy_options']);
  tableInfo.compression = JSON.parse(tableRow['compression_parameters']);
  tableInfo.gcGraceSeconds = tableRow['gc_grace_seconds'];
  tableInfo.localReadRepairChance = tableRow['local_read_repair_chance'];
  tableInfo.readRepairChance = tableRow['read_repair_chance'];
  if (typeof tableRow['replicate_on_write'] !== 'undefined') {
    //leave the default otherwise
    tableInfo.replicateOnWrite = tableRow['replicate_on_write'];
  }
  for (i = 0; i < columnRows.length; i++) {
    var row = columnRows[i];
    var type = encoder.parseTypeName(row['validator']);
    c = {
      name: row['column_name'],
      type: type
    };
    tableInfo.columns.push(c);
    columnsKeyed[c.name] = c;
    switch (row['type']) {
      case 'partition_key':
        partitionKeys.push({ c: c, index: (row['component_index'] || 0)});
        break;
      case 'clustering_key':
        clusteringKeys.push({ c: c, index: (row['component_index'] || 0), order: c.type.options.reversed ? 'DESC' : 'ASC'});
        break;
    }
  }
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
  if (tableRow['key_aliases'] && tableInfo.partitionKeys.length === 0) {
    //In C* 1.2, keys are not stored on the schema_columns table
    partitionKeys = JSON.parse(tableRow['key_aliases']);
    types = adaptKeyTypes(tableRow['key_validator'], encoder);
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
  if (tableRow['column_aliases'] && tableInfo.clusteringKeys.length === 0) {
    clusteringKeys = JSON.parse(tableRow['column_aliases']);
    types = adaptKeyTypes(tableRow['comparator'], encoder);
    for (i = 0; i < clusteringKeys.length; i++) {
      name = clusteringKeys[i];
      c = columnsKeyed[name];
      if (!c) {
        c = {
          name: name,
          type: types[i]
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
    tableInfo.isCompact = !!(
    tableRow['comparator'].indexOf(_compositeTypeName) !== 0 ||
    (types && tableInfo.clusteringKeys.length !== (types.length - 1)));
  }
  name = tableRow['value_alias'];
  if (tableInfo.isCompact && name && !columnsKeyed[name]) {
    //additional column in C* 1.2 as value_alias
    c = {
      name: name,
      type: encoder.parseTypeName(tableRow['default_validator'])
    };
    tableInfo.columns.push(c);
    columnsKeyed[name] = c;
  }
  tableInfo.columnsByName = columnsKeyed;
  return tableInfo;
};

SchemaParserV1.prototype._parseAggregate = function (encoder, aggregate, row) {
  aggregate.name = row['aggregate_name'];
  aggregate.keyspaceName = row['keyspace_name'];
  aggregate.signature = row['signature'] || utils.emptyArray;
  aggregate.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
    return encoder.parseTypeName(name);
  });
  aggregate.stateFunction = row['state_func'];
  aggregate.stateType = encoder.parseTypeName(row['state_type']);
  aggregate.finalFunction = row['final_func'];
  aggregate.initConditionRaw = row['initcond'];
  aggregate.initCondition = encoder.decode(aggregate.initConditionRaw, aggregate.stateType);
  aggregate.returnType = encoder.parseTypeName(row['return_type']);
};

SchemaParserV1.prototype._parseFunction = function (encoder, func, row) {
  func.name = row['function_name'];
  func.keyspaceName = row['keyspace_name'];
  func.signature = row['signature'] || utils.emptyArray;
  func.argumentNames = row['argument_names'] || utils.emptyArray;
  func.argumentTypes = (row['argument_types'] || utils.emptyArray).map(function (name) {
    return encoder.parseTypeName(name);
  });
  func.body = row['body'];
  func.calledOnNullInput = row['called_on_null_input'];
  func.language = row['language'];
  func.returnType = encoder.parseTypeName(row['return_type']);
};

/**
 * Parses the udt information from the row
 * @returns {{fields: Array}}|null
 */
SchemaParserV1.prototype._parseUdt = function (udtInfo, encoder, row) {
  if (!row) {
    return null;
  }
  var fieldNames = row['field_names'];
  var fieldTypes = row['field_types'];
  var fields = new Array(fieldNames.length);
  for (var i = 0; i < fieldNames.length; i++) {
    fields[i] = {
      name: fieldNames[i],
      type: encoder.parseTypeName(fieldTypes[i])
    };
  }
  udtInfo.fields = fields;
  return udtInfo;
};

/**
 * Used to parse schema information for Cassandra versions 3.x and above
 * @constructor
 */
function SchemaParserV2() {

}

util.inherits(SchemaParserV2, SchemaParser);



/**
 * @param {String} typesString
 * @param {Encoder} encoder
 * @returns {Array}
 * @private
 */
function adaptKeyTypes(typesString, encoder) {
  var i;
  var indexes = [];
  for (i = 1; i < typesString.length; i++) {
    if (typesString[i] === ',') {
      indexes.push(i + 1);
    }
  }
  if (typesString.indexOf(_compositeTypeName) === 0) {
    indexes.unshift(_compositeTypeName.length + 1);
    indexes.push(typesString.length);
  }
  else {
    indexes.unshift(0);
    //we are talking about indexes
    //the next valid start indexes would be at length + 1
    indexes.push(typesString.length + 1);
  }
  var types = new Array(indexes.length - 1);
  for (i = 0; i < types.length; i++) {
    types[i] = encoder.parseTypeName(typesString, indexes[i], indexes[i + 1] - indexes[i] - 1);
  }
  return types;
}

/** @private */
function getTokenToReplicaMapper(strategy, strategyOptions) {
  if (/SimpleStrategy$/.test(strategy)) {
    var rf = strategyOptions['replication_factor'];
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
 * @private
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
  //                A(1)
  //
  //           H         B(2)
  //                |
  //      G       --+--       C(1)
  //                |
  //           F         D(2)
  //
  //                E(1)
  return (function tokenNetworkStrategy(tokenizer, ring, primaryReplicas, datacenters) {
    function isDoneForToken(replicasByDc) {
      for (var dc in replicationFactors) {
        if (!replicationFactors.hasOwnProperty(dc)) {
          continue;
        }
        var rf = Math.min(replicationFactors[dc], datacenters[dc]);
        if (replicasByDc[dc] < rf) {
          return false;
        }
      }
      return true;
    }
    //For each token
    //Get an Array of tokens
    //Checking that there aren't more tokens per dc than specified by the replication factors
    var replicas = {};
    for (var i = 0; i < ring.length; i++) {
      var token = ring[i];
      var key = tokenizer.stringify(token);
      var tokenReplicas = [];
      var replicasByDc = {};
      for (var j = 0; j < ring.length; j++) {
        var nextReplicaIndex = i + j;
        if (nextReplicaIndex >= ring.length) {
          //circle back
          nextReplicaIndex = nextReplicaIndex % ring.length;
        }
        var h = primaryReplicas[tokenizer.stringify(ring[nextReplicaIndex])];
        //Check if the next replica belongs to one of the targeted dcs
        var dcRf = parseFloat(replicationFactors[h.datacenter]);
        if (!dcRf) {
          continue;
        }
        dcRf = Math.min(dcRf, datacenters[h.datacenter]);
        var dcReplicas = replicasByDc[h.datacenter] || 0;
        //Amount of replicas per dc is greater than rf or the amount of host in the datacenter
        if (dcReplicas >= dcRf) {
          continue;
        }
        replicasByDc[h.datacenter] = dcReplicas + 1;
        tokenReplicas.push(h);
        if (isDoneForToken(replicasByDc)) {
          break;
        }
      }
      replicas[key] = tokenReplicas;
    }
    return replicas;
  });
}

//singletons
var schemaParserV1Instance = new SchemaParserV1();
var schemaParserV2Instance = new SchemaParserV2();
/**
 * @param {Array.<Number>} version
 * @returns {SchemaParser}
 */
function getByVersion(version) {
  if (version[0] >= 3) {
    return schemaParserV2Instance;
  }
  return schemaParserV1Instance;
}

exports.getByVersion = getByVersion;