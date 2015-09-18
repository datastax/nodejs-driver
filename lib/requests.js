var async = require('async');
var events = require('events');
var util = require('util');

var FrameWriter = require('./writers').FrameWriter;
var types = require('./types');
var utils = require('./utils');

/**
 * Options for the execution of the query / prepared statement
 * @private
 */
var queryFlag = {
  values:                 0x01,
  skipMetadata:           0x02,
  pageSize:               0x04,
  withPagingState:        0x08,
  withSerialConsistency:  0x10,
  withDefaultTimestamp:   0x20,
  withNameForValues:      0x40
};

/**
 * Options for the executing of a batch request from protocol v3 and above
 * @private
 */
var batchFlag = {
  withSerialConsistency:  0x10,
  withDefaultTimestamp:   0x20,
  withNameForValues:      0x40
};

/**
 * Abstract class Request
 * @constructor
 */
function Request() {

}

/**
 * @abstract
 * @param {Encoder} encoder
 * @throws {TypeError}
 * @returns {Buffer}
 */
Request.prototype.write = function (encoder) {
  throw new Error('Method must be implemented');
};

/**
 * Writes a execute query (given a prepared queryId)
 */
function ExecuteRequest(queryId, params, options) {
  this.streamId = null;
  this.queryId = queryId;
  this.params = params;
  this.setOptions(options);
}

util.inherits(ExecuteRequest, Request);

ExecuteRequest.prototype.write = function (encoder) {
  //v1: <queryId>
  //      <n><value_1>....<value_n><consistency>
  //v2: <queryId>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  //v3: <queryId>
  //      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  var frameWriter = new FrameWriter(types.opcodes.execute);
  var headerFlags = this.options.traceQuery ? types.frameFlags.tracing : 0;
  if (this.options.customPayload) {
    //The body may contain the custom payload
    headerFlags |= types.frameFlags.customPayload;
    frameWriter.writeCustomPayload(this.options.customPayload);
  }
  frameWriter.writeShortBytes(this.queryId);
  this.writeQueryParameters(frameWriter, encoder);
  return frameWriter.write(this.version, this.streamId, headerFlags);
};

/**
 * Writes v1 and v2 execute query parameters
 * @param {FrameWriter} frameWriter
 * @param {Encoder} encoder
 */
ExecuteRequest.prototype.writeQueryParameters = function (frameWriter, encoder) {
  //v1: <n><value_1>....<value_n><consistency>
  //v2: <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  //v3: <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  var flags = 0;
  if (this.version > 1) {
    flags |= (this.params && this.params.length) ? queryFlag.values : 0;
    flags |= (this.options.fetchSize > 0) ? queryFlag.pageSize : 0;
    flags |= this.options.pageState ? queryFlag.withPagingState : 0;
    flags |= this.options.serialConsistency ? queryFlag.withSerialConsistency : 0;
    flags |= this.options.timestamp ? queryFlag.withDefaultTimestamp : 0;
    flags |= this.options.namedParameters ? queryFlag.withNameForValues : 0;
    frameWriter.writeShort(this.consistency);
    frameWriter.writeByte(flags);
  }
  if (this.params && this.params.length) {
    frameWriter.writeShort(this.params.length);
    for (var i = 0; i < this.params.length; i++) {
      var paramValue = this.params[i];
      if (flags & queryFlag.withNameForValues) {
        //parameter is composed by name / value
        frameWriter.writeString(paramValue.name);
        paramValue = paramValue.value;
      }
      frameWriter.writeBytes(encoder.encode(paramValue, this.hints[i]));
    }
  }
  if (this.version === 1) {
    if (!this.params || !this.params.length) {
      //zero parameters
      frameWriter.writeShort(0);
    }
    frameWriter.writeShort(this.consistency);
    return;
  }
  if (flags & queryFlag.pageSize) {
    frameWriter.writeInt(this.options.fetchSize);
  }
  if (flags & queryFlag.withPagingState) {
    frameWriter.writeBytes(this.options.pageState);
  }
  if (flags & queryFlag.withSerialConsistency) {
    frameWriter.writeShort(this.options.serialConsistency);
  }
  if (flags & queryFlag.withDefaultTimestamp) {
    var timestamp = this.options.timestamp;
    if (typeof timestamp === 'number') {
      timestamp = types.Long.fromNumber(timestamp);
    }
    frameWriter.writeLong(timestamp);
  }
};

ExecuteRequest.prototype.setOptions = function (options) {
  this.options = options || utils.emptyObject;
  this.consistency = this.options.consistency || types.consistencies.one;
  this.hints = this.options.hints || utils.emptyArray;
};

function QueryRequest(query, params, options) {
  this.streamId = null;
  this.query = query;
  this.params = params;
  this.setOptions(options);
}

util.inherits(QueryRequest, ExecuteRequest);

QueryRequest.prototype.write = function (encoder) {
  //v1: <query><consistency>
  //v2: <query>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  //v3: <query>
  //      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  var frameWriter = new FrameWriter(types.opcodes.query);
  var headerFlags = this.options.traceQuery ? types.frameFlags.tracing : 0;
  if (this.options.customPayload) {
    //The body may contain the custom payload
    headerFlags |= types.frameFlags.customPayload;
    frameWriter.writeCustomPayload(this.options.customPayload);
  }
  frameWriter.writeLString(this.query);
  if (this.version === 1) {
    frameWriter.writeShort(this.consistency);
  }
  else {
    //Use the same fields as the execute writer
    this.writeQueryParameters(frameWriter, encoder);
  }
  return frameWriter.write(this.version, this.streamId, headerFlags);
};

function PrepareRequest(query) {
  this.streamId = null;
  this.query = query;
}

util.inherits(PrepareRequest, Request);

PrepareRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.prepare);
  frameWriter.writeLString(this.query);
  return frameWriter.write(this.version, this.streamId);
};

function StartupRequest(cqlVersion) {
  this.cqlVersion = cqlVersion || '3.0.0';
  this.streamId = null;
}

util.inherits(StartupRequest, Request);

StartupRequest.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.startup);
  frameWriter.writeStringMap({
    CQL_VERSION: this.cqlVersion
  });
  return frameWriter.write(this.version, this.streamId);
};

function RegisterRequest(events) {
  this.events = events;
  this.streamId = null;
}

util.inherits(RegisterRequest, Request);

RegisterRequest.prototype.write = function() {
  var frameWriter = new FrameWriter(types.opcodes.register);
  frameWriter.writeStringList(this.events);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Represents an AUTH_RESPONSE request
 * @param {Buffer} token
 * @constructor
 */
function AuthResponseRequest(token) {
  this.token = token;
  this.streamId = null;
}

util.inherits(AuthResponseRequest, Request);

AuthResponseRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.authResponse);
  frameWriter.writeBytes(this.token);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Represents a protocol v1 CREDENTIALS request message
 * @constructor
 */
function CredentialsRequest(username, password) {
  this.username = username;
  this.password = password;
  this.streamId = null;
}

util.inherits(CredentialsRequest, Request);

CredentialsRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.credentials);
  frameWriter.writeStringMap({username:this.username, password:this.password});
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Writes a batch request
 * @param {Array.<{query, params, [info]}>} queries Array of objects with the properties query and params
 * @param {QueryOptions} options
 * @constructor
 */
function BatchRequest(queries, options) {
  this.queries = queries;
  /** @type {QueryOptions} */
  this.options = options;
  this.type = options.logged ? 0 : 1;
  this.type = options.counter ? 2 : this.type;
  this.streamId = null;
  this.hints = options.hints || utils.emptyArray;
}

util.inherits(BatchRequest, Request);

BatchRequest.prototype.write = function (encoder) {
  //v2: <type><n><query_1>...<query_n><consistency>
  //v3: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
  if (!this.queries || !(this.queries.length > 0)) {
    throw new TypeError(util.format('Invalid queries provided %s', this.queries));
  }
  var frameWriter = new FrameWriter(types.opcodes.batch);
  var headerFlags = this.options.traceQuery ? types.frameFlags.tracing : 0;
  if (this.options.customPayload) {
    //The body may contain the custom payload
    headerFlags |= types.frameFlags.customPayload;
    frameWriter.writeCustomPayload(this.options.customPayload);
  }
  frameWriter.writeByte(this.type);
  frameWriter.writeShort(this.queries.length);
  var self = this;
  this.queries.forEach(function eachQuery(item, i) {
    var hints = self.hints[i];
    var params = item.params || utils.emptyArray;
    if (item.info) {
      //As prepared queries
      frameWriter.writeByte(1);
      frameWriter.writeShortBytes(item.info.queryId);
      hints = item.info.meta.columns.map(function (c) { return c.type; });
      var paramsInfo = utils.adaptNamedParamsPrepared(params, item.info.meta.columns);
      params = paramsInfo.params;
    }
    else {
      //as simple query
      frameWriter.writeByte(0);
      frameWriter.writeLString(item.query);
    }
    frameWriter.writeShort(params.length);
    params.forEach(function (param, paramIndex) {
      frameWriter.writeBytes(encoder.encode(param, hints ? hints[paramIndex] : null));
    });
  }, this);
  frameWriter.writeShort(this.options.consistency);
  if (this.version >= 3) {
    //Batch flags
    var flags = this.options.serialConsistency ? batchFlag.withSerialConsistency : 0;
    flags |= this.options.timestamp ? batchFlag.withDefaultTimestamp : 0;
    frameWriter.writeByte(flags);
    if (this.options.serialConsistency) {
      frameWriter.writeShort(this.options.serialConsistency);
    }
    if (this.options.timestamp) {
      var timestamp = this.options.timestamp;
      if (typeof timestamp === 'number') {
        timestamp = types.Long.fromNumber(timestamp);
      }
      frameWriter.writeLong(timestamp);
    }
  }
  return frameWriter.write(this.version, this.streamId, headerFlags);
};


exports.AuthResponseRequest = AuthResponseRequest;
exports.BatchRequest = BatchRequest;
exports.CredentialsRequest = CredentialsRequest;
exports.ExecuteRequest = ExecuteRequest;
exports.PrepareRequest = PrepareRequest;
exports.QueryRequest = QueryRequest;
exports.Request = Request;
exports.RegisterRequest = RegisterRequest;
exports.StartupRequest = StartupRequest;