'use strict';
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
 * @param {Number} streamId
 * @throws {TypeError}
 * @returns {Buffer}
 */
Request.prototype.write = function (encoder, streamId) {
  throw new Error('Method must be implemented');
};

/**
 * Creates a new instance using the same constructor as the current instance, copying the properties.
 * @return {Request}
 */
Request.prototype.clone = function () {
  var newRequest = new (this.constructor)();
  var keysArray = Object.keys(this);
  for (var i = 0; i < keysArray.length; i++) {
    var key = keysArray[i];
    newRequest[key] = this[key];
  }
  return newRequest;
};

/**
 * Writes a execute query (given a prepared queryId)
 * @param {String} query
 * @param {Buffer} queryId
 * @param {Array} params
 * @param options
 */
function ExecuteRequest(query, queryId, params, options) {
  this.query = query;
  this.queryId = queryId;
  this.params = params;
  this.setOptions(options);
}

util.inherits(ExecuteRequest, Request);

ExecuteRequest.prototype.write = function (encoder, streamId) {
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
  return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
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
  if (types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
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
  if (!types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
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
  this.query = query;
  this.params = params;
  this.setOptions(options);
}

util.inherits(QueryRequest, ExecuteRequest);

QueryRequest.prototype.write = function (encoder, streamId) {
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
  if (!types.protocolVersion.supportsPaging(encoder.protocolVersion)) {
    frameWriter.writeShort(this.consistency);
  }
  else {
    //Use the same fields as the execute writer
    this.writeQueryParameters(frameWriter, encoder);
  }
  return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
};

function PrepareRequest(query) {
  this.query = query;
}

util.inherits(PrepareRequest, Request);

PrepareRequest.prototype.write = function (encoder, streamId) {
  var frameWriter = new FrameWriter(types.opcodes.prepare);
  frameWriter.writeLString(this.query);
  return frameWriter.write(encoder.protocolVersion, streamId);
};

function StartupRequest(cqlVersion) {
  this.cqlVersion = cqlVersion || '3.0.0';
}

util.inherits(StartupRequest, Request);

StartupRequest.prototype.write = function (encoder, streamId) {
  var frameWriter = new FrameWriter(types.opcodes.startup);
  frameWriter.writeStringMap({
    CQL_VERSION: this.cqlVersion
  });
  return frameWriter.write(encoder.protocolVersion, streamId);
};

function RegisterRequest(events) {
  this.events = events;
}

util.inherits(RegisterRequest, Request);

RegisterRequest.prototype.write = function (encoder, streamId) {
  var frameWriter = new FrameWriter(types.opcodes.register);
  frameWriter.writeStringList(this.events);
  return frameWriter.write(encoder.protocolVersion, streamId);
};

/**
 * Represents an AUTH_RESPONSE request
 * @param {Buffer} token
 * @constructor
 */
function AuthResponseRequest(token) {
  this.token = token;
}

util.inherits(AuthResponseRequest, Request);

AuthResponseRequest.prototype.write = function (encoder, streamId) {
  var frameWriter = new FrameWriter(types.opcodes.authResponse);
  frameWriter.writeBytes(this.token);
  return frameWriter.write(encoder.protocolVersion, streamId);
};

/**
 * Represents a protocol v1 CREDENTIALS request message
 * @constructor
 */
function CredentialsRequest(username, password) {
  this.username = username;
  this.password = password;
}

util.inherits(CredentialsRequest, Request);

CredentialsRequest.prototype.write = function (encoder, streamId) {
  var frameWriter = new FrameWriter(types.opcodes.credentials);
  frameWriter.writeStringMap({ username:this.username, password:this.password });
  return frameWriter.write(encoder.protocolVersion, streamId);
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
  this.hints = options.hints || utils.emptyArray;
}

util.inherits(BatchRequest, Request);

BatchRequest.prototype.write = function (encoder, streamId) {
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
    if (item.queryId) {
      // Contains prepared queries
      frameWriter.writeByte(1);
      frameWriter.writeShortBytes(item.queryId);
      hints = item.meta.columns.map(function (c) { return c.type; });
      var paramsInfo = utils.adaptNamedParamsPrepared(params, item.meta.columns);
      params = paramsInfo.params;
    }
    else {
      // Contains string queries
      frameWriter.writeByte(0);
      frameWriter.writeLString(item.query);
    }
    frameWriter.writeShort(params.length);
    params.forEach(function (param, paramIndex) {
      frameWriter.writeBytes(encoder.encode(param, hints ? hints[paramIndex] : null));
    });
  }, this);
  frameWriter.writeShort(this.options.consistency);
  if (types.protocolVersion.supportsTimestamp(encoder.protocolVersion)) {
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
  return frameWriter.write(encoder.protocolVersion, streamId, headerFlags);
};

BatchRequest.prototype.clone = function () {
  return new BatchRequest(this.queries, this.options);
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