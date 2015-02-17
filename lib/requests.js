var async = require('async');
var events = require('events');
var util = require('util');

var FrameWriter = require('./writers').FrameWriter;
var types = require('./types');
var utils = require('./utils');

/**
 *  Options for the execution of the query / prepared statement
 */
var queryFlag = {
  values:                 0x01,
  skipMetadata:           0x02,
  pageSize:               0x04,
  withPagingState:        0x08,
  withSerialConsistency:  0x10
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
  options = options || {};
  this.consistency = options.consistency || types.consistencies.one;
  this.fetchSize = options.fetchSize;
  this.pageState = options.pageState;
  this.hints = options.hints || [];
}

util.inherits(ExecuteRequest, Request);

ExecuteRequest.prototype.write = function (encoder) {
  //v1: <queryId>
  //      <n><value_1>....<value_n><consistency>
  //v2: <queryId>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  var frameWriter = new FrameWriter(types.opcodes.execute);
  frameWriter.writeShortBytes(this.queryId);
  this.writeQueryParameters(frameWriter, encoder);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Writes v1 and v2 execute query parameters
 * @param {FrameWriter} frameWriter
 * @param {Encoder} encoder
 */
ExecuteRequest.prototype.writeQueryParameters = function (frameWriter, encoder) {
  //v1: <n><value_1>....<value_n><consistency>
  //v2: <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  if (this.version > 1) {
    var flags = 0;
    flags += (this.params && this.params.length) ? queryFlag.values : 0;
    //only supply page size when there is no page state
    flags += (this.fetchSize > 0) ? queryFlag.pageSize : 0;
    flags += this.pageState ? queryFlag.withPagingState : 0;
    frameWriter.writeShort(this.consistency);
    frameWriter.writeByte(flags);
  }

  if (this.params && this.params.length) {
    frameWriter.writeShort(this.params.length);
    for (var i = 0; i < this.params.length; i++) {
      frameWriter.writeBytes(encoder.encode(this.params[i], this.hints[i]));
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
  if (this.fetchSize > 0) {
    frameWriter.writeInt(this.fetchSize);
  }
  if (this.pageState) {
    frameWriter.writeBytes(this.pageState);
  }
};

function QueryRequest(query, params, options) {
  this.streamId = null;
  this.query = query;
  this.params = params;
  options = options || {};
  this.consistency = options.consistency || types.consistencies.one;
  this.fetchSize = options.fetchSize;
  this.pageState = options.pageState;
  this.hints = options.hints || [];
}

util.inherits(QueryRequest, ExecuteRequest);

QueryRequest.prototype.write = function (encoder) {
  //v1: <query><consistency>
  //v2: <query>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  var frameWriter = new FrameWriter(types.opcodes.query);
  frameWriter.writeLString(this.query);
  if (this.version === 1) {
    frameWriter.writeShort(this.consistency);
  }
  else {
    //Use the same fields as the execute writer
    this.writeQueryParameters(frameWriter, encoder);
  }
  return frameWriter.write(this.version, this.streamId);
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
  var frameWriter = new FrameWriter(types.opcodes.credentials, this.streamId);
  frameWriter.writeStringMap({username:this.username, password:this.password});
  return frameWriter.write(this.version, this.streamId);
};

/**
 *
 * Writes a batch request
 * @param {Array.<{query, params, [info]}>} queries Array of objects with the properties query and params
 * @param {Object} options
 * @constructor
 */
function BatchRequest(queries, options) {
  this.queries = queries;
  this.type = options.logged ? 0 : 1;
  this.type = options.counter ? 2 : this.type;
  this.consistency = options.consistency;
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
      hints = utils.parseColumnDefinitions(item.info.meta.columns);
      var paramsInfo = utils.adaptNamedParams(params, item.info.meta.columns);
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

  frameWriter.writeShort(this.consistency);
  return frameWriter.write(this.version, this.streamId);
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