var async = require('async');
var events = require('events');
var util = require('util');

var encoder = require('./encoder.js');
var FrameWriter = require('./writers.js').FrameWriter;
var types = require('./types');
var utils = require('./utils.js');

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

QueryRequest.prototype.write = function () {
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
    this.writeQueryParameters(frameWriter);
  }
  return frameWriter.write(this.version, this.streamId);
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

ExecuteRequest.prototype.write = function () {
  //v1: <queryId>
  //      <n><value_1>....<value_n><consistency>
  //v2: <queryId>
  //      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  var frameWriter = new FrameWriter(types.opcodes.execute);
  frameWriter.writeShortBytes(this.queryId);
  this.writeQueryParameters(frameWriter);
  return frameWriter.write(this.version, this.streamId);
};

/**
 * Writes v1 and v2 execute query parameters
 * @param {FrameWriter} frameWriter
 */
ExecuteRequest.prototype.writeQueryParameters = function (frameWriter) {
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



function PrepareRequest(query) {
  this.streamId = null;
  this.query = query;
}

PrepareRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.prepare);
  frameWriter.writeLString(this.query);
  return frameWriter.write(this.version, this.streamId);
};

function StartupRequest(cqlVersion) {
  this.cqlVersion = cqlVersion || '3.0.0';
  this.streamId = null;
}

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

CredentialsRequest.prototype.write = function () {
  var frameWriter = new FrameWriter(types.opcodes.credentials, this.streamId);
  frameWriter.writeStringMap({username:this.username, password:this.password});
  return frameWriter.write(this.version, this.streamId);
};

/**
 *
 * Writes a batch request
 * @param {Array} queries Array of objects with the properties query and params
 * @param {Number} consistency
 * @param {Object} options
 * @constructor
 */
function BatchRequest(queries, consistency, options) {
  this.queries = queries;
  this.type = options.logged ? 0 : 1;
  this.type = options.counter ? 2 : this.type;
  this.consistency = consistency;
  this.streamId = null;
  this.hints = options.hints || [];
}

BatchRequest.prototype.write = function () {
  if (!this.queries || !(this.queries.length > 0)) {
    throw new TypeError(util.format('Invalid queries provided %s', this.queries));
  }
  var frameWriter = new FrameWriter(types.opcodes.batch);
  frameWriter.writeByte(this.type);
  frameWriter.writeShort(this.queries.length);
  var self = this;
  this.queries.forEach(function (item, i) {
    if (!item) return;
    var query = item.query;
    if (typeof item === 'string') {
      query = item;
    }
    if (!query) {
      throw new TypeError(util.format('Invalid query at index %d', i));
    }
    //kind flag for not prepared
    frameWriter.writeByte(0);
    frameWriter.writeLString(query);
    var params = item.params || [];
    var hints = self.hints[i];
    frameWriter.writeShort(params.length);
    params.forEach(function (param, paramIndex) {
      frameWriter.writeBytes(encoder.encode(param, hints ? hints[paramIndex] : null));
    }, this);
  }, this);

  frameWriter.writeShort(this.consistency);
  return frameWriter.write(this.version, this.streamId);
};


exports.AuthResponseRequest = AuthResponseRequest;
exports.CredentialsRequest = CredentialsRequest;
exports.PrepareRequest = PrepareRequest;
exports.QueryRequest = QueryRequest;
exports.RegisterRequest = RegisterRequest;
exports.StartupRequest = StartupRequest;
exports.ExecuteRequest = ExecuteRequest;
exports.BatchRequest = BatchRequest;