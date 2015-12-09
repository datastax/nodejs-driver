"use strict";
var util = require('util');
var events = require('events');
/**
 * Creates a new SchemaFunction.
 * @classdesc Describes a CQL function.
 * @alias module:metadata~SchemaFunction
 * @constructor
 */
function SchemaFunction() {
  /**
   * Name of the cql function.
   * @type {String}
   */
  this.name = null;
  /**
   * Name of the keyspace where the cql function is declared.
   */
  this.keyspaceName = null;
  /**
   * Signature of the function.
   * @type {Array.<String>}
   */
  this.signature = null;
  /**
   * List of the function argument names.
   * @type {Array.<String>}
   */
  this.argumentNames = null;
  /**
   * List of the function argument types.
   * @type {Array.<{code, info}>}
   */
  this.argumentTypes = null;
  /**
   * Body of the function.
   * @type {String}
   */
  this.body = null;
  /**
   * Determines if the function is called when the input is null.
   * @type {Boolean}
   */
  this.calledOnNullInput = null;
  /**
   * Name of the programming language, for example: java, javascript, ...
   * @type {String}
   */
  this.language = null;
  /**
   * Type of the return value.
   * @type {{code: number, info: (Object|Array|null)}}
   */
  this.returnType = null;
}

module.exports = SchemaFunction;