/**
 * Copyright (C) 2016-2017 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

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
  /**
   * Indicates whether or not this function is deterministic.  This means that
   * given a particular input, the function will always produce the same output.
   * @type {Boolean}
   */
  this.deterministic = null;
  /**
   * Indicates whether or not this function is monotonic on all of its
   * arguments.  This means that it is either entirely non-increasing or
   * non-decreasing.  Even if the function is not monotonic on all of its
   * arguments, it's possible to specify that it is monotonic on one of
   * its arguments, meaning that partial applications of the function over
   * that argument will be monotonic.
   * 
   * Monotonicity is required to use the function in a GROUP BY clause.
   * @type {Boolean}
   */
  this.monotonic = null;
  /**
   * The argument names that the function is monotonic on.
   * 
   * If {@link monotonic} is true, this will return all argument names.
   * Otherwise, this will return either one argument or an empty array.
   * @type {Array.<String>}
   */
  this.monotonicOn = null;
}

module.exports = SchemaFunction;