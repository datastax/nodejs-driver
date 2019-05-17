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

"use strict";

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