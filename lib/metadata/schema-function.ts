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

import type { DataTypeInfo } from "../encoder";


/**
 * @classdesc Describes a CQL function.
 * @alias module:metadata~SchemaFunction
*/
class SchemaFunction {
  /**
   * Name of the cql function.
   * @type {String}
   */
  name: string;
  /**
   * Name of the keyspace where the cql function is declared.
   */
  keyspaceName: string;
  /**
   * Signature of the function.
   * @type {Array.<String>}
   */
  signature: Array<string>;
  /**
   * List of the function argument names.
   * @type {Array.<String>}
   */
  argumentNames: Array<string>;
  /**
   * List of the function argument types.
   * @type {Array.<{code, info}>}
   */
  argumentTypes: Array<DataTypeInfo>;
  /**
   * Body of the function.
   * @type {String}
   */
  body: string;
  /**
   * Determines if the function is called when the input is null.
   * @type {Boolean}
   */
  calledOnNullInput: boolean;
  /**
   * Name of the programming language, for example: java, javascript, ...
   * @type {String}
   */
  language: string;

  // Was exposed as string, but I believe it should be DataTypeInfo
  /**
   * Type of the return value.
   * @type {DataTypeInfo}
   */
  returnType: DataTypeInfo;

  //TODO: not exposed. I believe it should be.
  /**
   * Indicates whether or not this function is deterministic.  This means that
   * given a particular input, the function will always produce the same output.
   * @type {Boolean}
   */
  deterministic: boolean;
  //TODO: not exposed. I believe it should be.
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
  monotonic: boolean;
  //TODO: not exposed. I believe it should be.
  /**
   * The argument names that the function is monotonic on.
   *
   * If {@link monotonic} is true, this will return all argument names.
   * Otherwise, this will return either one argument or an empty array.
   * @type {Array.<String>}
   */
  monotonicOn: Array<string>;
  /**
   * Creates a new SchemaFunction.
   * @internal
   * @alias module:metadata~SchemaFunction
   * @constructor
   */
  constructor() {
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
}

export default SchemaFunction;