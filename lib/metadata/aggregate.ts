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


/**
 * Creates a new Aggregate.
 * @classdesc Describes a CQL aggregate.
 * @alias module:metadata~Aggregate
 * @constructor
 */
class Aggregate {
  /**
   * Name of the aggregate.
   * @type {String}
   * @internal
   */
  name: string;
  /**
   * Name of the keyspace where the aggregate is declared.
   */
  keyspaceName: string;
  /**
   * Signature of the aggregate.
   * @type {Array.<String>}
   */
  signature: Array<string>;
  /**
   * List of the CQL aggregate argument types.
   * @type {Array.<{code, info}>}
   */
  argumentTypes: Array<{ code: number, info?: (object | Array<any> | string) }>;
  /**
   * State Function.
   * @type {String}
   */
  stateFunction: string;
  /**
   * State Type.
   * @type {{code, info}}
   */
  stateType: { code: number, info?: (object | Array<any> | string) };
  /**
    * Final Function.
    * @type {String}
    */
  finalFunction: string;
  /** @internal */
  initConditionRaw: any;
  /**
   * Initial state value of this aggregate.
   * @type {String}
   */
  initCondition: string;
  //TODO: was exposed as a string.
  /**
   * Type of the return value.
   * @type {{code: number, info: (Object|Array|null)}}
   */
  returnType: { code: number, info?: (object | Array<any> | string) };
  /**
   * Indicates whether or not this aggregate is deterministic.  This means that
   * given a particular input, the aggregate will always produce the same output.
   * @type {Boolean}
   */
  //TODO: It was not exposed. I believe it should be.
  deterministic: boolean;
  /** @internal */
  constructor() {
    this.name = null;
    this.keyspaceName = null;
    this.signature = null;
    this.argumentTypes = null;
    this.stateFunction = null;
    this.stateType = null;
    this.finalFunction = null;
    this.initConditionRaw = null;
    this.initCondition = null;
    this.returnType = null;
    this.deterministic = null;
  }
}

export default Aggregate;