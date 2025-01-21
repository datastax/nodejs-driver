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
'use strict';

/**
 * Creates a new Aggregate.
 * @classdesc Describes a CQL aggregate.
 * @alias module:metadata~Aggregate
 * @constructor
 */
function Aggregate() {
  /**
   * Name of the aggregate.
   * @type {String}
   */
  this.name = null;
  /**
   * Name of the keyspace where the aggregate is declared.
   */
  this.keyspaceName = null;
  /**
   * Signature of the aggregate.
   * @type {Array.<String>}
   */
  this.signature = null;
  /**
   * List of the CQL aggregate argument types.
   * @type {Array.<{code, info}>}
   */
  this.argumentTypes = null;
  /**
   * State Function.
   * @type {String}
   */
  this.stateFunction = null;
  /**
   * State Type.
   * @type {{code, info}}
   */
  this.stateType = null;
  /**
   * Final Function.
   * @type {String}
   */
  this.finalFunction = null;
  this.initConditionRaw = null;
  /**
   * Initial state value of this aggregate.
   * @type {String}
   */
  this.initCondition = null;
  /**
   * Type of the return value.
   * @type {{code: number, info: (Object|Array|null)}}
   */
  this.returnType = null;
  /**
   * Indicates whether or not this aggregate is deterministic.  This means that
   * given a particular input, the aggregate will always produce the same output.
   * @type {Boolean}
   */
  this.deterministic = null;
}

module.exports = Aggregate;