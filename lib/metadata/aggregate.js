"use strict";

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
}

module.exports = Aggregate;