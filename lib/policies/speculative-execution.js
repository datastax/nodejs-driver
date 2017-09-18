'use strict';

var util = require('util');
var errors = require('../errors');

/** @module policies/speculativeExecution */

/**
 * @classdesc
 * The policy that decides if the driver will send speculative queries to the next hosts when the current host takes too
 * long to respond.
 * <p>Note that only idempotent statements will be speculatively retried.</p>
 * @constructor
 * @abstract
 */
function SpeculativeExecutionPolicy() {
  
}

/**
 * Initialization method that gets invoked on Client startup.
 * @param {Client} client
 * @abstract
 */
SpeculativeExecutionPolicy.prototype.init = function (client) {

};

/**
 * Gets invoked at client shutdown, giving the opportunity to the implementor to perform cleanup.
 * @abstract
 */
SpeculativeExecutionPolicy.prototype.shutdown = function () {

};

/**
 * Gets the plan to use for a new query.
 * Returns an object with a <code>nextExecution()</code> method, which returns a positive number representing the
 * amount of milliseconds to delay the next execution or a non-negative number to avoid further executions.
 * @param {String} keyspace The currently logged keyspace.
 * @param {String|Array<String>} queryInfo The query, or queries in the case of batches, for which to build a plan.
 * @return {{nextExecution: function}}
 * @abstract
 */
SpeculativeExecutionPolicy.prototype.newPlan = function (keyspace, queryInfo) {
  throw new Error('You must implement newPlan() method in the SpeculativeExecutionPolicy');
};

/**
 * Creates a new instance of NoSpeculativeExecutionPolicy.
 * @classdesc
 * A {@link SpeculativeExecutionPolicy} that never schedules speculative executions.
 * @constructor
 * @extends {SpeculativeExecutionPolicy}
 */
function NoSpeculativeExecutionPolicy() {
  this._plan = {
    nextExecution: function () {
      return -1;
    }
  };
}

util.inherits(NoSpeculativeExecutionPolicy, SpeculativeExecutionPolicy);

NoSpeculativeExecutionPolicy.prototype.newPlan = function () {
  return this._plan;
};


/**
 * Creates a new instance of ConstantSpeculativeExecutionPolicy.
 * @classdesc
 * A {@link SpeculativeExecutionPolicy} that schedules a given number of speculative executions,
 * separated by a fixed delay.
 * @constructor
 * @param {Number} delay The delay between each speculative execution.
 * @param {Number} maxSpeculativeExecutions The amount of speculative executions that should be scheduled after the
 * initial execution. Must be strictly positive.
 * @extends {SpeculativeExecutionPolicy}
 */
function ConstantSpeculativeExecutionPolicy(delay, maxSpeculativeExecutions) {
  if (!(delay >= 0)) {
    throw new errors.ArgumentError('delay must be a positive number or zero');
  }
  if (!(maxSpeculativeExecutions > 0)) {
    throw new errors.ArgumentError('maxSpeculativeExecutions must be a positive number');
  }
  this._delay = delay;
  this._maxSpeculativeExecutions = maxSpeculativeExecutions;
}

util.inherits(ConstantSpeculativeExecutionPolicy, SpeculativeExecutionPolicy);

ConstantSpeculativeExecutionPolicy.prototype.newPlan = function () {
  var executions = 0;
  var self = this;
  return {
    nextExecution: function () {
      if (executions++ < self._maxSpeculativeExecutions) {
        return self._delay;
      }
      return -1;
    }
  };
};

exports.NoSpeculativeExecutionPolicy = NoSpeculativeExecutionPolicy;
exports.SpeculativeExecutionPolicy = SpeculativeExecutionPolicy;
exports.ConstantSpeculativeExecutionPolicy = ConstantSpeculativeExecutionPolicy;