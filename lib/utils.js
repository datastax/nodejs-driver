/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var util = require('util');
var types = require('cassandra-driver').types;
var consistencyNames;

exports.extend = function extend(target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
};

/**
 * Gets the name in upper case of the consistency level.
 * @param {Number} consistency
 */
exports.getConsistencyName = function getConsistencyName(consistency) {
  if (consistency == undefined) {
    //null or undefined => undefined
    return undefined;
  }
  loadConsistencyNames();
  var name = consistencyNames[consistency];
  if (!name) {
    throw new Error(util.format(
      'Consistency %s not found, use values defined as properties in types.consistencies object', consistency
    ));
  }
  return name;
};

function loadConsistencyNames() {
  if (consistencyNames) {
    return;
  }
  consistencyNames = {};
  var propertyNames = Object.keys(types.consistencies);
  for (var i = 0; i < propertyNames.length; i++) {
    var name = propertyNames[i];
    consistencyNames[types.consistencies[name]] = name.toUpperCase();
  }
  //Using java constants naming conventions
  consistencyNames[types.consistencies.localQuorum]  = 'LOCAL_QUORUM';
  consistencyNames[types.consistencies.eachQuorum]   = 'EACH_QUORUM';
  consistencyNames[types.consistencies.localSerial]  = 'LOCAL_SERIAL';
  consistencyNames[types.consistencies.localOne]     = 'LOCAL_ONE';
}