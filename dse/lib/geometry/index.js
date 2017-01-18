/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
/**
 * Geometry module.
 * <p>
 *   Contains the classes to represent the set of additional CQL types for geospatial data that come with
 *   DSE 5.0.
 * </p>
 * @module geometry
 */
exports.LineString = require('./line-string');
exports.Point = require('./point');
exports.Polygon = require('./polygon');