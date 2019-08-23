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
 * Geometry module.
 * <p>
 *   Contains the classes to represent the set of additional CQL types for geospatial data that come with
 *   DSE 5.0.
 * </p>
 * @module geometry
 */

exports.Geometry = require('./geometry');
exports.LineString = require('./line-string');
exports.Point = require('./point');
exports.Polygon = require('./polygon');