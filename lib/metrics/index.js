'use strict';

const ClientMetrics = require('./client-metrics');
const DefaultMetrics = require('./default-metrics');

/**
 * The <code>metrics</code> module contains interfaces and implementations used by the driver to expose
 * measurements of its internal behavior and of the server as seen from the driver side.
 * @module metrics
 */

module.exports = { ClientMetrics, DefaultMetrics };