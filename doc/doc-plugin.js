'use strict';

/**
 * Name of the doclets and the maximum amount of occurrences.
 * @private
 */
var filterDoclets = {
  'module:types': 1
};
var filtered = {};

exports.handlers = {
  newDoclet: function (e) {
    var key = e.doclet.longname;
    if (!key) {
      return;
    }
    var maxLength = filterDoclets[key];
    if (maxLength === undefined ) {
      return;
    }
    filtered[key] = filtered[key] || 0;
    if (filtered[key]++ < maxLength) {
      return;
    }
    e.doclet.access = 'private';
  }
};