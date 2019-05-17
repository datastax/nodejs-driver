/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';

/**
 * Name of the doclets and the maximum amount of occurrences.
 * @private
 */
var filterDoclets = {
  'module:types': 1,
  'Client': 1,
  'ClientOptions': 0
};
var importPropDoclets = {
  'DseClientOptions': 'ClientOptions'
};
var filtered = {};
var importedProps = {};

exports.handlers = {
  newDoclet: function (e) {
    var key = e.doclet.longname;
    if (!key) {
      return;
    }
    if (importPropDoclets[key]) {
      // doclet to import must come after
      importedProps[importPropDoclets[key]] = e.doclet;
    }
    else if (importedProps[key]) {
      var props = importedProps[key].properties;
      props.unshift.apply(props, e.doclet.properties);
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