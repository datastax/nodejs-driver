/**
 * Copyright (C) 2016 DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
var assert = require("assert");
var path = require("path");
var fs = require("fs");

var licenseHeader = "/**\n\
 * Copyright (C) 2016 DataStax, Inc.\n\
 *\n\
 * Please see the license for details:\n\
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms\n\
 */\n";

describe('All source files', function() {
  it('should start with license header', function () {
    var root = path.normalize(path.join(__dirname, '../../'));
    // Files to capture and validate header on.
    var candidateRE = /.*\.(js)$/;
    // List of directories to ignore, this may not be comprehensive depending on your local workspace.
    var dirsToIgnoreRE = /(node_modules)|(.git)|(.idea)|(coverage)|(out)/;
    var validateLicenses = function(dir) {
      fs.readdirSync(dir).forEach(function(file) {
        var filePath = path.join(dir, file);
        if(fs.statSync(filePath).isDirectory() && !file.match(dirsToIgnoreRE)) {
          validateLicenses(filePath);
        } else if(file.match(candidateRE)) {
          var data = fs.readFileSync(filePath, 'utf8');
          assert.ok(data.length >= licenseHeader.length,
            filePath + ' does not contain license header, contents:\n' + data);
          var dataHeader = data.substring(0, licenseHeader.length);
          assert.strictEqual(dataHeader, licenseHeader,
            'Beginning of ' + filePath + ' does not start with license header.');
        }
      });
    };
    validateLicenses(root);
  });
});