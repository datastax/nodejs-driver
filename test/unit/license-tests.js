/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
'use strict';
const assert = require("assert");
const path = require("path");
const fs = require("fs");

const licenseHeaderRegex = new RegExp(
  '^\\/\\*\\*\n \\* Copyright (?:\\(C\\) )?(?:\\d{4}(?:\\-\\d{4})? )?DataStax, Inc\\.\\n \\*\\n' +
  ' \\* Please see the license for details:\\n' +
  ' \\* http://www.datastax.com/terms/datastax-dse-driver-license-terms');

describe('All source files', function() {
  this.timeout(5000);

  it('should start with license header', function () {
    // eslint-disable-next-line no-undef
    const root = path.normalize(path.join(__dirname, '../../'));
    // Files to capture and validate header on.
    const candidateRE = /.*\.(js)$/;
    // List of directories to ignore, this may not be comprehensive depending on your local workspace.
    const dirsToIgnoreRE = /(node_modules)|(\.git)|(\.idea)|(coverage)|(out)|(examples)/;

    function validateLicenses(dir) {
      fs.readdirSync(dir).forEach(function(file) {
        const filePath = path.join(dir, file);
        if (fs.statSync(filePath).isDirectory() && !file.match(dirsToIgnoreRE)) {
          validateLicenses(filePath);
        }
        else if (file.charAt(0) !== '.' && file.match(candidateRE) && file !== 'integer.js') {
          const data = fs.readFileSync(filePath, 'utf8');
          assert.ok(licenseHeaderRegex.test(data), 'Beginning of ' + filePath + ' does not start with license header.');
        }
      });
    }
    validateLicenses(root);
  });
});