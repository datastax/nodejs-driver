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
const assert = require("assert");
const path = require("path");
const fs = require("fs");

const licenseHeaderRegex = new RegExp(
  `/\\*
 \\* Copyright DataStax, Inc\\.
 \\*
 \\* Licensed under the Apache License, Version 2\\.0 \\(the "License"\\);
 \\* you may not use this file except in compliance with the License\\.
 \\* You may obtain a copy of the License at
 \\*
 \\* http://www\\.apache\\.org/licenses/LICENSE-2\\.0
 \\*
 \\* Unless required by applicable law or agreed to in writing, software
 \\* distributed under the License is distributed on an "AS IS" BASIS,
 \\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied\\.
 \\* See the License for the specific language governing permissions and
 \\* limitations under the License\\.
 \\*/`
);

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