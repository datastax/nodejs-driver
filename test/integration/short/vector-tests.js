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
const assert = require('assert');
const util = require('util');
const helper = require('../../test-helper.js');
const Client = require('../../../lib/client.js');
const ExecutionProfile = require('../../../lib/execution-profile.js').ExecutionProfile;

const types = require('../../../lib/types/index.js');
const utils = require('../../../lib/utils.js');
const errors = require('../../../lib/errors.js');
const vit = helper.vit;
const vdescribe = helper.vdescribe;
const numericTests = require('./numeric-tests.js');
const pagingTests = require('./paging-tests.js');

vdescribe('5.0.0', 'Vector tests', function () {
    this.timeout(120000);
    describe('#execute with vectors', function () {
        const keyspace = helper.getRandomName('ks');
        const table = keyspace + '.' + helper.getRandomName('table');
        const createTableCql = `CREATE TABLE ${table} (id uuid PRIMARY KEY, v1 vector<float>)`;
    
        const setupInfo = helper.setup(1, {
          keyspace: keyspace,
          queries: [ createTableCql ]
        });
        it('should insert and select vectors', function(done){
            const client = setupInfo.client;
            // if client undefined, raise error
            if(!client) return done(new Error('client is not defined'));
            const id = types.types.Uuid.random();
            const v1 = new types.Vector([1.1, 2.2, 3.3]);
            done();
        });
    });
});