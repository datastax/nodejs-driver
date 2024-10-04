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
var assert = require('assert');
var util = require('util');
var helper = require('../../test-helper.js');
var Client = require('../../../lib/client.js');
var ExecutionProfile = require('../../../lib/execution-profile.js').ExecutionProfile;
var types = require('../../../index.js').types;
var utils = require('../../../lib/utils.js');
var errors = require('../../../lib/errors.js');
var vit = helper.vit;
var vdescribe = helper.vdescribe;
var numericTests = require('./numeric-tests.js');
var pagingTests = require('./paging-tests.js');
vdescribe('5.0.0', 'Vector tests TypeScript', function () {
    this.timeout(120000);
    describe('#execute with vectors', function () {
        var keyspace = helper.getRandomName('ks');
        var table = keyspace + '.' + helper.getRandomName('table');
        var createTableCql = "CREATE TABLE ".concat(table, " (id uuid PRIMARY KEY, v1 vector<float, 3>);");
        var setupInfo = helper.setup(1, {
            keyspace: keyspace,
            queries: [createTableCql]
        });
        it('should insert and select vectors', function (done) {
            var client = setupInfo.client;
            // if client undefined, raise error
            if (!client)
                return done(new Error('client is not defined'));
            var id = types.Uuid.random();
            var v1 = new Float32Array([1.1, 2.2, 3.3]);
            var query = "INSERT INTO ".concat(table, " (id, v1) VALUES (?, ?)");
            client.execute(query, [id, v1], { prepare: true }, function (err) {
                if (err)
                    return done(err);
                client.execute("SELECT v1 FROM ".concat(table, " WHERE id = ?"), [id], { prepare: true }, function (err, result) {
                    if (err)
                        return done(err);
                    var v1 = result.rows[0].v1;
                    assert.strictEqual(result.rows.length, 1);
                    assert.strictEqual(v1.length, 3);
                    assert.strictEqual(v1[0], v1[0]);
                    assert.strictEqual(v1[1], v1[1]);
                    assert.strictEqual(v1[2], v1[2]);
                    done();
                });
            });
        });
    });
});
