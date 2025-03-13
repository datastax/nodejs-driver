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
import assert from "assert";
import dns from "dns";
import * as addressResolution from "../../lib/policies/address-resolution";


const EC2MultiRegionTranslator = addressResolution.EC2MultiRegionTranslator;

describe('EC2MultiRegionTranslator', function () {
  this.timeout(10000);
  describe('#translate()', function () {
    it('should return the same address when it could not be resolved', function (done) {
      const t = new EC2MultiRegionTranslator();
      t.translate('192.0.2.1', 9042, function (endPoint) {
        assert.strictEqual(endPoint, '192.0.2.1:9042');
        done();
      });
    });
    it('should do a reverse and a forward dns lookup', function (done) {
      const t = new EC2MultiRegionTranslator();
      dns.lookup('datastax.com', function (err, address) {
        assert.ifError(err);
        assert.ok(address);
        t.translate(address, 9001, function (endPoint) {
          assert.strictEqual(endPoint, address + ':9001');
          done();
        });
      });
    });
  });
});
