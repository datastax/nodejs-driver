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
const protocolVersion = require('../../lib/types/').protocolVersion;
const Host = require('../../lib/host').Host;
const options = require('../../lib/client-options').defaultOptions();

describe('protocolVersion', function () {
  describe('#getHighestCommon()', function () {
    it('should downgrade to protocol v3 with versions 3.0 & 2.1', testWithHosts(['3.0.13', '2.1.17'], 3));
    it('should downgrade to protocol v3 with versions 2.2 & 2.1', testWithHosts(['2.2.11', '2.1.17'], 3));
    it('should downgrade to protocol v2 with versions 2.2 & 2.0', testWithHosts(['2.2.11', '2.0.17'], 2));
    it('should downgrade to protocol v1 with versions 2.2 & 1.2', testWithHosts(['2.2.11', '1.2.19'], 1));
    it('should downgrade to protocol v2 with versions 2.1 & 2.0', testWithHosts(['2.1.17', '2.0.17'], 2));
    it('should downgrade to protocol v1 with versions 2.1 & 1.2', testWithHosts(['2.1.17', '1.2.19'], 1));
    it('should downgrade to protocol v1 with versions 2.0 & 1.2', testWithHosts(['2.0.17', '1.2.19'], 1));
    // no need to downgrade since both support protocol V4.
    it('should not downgrade with versions 3.0 & 2.2', testWithHosts(['3.0.13', '3.0.11', '2.2.9'], 4));
    // can't downgrade because C* 3.0 does not support protocol V2.
    it('should not downgrade with versions 3.0 & 2.0', testWithHosts(['3.0.13', '2.0.17'], 4));
    // can't downgrade because C* 3.0 does not support protocol V1.
    it('should not downgrade with versions 3.0 & 1.2', testWithHosts(['3.0.13', '1.2.19'], 4));
    // since connection uses protocol v1, we should stick with v1 even if highest common is greater.
    it('should use connection protocol version even if highest common is greater', testWithHosts(['1.2.19', '3.0.13'], 1));
    // disregard connection protocol version if highest common is lower, this should not happen in practice.
    // this is technically covered by other tests, but good to validate explicitly.
    it('should use highest common even if connection protocol version is greater', testWithHosts(['2.1.17', '2.0.17'], 2, 4));
    // DSE specific tests
    // should downgrade when detects older dse version that doesn't support DSE protocol versions so falls back on C* version.
    it('should downgrade to protocol v4 with dse versions 5.1 & 5.0', testWithHosts([['3.11.0', '5.1.5'], ['3.10.0', '5.0.11']], 4));
    it('should downgrade to protocol v3 with dse versions 5.1 & 4.8', testWithHosts([['3.11.0', '5.1.5'], ['2.1.17', '4.8.12']], 3));
    // DSE nodes should interop with C* nodes
    it('should downgrade to protocol v4 with dse version 5.1 & cassandra 3.11', testWithHosts([['3.11.0', '5.1.5'], '3.11.0'], 4));
    // can't downgrade because DSE 5.0+ (C* 3.0+) does not support protocol V2.
    it('should not downgrade with dse versions 5.1 & 4.6', testWithHosts([['3.11.0', '5.1.5'], ['2.0.17', '4.6.14']], protocolVersion.dseV1));
    // since connection uses protocol v4, we should stick with v4 even if highest common is a dse protocol version.
    it('should use connection protocol version even if highest common is a dse protocol version', testWithHosts([['3.11.0', '5.1.5']], 4, 4));
  });
});

function testWithHosts(hostVersions, expectedProtocolVersion, connectionProtocolVersion) {
  const mockConnection = {
    address: '192.1.1.0',
    port: 9042,
    protocolVersion: connectionProtocolVersion || protocolVersion.maxSupported,
  };
  const hosts = [];
  for (let i = 0; i < hostVersions.length; i++) {
    const host = new Host('192.1.1.' + i, protocolVersion.maxSupported, options);
    if (typeof hostVersions[i] === 'string') {
      host.cassandraVersion = hostVersions[i];
    } else {
      host.cassandraVersion = hostVersions[i][0];
      host.dseVersion = hostVersions[i][1];
    }
    hosts.push(host);
  }

  return function (done) {
    const highestVersion = protocolVersion.getHighestCommon(mockConnection, hosts);
    assert.strictEqual(highestVersion, expectedProtocolVersion);
    done();
  };

}