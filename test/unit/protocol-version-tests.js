/**
 * Copyright (C) DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    host.cassandraVersion = hostVersions[i];
    hosts.push(host);
  }

  return function (done) {
    const highestVersion = protocolVersion.getHighestCommon(mockConnection, hosts);
    assert.strictEqual(highestVersion, expectedProtocolVersion);
    done();
  };

}