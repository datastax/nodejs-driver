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
import { assert } from "chai";
import { checkServerIdentity } from "../../../lib/datastax/cloud/index";

'use strict';
describe('checkServerIdentity()', () => {
  const port = 32598;
  const sampleHostName = 'host1.datastax.com';
  const wrongNames = [
    'host1.datastax.org', //wrong tld
    'abc.datastax.com', // wrong subdomain
    'abc.nodejs.org', // wrong domain
    'nodejs.org', // wrong domain
    '*.nodejs.org', // wrong domain
    '*.datastax.org' // wrong domain
  ];

  it('should allow certificate common name matching the host name', () => {
    [
      'host1.datastax.com',
      'abc.datastax.com',
      'abc.nodejs.org',
      'nodejs.org',
      'ef5d2e2d-5cc1-4abc-ad58-4547d7f62c90-us-east-1.db.apollo.datastax.com'
    ].forEach(hostName => {
      assert.isUndefined(checkServerIdentity({ subject: { CN: hostName }}, `${hostName}:${port}`));
    });
  });

  it('should not match incorrect domains in the common name', () => {
    wrongNames.forEach(name => {
      const err = checkServerIdentity({ subject: { CN: name }}, `${sampleHostName}:${port}`);
      assert.instanceOf(err, Error);
      assert.strictEqual(err.reason, `Host: ${sampleHostName} is not cert's CN/altnames: ${name} / undefined`);
      assert.ok(err.cert);
      assert.strictEqual(err.host, sampleHostName);
    });
  });

  it('should not match incorrect domains in the alt names', () => {
    wrongNames.forEach(name => {
      const err = checkServerIdentity({ subject: { CN: 'xyz.org' }, subjectaltname: name}, `${sampleHostName}:${port}`);
      assert.instanceOf(err, Error);
      assert.strictEqual(err.reason, `Host: ${sampleHostName} is not cert's CN/altnames: xyz.org / ${name}`);
      assert.ok(err.cert);
      assert.strictEqual(err.host, sampleHostName);
    });
  });

  it('should allow certificate common name with wildcard', () => {
    [
      [
        '*.datastax.com',
        [
          'host1.datastax.com', 'cec95af1-a06c-42e1-9e59-068bb9be11f7.datastax.com'
        ]
      ], [
        '*.db.apollo.datastax.com',
        [
          'ef5d2e2d-5cc1-4abc-ad58-4547d7f62c90-us-east-1.db.apollo.datastax.com'
        ]
      ]
    ].forEach(item => {
      const hostNames = item[1];
      hostNames.forEach(hostName =>
        assert.isUndefined(checkServerIdentity({ subject: { CN: item[0] }}, `${hostName}:${port}`)));
    });
  });

  it('should allow certificate alt names with wildcard', () => {
    [
      [
        'DNS:*.nodejs.org, DNS:nodejs.org',
        [
          'nodejs.org', 'www.nodejs.org'
        ]
      ],
      [
        'DNS:abc.nodejs.org, DNS:def.nodejs.org',
        [
          'abc.nodejs.org', 'def.nodejs.org'
        ]
      ],
      [
        'DNS:*.datastax.com, DNS:datastax.com, DNS:*.db.apollo.datastax.com',
        [
          'datastax.com', 'apollo.datastax.com', 'ef5d2e2d-5cc1-4abc-ad58-4547d7f62c90-us-east-1.db.apollo.datastax.com'
        ]
      ]
    ].forEach(item => {
      const hostNames = item[1];
      hostNames.forEach(hostName =>
        assert.isUndefined(
          checkServerIdentity({ subject: { CN: 'xyz.org' }, subjectaltname: item[0]}, `${hostName}:${port}`)));
    });
  });
});