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

import { auth, Client, policies, types } from "../../../index";

/*
 * TypeScript definitions compilation tests for Client class.
 */

const client = new Client({
  contactPoints: ['h1', 'h2'],
  localDataCenter: 'dc1',
  keyspace: 'ks1',
  authProvider: new auth.PlainTextAuthProvider('a', 'b')
});

client.connect()
  .then(() => {});

client.shutdown()
  .then(() => {});

const lbp = new policies.loadBalancing.DCAwareRoundRobinPolicy('dc1');
lbp.getDistance(null);

types.protocolVersion.isSupported(types.protocolVersion.v4);

types.Long.fromNumber(2).div(types.Long.fromString('a'));

types.TimeUuid.now();

types.TimeUuid.now((err, value) => value.getDate() === new Date());