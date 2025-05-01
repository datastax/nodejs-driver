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

import { types, Client } from "../../../";
const Uuid = types.Uuid;
type Uuid = types.Uuid;
const TimeUuid = types.TimeUuid;
type TimeUuid = types.TimeUuid;
const Long = types.Long;
type Long = types.Long;
const BigDecimal = types.BigDecimal;
type BigDecimal = types.BigDecimal;
const InetAddress = types.InetAddress;
type InetAddress = types.InetAddress;
const Tuple = types.Tuple;
type Tuple = types.Tuple;
import ResultSet = types.ResultSet;
import Row = types.Row;

/*
 * TypeScript definitions compilation tests for types module.
 */

async function myTest(): Promise<void> {
  let id:Uuid;
  let tid:TimeUuid;
  let b: boolean;
  let s: string;
  let buffer: Buffer;
  let rs: ResultSet;

  types.protocolVersion.isSupported(types.protocolVersion.v4);

  id = Uuid.random();

  id = TimeUuid.now();
  tid = TimeUuid.now();

  b = id.equals(tid);

  TimeUuid.now((err, value) => value.getDate() === new Date());

  let dec:BigDecimal = BigDecimal.fromString('123');
  dec = dec.add(new BigDecimal(10, 4)).subtract(dec);

  let address: InetAddress = InetAddress.fromString('127.0.0.1');
  s = address.toString();
  buffer = address.getBuffer();

  let tuple = Tuple.fromArray([ 'a', 1]);
  b = tuple !== new Tuple('a', 1);

  // Long is an external dependency
  // Use static methods
  let long: Long = Long.fromNumber(2).div(Long.fromString('a')).toUnsigned();
  // Use as an instance
  long.div(long);
  // Use constructor
  long = new Long(1, 2);

  const client = new Client({
    contactPoints: ['host1'],
    localDataCenter: 'dc1'
  });

  rs = await client.execute('SELECT * FROM ks1.table1');
  // Test iteration
  for (const row of rs) {
    // Check is of type Row
    const r: Row = row;
  }

  rs = await client.execute('SELECT * FROM ks1.table1');
  // Test async iteration
  for await (const row of rs) {
    // Check is of type Row
    const r: Row = row;
  }
}