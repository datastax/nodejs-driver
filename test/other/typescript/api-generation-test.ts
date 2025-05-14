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

import { auth, concurrent, errors, geometry, mapping, metadata, metrics, policies, tracker, types, graph } from "cassandra-driver";
import * as root from "cassandra-driver";

// import graph = datastax.graph;

let counter:number = 0;

/**
 * Should be executed to output a ts file:
 * - pushd test/other/typescript/
 * - npm install
 * - tsc -p .
 */
export function generate(): void {

  console.log(`/*
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
  

  
import { auth, concurrent, errors, datastax, mapping, geometry, metadata, metrics, policies, tracker, types } from "../../../index";
import * as root from "../../../index";

import graph = datastax.graph;

export async function generatedFn() {
  let n:number;
  let s:string;
  let o:object;
  let f:Function;
`);

  printClasses(root, 'root', new Set([ 'Encoder' ]));
  printObjects(root, 'root', new Set([ 'token' ]));

  printClasses(auth, 'auth', new Set(['NoAuthProvider']));
  printClasses(errors, 'errors');
  printFunctions(concurrent, 'concurrent');
  printClasses(concurrent, 'concurrent');
  printClasses(metadata, 'metadata');
  printClasses(metrics, 'metrics');
  printClasses(tracker, 'tracker');
  printClasses(geometry, 'geometry', new Set(['Geometry']));
  printClasses(graph, 'graph', new Set(['UdtGraphWrapper', 'GraphTypeWrapper']));

  // types
  printEnum(types.dataTypes, 'types.dataTypes');
  printEnum(types.consistencies, 'types.consistencies');
  printEnum(types.protocolVersion, 'types.protocolVersion');
  printEnum(types.distance, 'types.distance');
  printEnum(types.responseErrorCodes, 'types.responseErrorCodes');
  console.log(`  o = types.unset;\n\n`);
  printClasses(types, 'types', new Set([ 'TimeoutError', 'DriverError', 'FrameHeader' ]));

  // policies
  printClasses(policies.addressResolution, 'policies.addressResolution');
  printClasses(policies.loadBalancing, 'policies.loadBalancing');
  printClasses(policies.reconnection, 'policies.reconnection');
  printClasses(policies.retry, 'policies.retry');
  printFunctions(policies, 'policies');

  // mapping
  printClasses(mapping, 'mapping');
  printFunctions(mapping.q, 'mapping.q');

  console.log('\n}\n');
}

function printEnum(enumObject:{ [key: string]: any }, name: string): void {
  console.log(`  // ${name} enum values`);

  Object.keys(enumObject)
    .filter(k => typeof enumObject[k] === 'number')
    .forEach(k => {
      console.log(`  n = ${name}.${k};`);
    });
  console.log();
}

/**
 * Prints classes and interfaces
 */
function printClasses(ns:{ [key: string]: any }, namespaceString: string, except: Set<string> = new Set()): void {
  console.log(`  // ${namespaceString} classes and interfaces`);

  Object.keys(ns)
    .filter(k => typeof ns[k] === 'function' && k[0].toUpperCase() === k[0] && !except.has(k))
    .forEach(k => {
      console.log(`  let c${id()}: ${namespaceString}.${k};`);
    });
  console.log();
}

/**
 * Prints static functions
 */
function printFunctions(ns:{ [key: string]: any }, namespaceString: string, except: Set<string> = new Set()): void {
  console.log(`  // ${namespaceString} static functions`);

  Object.keys(ns)
    .filter(k => typeof ns[k] === 'function' && k[0].toLowerCase() === k[0] && !except.has(k))
    .forEach(k => {
      console.log(`  f = ${namespaceString}.${k};`);
    });
  console.log();
}

/**
 * Prints static functions
 */
function printObjects(ns:{ [key: string]: any }, namespaceString: string, except: Set<string> = new Set()): void {
  console.log(`  // ${namespaceString} namespaces/objects`);

  Object.keys(ns)
    .filter(k => typeof ns[k] === 'object' && k[0].toLowerCase() === k[0] && !except.has(k))
    .forEach(k => {
      console.log(`  o = ${namespaceString}.${k};`);
    });
  console.log();
}

function id(): number {
  return ++counter;
}