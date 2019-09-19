/**
 * Copyright DataStax, Inc.
 *
 * Please see the license for details:
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

import { auth, concurrent, errors, geometry, graph, mapping, metadata, metrics, policies, tracker, types } from "../../../index";
import * as root from "../../../index";

let counter:number = 0;

/**
 * Should be executed to output a ts file:
 * - pushd test/unit/typescript/
 * - tsc -p .
 * - node -e "require('./api-generation-test').generate()" > generated.ts
 * - tsc generated.ts
 * - popd
 */
export function generate(): void {

  console.log(`
'use strict';
  
import { auth, concurrent, errors, mapping, geometry, graph, metadata, metrics, policies, tracker, types } from "../../../index";
import * as root from "../../../index";

export async function generatedFn() {
  let n:number;
  let s:string;
  let o:object;
  let f:Function;
`);

  printClasses(root, 'root', new Set([ 'Encoder' ]));
  printObjects(root, 'root', new Set([ 'token' ]));

  printClasses(auth, 'auth');
  printClasses(errors, 'errors');
  printFunctions(concurrent, 'concurrent');
  printClasses(concurrent, 'concurrent');
  printClasses(metadata, 'metadata');
  printClasses(metrics, 'metrics');
  printClasses(tracker, 'tracker');
  printClasses(geometry, 'geometry');
  printClasses(graph, 'graph');

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