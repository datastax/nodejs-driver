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
import LoadBalancingPolicy = policies.loadBalancing.LoadBalancingPolicy;
import TokenAwarePolicy = policies.loadBalancing.TokenAwarePolicy;

/*
 * TypeScript definitions compilation tests for policy module.
 */

function myTest(): void {
  let lbp:LoadBalancingPolicy;

  lbp = new policies.loadBalancing.DCAwareRoundRobinPolicy('dc1');
  lbp = new policies.loadBalancing.WhiteListPolicy(lbp, [ 'a', 'b', 'c' ]);
  lbp = new TokenAwarePolicy(lbp);
}