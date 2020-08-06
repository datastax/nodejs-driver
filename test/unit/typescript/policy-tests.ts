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

import { policies } from "../../../index";
import LoadBalancingPolicy = policies.loadBalancing.LoadBalancingPolicy;
import TokenAwarePolicy = policies.loadBalancing.TokenAwarePolicy;
import ReconnectionPolicy = policies.reconnection.ReconnectionPolicy;
import RetryPolicy = policies.retry.RetryPolicy;
import ConstantReconnectionPolicy = policies.reconnection.ConstantReconnectionPolicy;
import ExponentialReconnectionPolicy = policies.reconnection.ExponentialReconnectionPolicy;
import addressResolution = policies.addressResolution;

/*
 * TypeScript definitions compilation tests for policy module.
 */

function myTest(): void {
  let lbp:LoadBalancingPolicy;
  let rp:ReconnectionPolicy;
  let retryPolicy:RetryPolicy;

  lbp = new policies.loadBalancing.DCAwareRoundRobinPolicy('dc1');
  lbp = new policies.loadBalancing.AllowListPolicy(lbp, [ 'a', 'b', 'c' ]);
  // For backward compatibility only
  lbp = new policies.loadBalancing.WhiteListPolicy(lbp, [ 'a', 'b', 'c' ]);
  lbp = new TokenAwarePolicy(lbp);
  lbp.getOptions();

  // defaultLoadBalancingPolicy method should have an optional string parameter
  lbp = policies.defaultLoadBalancingPolicy('dc1');
  lbp = policies.defaultLoadBalancingPolicy();

  rp = new ConstantReconnectionPolicy(10);
  rp = new ExponentialReconnectionPolicy(1000, 60 * 1000);
  rp.getOptions();

  retryPolicy = new RetryPolicy();

  let ar: addressResolution.AddressTranslator = new addressResolution.EC2MultiRegionTranslator();
}