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
import helper from "../test-helper";
import specExecModule from "../../lib/policies/speculative-execution";


describe('NoSpeculativeExecutionPolicy', () => {
  describe('#getOptions()', () => {
    it('should return an empty Map', () => {
      helper.assertMapEqual(new specExecModule.NoSpeculativeExecutionPolicy().getOptions(), new Map());
    });
  });
});

describe('ConstantSpeculativeExecutionPolicy', () => {
  describe('#getOptions()', () => {
    it('should return a Map with the policy options', () => {
      helper.assertMapEqual(new specExecModule.ConstantSpeculativeExecutionPolicy(200, 1).getOptions(),
        new Map([['delay', 200], ['maxSpeculativeExecutions', 1]]));
    });
  });
});
