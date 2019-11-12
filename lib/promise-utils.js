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

/**
 * @param {LoadBalancingPolicy} lbp The load balancing policy.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {ExecutionOptions|null} executionOptions The information related to the execution of the request.
 * @returns {Promise<Iterator>}
 */
function newQueryPlan(lbp, keyspace, executionOptions) {
  return new Promise((resolve, reject) => {
    lbp.newQueryPlan(keyspace, executionOptions, (err, iterator) => {
      if (err) {
        reject(err);
      } else {
        resolve(iterator);
      }
    });
  });
}

module.exports = {
  newQueryPlan
};