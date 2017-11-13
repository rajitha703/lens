/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 *
 */

package org.apache.lens.server.api.query.cost;

import org.apache.lens.api.query.QueryCostType;
import org.apache.lens.server.api.error.LensException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CostRangeQueryTypeDecider implements QueryTypeDecider {

  @NonNull
  private final CostToQueryTypeRangeConf costToQueryTypeRangeMap;

  @Override
  public QueryCostType decideCostType(@NonNull final QueryCost cost) throws LensException {
    QueryCostType q = costToQueryTypeRangeMap.get(cost.getEstimatedResourceUsage());
    log.info("cost was: {}, decided querytype: {}", cost, q);
    return q;
  }
}