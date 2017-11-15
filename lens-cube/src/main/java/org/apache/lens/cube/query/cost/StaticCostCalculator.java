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
package org.apache.lens.cube.query.cost;

import static org.apache.lens.server.api.LensConfConstants.DEFAULT_DRIVER_QUERY_COST;
import static org.apache.lens.server.api.LensConfConstants.DRIVER_QUERY_COST;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.*;

public class StaticCostCalculator implements QueryCostCalculator {

  private QueryCost queryCost;
  private QueryCostTypeDecider queryCostTypeDecider;

  public StaticCostCalculator(String queryCostTypeRange) {
    queryCostTypeDecider = new RangeBasedQueryCostTypeDecider(queryCostTypeRange);
  }

  @Override
  public QueryCost calculateCost(AbstractQueryContext queryContext, LensDriver driver) throws LensException {
    if (null == this.queryCost) {
      Double cost = getStaticCostFromConf(driver);
      this.queryCost = new StaticQueryCost(cost);
      this.queryCost.setQueryCostType(queryCostTypeDecider.decideCostType(this.queryCost));
    }
    return this.queryCost;
  }

  private Double getStaticCostFromConf(LensDriver driver) {
    return driver.getConf().getDouble(DRIVER_QUERY_COST, DEFAULT_DRIVER_QUERY_COST);
  }
}
