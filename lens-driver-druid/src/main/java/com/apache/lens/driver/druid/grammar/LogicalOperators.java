/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.lens.driver.druid.grammar;

import java.util.List;

import org.apache.lens.server.api.driver.lib.exception.InvalidQueryException;

import com.google.common.collect.ImmutableMap;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;

public enum LogicalOperators {
  and {
    @Override
    public DimFilter build(List<DimFilter> dimFilters) {
      return new AndDimFilter(dimFilters);
    }
  },
  or {
    @Override
    public DimFilter build(List<DimFilter> dimFilters) {
      return new OrDimFilter(dimFilters);
    }
  },
  not {
    @Override
    public DimFilter build(List<DimFilter> dimFilters) {
      return new NotDimFilter(dimFilters.get(0));
    }
  };

  public abstract DimFilter build(List<DimFilter> dimFilters);

  private static final ImmutableMap<String, LogicalOperators> HQL_LOG_OP_MAP;

  static {
    final ImmutableMap.Builder<String, LogicalOperators> logicalOpsBuilder = ImmutableMap.builder();
    logicalOpsBuilder.put("and", and);
    logicalOpsBuilder.put("AND", and);
    logicalOpsBuilder.put("&&", and);
    logicalOpsBuilder.put("&", and);
    logicalOpsBuilder.put("or", or);
    logicalOpsBuilder.put("OR", or);
    logicalOpsBuilder.put("||", or);
    logicalOpsBuilder.put("|", or);
    logicalOpsBuilder.put("!", not);
    logicalOpsBuilder.put("not", not);
    HQL_LOG_OP_MAP = logicalOpsBuilder.build();
  }

  public static LogicalOperators getFor(String hqlLop) throws InvalidQueryException {
    if (HQL_LOG_OP_MAP.containsKey(hqlLop)) {
      return HQL_LOG_OP_MAP.get(hqlLop);
    }
    throw new InvalidQueryException("Handler not available for logical operator " + hqlLop);
  }
}
