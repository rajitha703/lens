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
package com.apache.lens.driver.druid.grammar.having;

import java.util.List;

import org.apache.lens.server.api.driver.lib.exception.InvalidQueryException;

import com.google.common.collect.ImmutableMap;
import io.druid.query.groupby.having.AndHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.NotHavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;

public enum HavingLogicalOperators {
  and {
    @Override
    public HavingSpec build(List<HavingSpec> havingSpecs) {
      return new AndHavingSpec(havingSpecs);
    }
  },
  or {
    @Override
    public HavingSpec build(List<HavingSpec> havingSpecs) {
      return new OrHavingSpec(havingSpecs);
    }
  },
  not {
    @Override
    public HavingSpec build(List<HavingSpec> havingSpecs) {
      return new NotHavingSpec(havingSpecs.get(0));
    }
  };

  public abstract HavingSpec build(List<HavingSpec> havingSpecs);

  private static final ImmutableMap<String, HavingLogicalOperators> HQL_LOG_OP_MAP;

  static {
    final ImmutableMap.Builder<String, HavingLogicalOperators> logicalOpsBuilder = ImmutableMap.builder();
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

  public static HavingLogicalOperators getFor(String hqlLop) throws InvalidQueryException {
    if (HQL_LOG_OP_MAP.containsKey(hqlLop)) {
      return HQL_LOG_OP_MAP.get(hqlLop);
    }
    throw new InvalidQueryException("Handler not available for Having logical operator " + hqlLop);
  }
}
