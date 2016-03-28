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

import org.apache.lens.server.api.driver.ast.exception.InvalidQueryException;

import com.google.common.collect.ImmutableMap;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.LessThanHavingSpec;

public enum HavingPredicate {
  EQUAL {
    @Override
    public HavingSpec build(String leftCol, String rightExp) {
      return new EqualToHavingSpec(leftCol, Integer.parseInt(rightExp));
    }

  },
  GREATER_THAN {
    @Override
    public HavingSpec build(String leftCol, String rightExp) {
      return new GreaterThanHavingSpec(leftCol, Integer.parseInt(rightExp));
    }

  },
  LESS_THAN {
    @Override
    public HavingSpec build(String leftCol, String rightExp) {
      return new LessThanHavingSpec(leftCol, Integer.parseInt(rightExp));
    }

  };

  public abstract HavingSpec build(String leftCol, String rightExp);

  public static HavingPredicate getFor(String hqlPredicate) throws InvalidQueryException {
    if (HQL_PREDICATE_MAP.containsKey(hqlPredicate)) {
      return HQL_PREDICATE_MAP.get(hqlPredicate);
    }
    throw new InvalidQueryException("Cannot find a handler for the hql predicate " + hqlPredicate);
  }

  private static final ImmutableMap<String, HavingPredicate> HQL_PREDICATE_MAP;

  static {
    final ImmutableMap.Builder<String, HavingPredicate> predicatesBuilder = ImmutableMap.builder();
    predicatesBuilder.put("=", EQUAL);
    predicatesBuilder.put(">", GREATER_THAN);
    predicatesBuilder.put("<", LESS_THAN);
    HQL_PREDICATE_MAP = predicatesBuilder.build();
  }
}
