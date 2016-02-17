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

import org.apache.lens.server.api.driver.lib.exception.InvalidQueryException;

import com.google.common.collect.ImmutableMap;
import io.druid.query.aggregation.*;

public enum Aggregators {
  count {
    @Override
    public AggregatorFactory getAggregatorFactory(String columnName, String alias) {
      return new CountAggregatorFactory(alias);
    }
  },
  sum {
    @Override
    public AggregatorFactory getAggregatorFactory(String columnName, String alias) {
      return new DoubleSumAggregatorFactory(alias, columnName);
    }
  },
  min {
    @Override
    public AggregatorFactory getAggregatorFactory(String columnName, String alias) {
      return new DoubleMinAggregatorFactory(alias, columnName);
    }
  },
  max {
    @Override
    public AggregatorFactory getAggregatorFactory(String columnName, String alias) {
      return new DoubleMaxAggregatorFactory(alias, columnName);
    }
  };

  public abstract AggregatorFactory getAggregatorFactory(String columnName, String alias);

  private static final ImmutableMap<String, Aggregators> AGGREGATOR_FACTORY_IMMUTABLE_MAP;

  static {
    final ImmutableMap.Builder<String, Aggregators> aggregateFactoryBuilder = ImmutableMap.builder();
    aggregateFactoryBuilder.put("count", count);
    aggregateFactoryBuilder.put("sum", sum);
    aggregateFactoryBuilder.put("min", min);
    aggregateFactoryBuilder.put("max", max);

    AGGREGATOR_FACTORY_IMMUTABLE_MAP = aggregateFactoryBuilder.build();
  }

  public static Aggregators getFor(String aggregator) throws InvalidQueryException {
    if (AGGREGATOR_FACTORY_IMMUTABLE_MAP.containsKey(aggregator)) {
      return AGGREGATOR_FACTORY_IMMUTABLE_MAP.get(aggregator);
    }
    throw new InvalidQueryException("Handler not available for aggregator " + aggregator);
  }
};
