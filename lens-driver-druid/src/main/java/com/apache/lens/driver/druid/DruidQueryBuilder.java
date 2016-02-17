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
package com.apache.lens.driver.druid;

import java.util.List;

import org.joda.time.Interval;

import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQueryBuilder;

public final class DruidQueryBuilder {

  private DruidQueryBuilder() {
  }

  public static DruidQuery createGroupByQuery(String dataSource, List<DimensionSpec> dimensions, HavingSpec having,
                                              List<AggregatorFactory> aggregatorFactories,
                                              List<PostAggregator> postAggregators, Interval interval,
                                              QueryGranularity granularity, DimFilter filter, LimitSpec orderBy,
                                              Integer limit, List<ColumnSchema> querySchema) {

    Query query = GroupByQuery.builder()
      .setDataSource(dataSource)
      .setDimensions(dimensions)
      .setHavingSpec(having)
      .setAggregatorSpecs(aggregatorFactories)
      .setPostAggregatorSpecs(postAggregators)
      .setInterval(interval)
      .setGranularity(granularity)
      .setDimFilter(filter)
      .setLimitSpec(orderBy)
      .setLimit(limit)
      .build();
    return new DruidQuery(query, ImmutableList.copyOf(querySchema), DruidQuery.QueryType.GROUPBY);
  }

  public static DruidQuery createTopNQuery(String dataSource, DimensionSpec dimensionSpec,
                                           TopNMetricSpec topNMetricSpec, int threshold,
                                           List<AggregatorFactory> aggregatorFactories,
                                           List<PostAggregator> postAggregators, List<Interval> intervals,
                                           QueryGranularity granularity, DimFilter filter,
                                           List<ColumnSchema> querySchema) {
    Query query = new TopNQueryBuilder().dataSource(dataSource)
      .dimension(dimensionSpec)
      .metric(topNMetricSpec)
      .threshold(threshold)
      .aggregators(aggregatorFactories)
      .postAggregators(postAggregators)
      .intervals(intervals)
      .granularity(granularity)
      .filters(filter)
      .build();
    return new DruidQuery(query, ImmutableList.copyOf(querySchema), DruidQuery.QueryType.TOPN);
  }
}
