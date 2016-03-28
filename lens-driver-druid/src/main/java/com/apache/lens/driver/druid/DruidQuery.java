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

import org.apache.lens.server.api.driver.ColumnSchema;

import com.google.common.collect.ImmutableList;
import io.druid.query.Query;
import io.druid.query.groupby.GroupByQuery;
import lombok.Data;

@Data
public class DruidQuery {

  public enum QueryType {GROUPBY, TOPN}

  /* the native druid query*/
  private final Query query;
  /**
   * aliases in the query (ordered)
   */
  private final ImmutableList<ColumnSchema> columnSchema;

  /*the query type*/
  private final QueryType queryType;

  public String toGroupByString() {
    GroupByQuery groupByQuery = (GroupByQuery) query;
    return "GroupByQuery{"
      + "dataSource=" + groupByQuery.getDataSource()
      + ", querySegmentSpec=" + groupByQuery.getQuerySegmentSpec()
      + ", dimFilter=" + groupByQuery.getDimFilter()
      + ", granularity=" + groupByQuery.getGranularity()
      + ", dimensions=" + groupByQuery.getDimensions()
      + ", aggregatorSpecs=" + groupByQuery.getAggregatorSpecs()
      + ", havingSpec=" + groupByQuery.getHavingSpec()
      + ", limitSpec=" + groupByQuery.getLimitSpec()
      + ", context=" + groupByQuery.getContext()
      + '}';
  }

  public String toString() {
    if (queryType.equals(QueryType.GROUPBY)) {
      return toGroupByString();
    } else {
      return query.toString();
    }
  }
}
