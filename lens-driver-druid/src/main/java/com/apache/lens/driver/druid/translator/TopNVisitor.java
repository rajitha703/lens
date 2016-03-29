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
package com.apache.lens.driver.druid.translator;

import java.util.List;

import org.apache.lens.server.api.driver.ColumnSchema;
import org.apache.lens.server.api.driver.ast.ASTCriteriaVisitor;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.conf.HiveConf;

import org.joda.time.Interval;

import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.DruidQueryBuilder;
import com.google.common.collect.Lists;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.LexicographicTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import lombok.Data;

@Data
public class TopNVisitor extends DruidVisitor {

  private DimensionSpec dimension;
  private TopNMetricSpec topNMetricSpec;

  protected TopNVisitor(DruidDriverConfig config, HiveConf hiveConf) {
    super(config, hiveConf);
  }

  @Override
  public void visitSimpleSelect(String columnName, String alias) {
    columnName = visitColumn(columnName);

    dimension = new DefaultDimensionSpec(columnName, alias);
    final String aliasName = alias == null ? columnName : alias;

    for (ColumnSchema schema : columnSchemas) {
      Validate.isTrue(!schema.getAliasName().equals(aliasName), "Ambiguous alias '" + aliasName + "'");
    }

    ColumnSchema columnSchema = new ColumnSchema(columnName);
    columnSchema.setAliasName(aliasName);
    columnSchema.setDimension(true);
    columnSchema.setDataType(getDataType(columnName));
    columnSchemas.add(columnSchema);

    this.topNMetricSpec = new LexicographicTopNMetricSpec(null);
  }

  @Override
  public void visitGroupBy(String columnName) {
    columnName = visitColumn(columnName);
    DimensionSpec dimensionSpec = new DefaultDimensionSpec(columnName, columnName);
    Validate.isTrue(this.getDimension().equals(dimensionSpec), "Group by column has to be used in select");
  }

  @Override
  public void visitOrderBy(String columnName, OrderBy orderBy) {
    this.topNMetricSpec = new NumericTopNMetricSpec(columnName);
    if (orderBy.equals(OrderBy.ASC)) {
      this.topNMetricSpec = new InvertedTopNMetricSpec(this.topNMetricSpec);
    }
  }

  @Override
  public void visitHaving(ASTCriteriaVisitor astCriteriaVisitor) {
    throw new UnsupportedOperationException("Having not valid in a TopN query");
  }

  @Override
  public void visitLimit(int limit) {
    this.limit = limit;
  }

  public DruidQuery getQuery() {
    Validate.isTrue(validateQuery());
    List<Interval> intervalList = Lists.newArrayList();
    intervalList.add(new Interval(startInstant, endInstant));
    return DruidQueryBuilder
      .createTopNQuery(dataSource, dimension, topNMetricSpec, limit, aggregatorFactories, postAggregators, intervalList,
        granularity, filter, columnSchemas);
  }

  public boolean validateQuery() {
    return !(startInstant == null
      || endInstant == null
      || dataSource == null
      || dimension == null
      || aggregatorFactories == null
      || aggregatorFactories.isEmpty()
      || topNMetricSpec == null);
  }

}
