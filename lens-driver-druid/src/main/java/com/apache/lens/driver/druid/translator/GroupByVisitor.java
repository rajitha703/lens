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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.server.api.driver.ast.ASTCriteriaVisitor;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.conf.HiveConf;

import org.joda.time.Interval;

import org.apache.lens.server.api.driver.ColumnSchema;
import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.DruidQueryBuilder;
import com.google.common.collect.Lists;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import lombok.Data;

@Data
public class GroupByVisitor extends DruidVisitor {

  private List<DimensionSpec> dimensions;
  private HavingSpec having;
  private LimitSpec orderBy;

  public GroupByVisitor(DruidDriverConfig driverConfig, HiveConf hiveConf) {
    super(driverConfig, hiveConf);
    this.dimensions = new ArrayList<>();
    this.limit = Integer.MAX_VALUE;
  }

  @Override
  public void visitSimpleSelect(String columnName, String alias) {
    columnName = visitColumn(columnName);

    dimensions.add(new DefaultDimensionSpec(columnName, alias));
    final String aliasName = alias == null ? columnName : alias;

    for (ColumnSchema schema : columnSchemas) {
      Validate.isTrue(!schema.getAliasName().equals(aliasName), "Ambiguous alias '" + aliasName + "'");
    }

    ColumnSchema columnSchema = new ColumnSchema(columnName);
    columnSchema.setAliasName(aliasName);
    columnSchema.setDimension(true);
    columnSchema.setDataType(getDataType(columnName));
    columnSchemas.add(columnSchema);
  }

  @Override
  public void visitGroupBy(String columnName) {
    columnName = visitColumn(columnName);
    DimensionSpec dimensionSpec = new DefaultDimensionSpec(columnName, columnName);
    if (!this.getDimensions().contains(dimensionSpec)){
      this.getDimensions().add(dimensionSpec);
    }
  }

  @Override
  public void visitOrderBy(String colName, OrderBy orderBy) {
    List<OrderByColumnSpec> orderByColSpec = Lists.newArrayList();
    OrderByColumnSpec.Direction direction;
    if (orderBy.equals(OrderBy.DESC)) {
      direction = OrderByColumnSpec.Direction.DESCENDING;
    } else {
      direction = OrderByColumnSpec.Direction.ASCENDING;
    }
    orderByColSpec.add(new OrderByColumnSpec(colName, direction));
    this.orderBy = new DefaultLimitSpec(orderByColSpec, this.limit);
  }

  @Override
  public void visitLimit(int limit) {
    this.limit = limit;
  }

  @Override
  public void visitHaving(ASTCriteriaVisitor visitedSubTree) {
    this.having = ((DruidHavingVisitor) visitedSubTree).getHavingSpec();
  }

  @Override
  public DruidQuery getQuery() {
    Validate.isTrue(validateQuery());
    return DruidQueryBuilder.createGroupByQuery(dataSource, dimensions, having, aggregatorFactories,
      postAggregators, new Interval(startInstant, endInstant), granularity, filter, orderBy, limit,
      columnSchemas);
  }

  public boolean validateQuery() {
    return !(startInstant == null
      || endInstant == null
      || dataSource == null
      || dimensions == null
      || dimensions.isEmpty()
      || aggregatorFactories == null
      || aggregatorFactories.isEmpty());
  }

}
