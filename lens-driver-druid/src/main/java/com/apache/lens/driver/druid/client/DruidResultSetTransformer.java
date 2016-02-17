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
package com.apache.lens.driver.druid.client;

import java.util.List;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.LensResultSetMetadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import com.apache.lens.driver.druid.ColumnSchema;
import com.apache.lens.driver.druid.DruidQuery;
import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNResultValue;
import lombok.NonNull;

public abstract class DruidResultSetTransformer {

  @NonNull
  protected final Object result;
  @NonNull
  protected final List<ColumnSchema> columnSchema;

  public DruidResultSetTransformer(Object result, List<ColumnSchema> columnSchema) {
    this.columnSchema = columnSchema;
    this.result = result;
  }

  static class GroupByResultTransformer extends DruidResultSetTransformer {
    private List<ResultRow> rows = Lists.newArrayList();

    public GroupByResultTransformer(Object result, List<ColumnSchema> columnSchema) {
      super(result, columnSchema);
    }

    @Override
    public DruidResultSet transform() {
      collectAllRows(result);
      return new DruidResultSet(
        rows.size(),
        rows,
        getMetaData(columnSchema)
      );
    }

    private void collectAllRows(Object druidResultObject) {

      List<Row> druidResultRows = (List<Row>) druidResultObject;

      for (final Row r : druidResultRows) {
        List<Object> currentPath = getEmptyRow();
        for (ColumnSchema cs : columnSchema) {
          if (cs.isDimension()) {
            final int index = columnSchema.indexOf(cs);
            currentPath.set(index, StringUtils.join(r.getDimension(cs.getAliasName()), ","));
          } else if (cs.isMetric()) {
            final int index = columnSchema.indexOf(cs);
            currentPath.set(index, r.getFloatMetric(cs.getAliasName()));
          }
        }
        rows.add(new ResultRow(Lists.newArrayList(currentPath)));

      }
    }
  }

  static class TopNResultTransformer extends DruidResultSetTransformer {
    private List<ResultRow> rows = Lists.newArrayList();


    public TopNResultTransformer(Object result, List<ColumnSchema> columnSchema) {
      super(result, columnSchema);
    }

    @Override
    public DruidResultSet transform() {
      collectAllRows(result);
      return new DruidResultSet(
        rows.size(),
        rows,
        getMetaData(columnSchema)
      );
    }

    private void collectAllRows(Object druidResultObject) {

      List<Result<TopNResultValue>> druidResult = (List<Result<TopNResultValue>>) druidResultObject;

      TopNResultValue druidResultRow;

      if (!druidResult.isEmpty()) {
        druidResultRow = druidResult.get(0).getValue();
      } else {
        return;
      }

      for (final DimensionAndMetricValueExtractor dm : druidResultRow.getValue()) {
        List<Object> currentPath = getEmptyRow();

        for (ColumnSchema cs : columnSchema) {
          if (cs.isDimension()) {
            final int index = columnSchema.indexOf(cs);
            currentPath.set(index, dm.getDimensionValue(cs.getAliasName()));
          } else if (cs.isMetric()) {
            final int index = columnSchema.indexOf(cs);
            currentPath.set(index, dm.getMetric(cs.getAliasName()));
          }
        }
        rows.add(new ResultRow(Lists.newArrayList(currentPath)));
      }
    }
  }

  protected LensResultSetMetadata getMetaData(final List<ColumnSchema> columnSchema) {
    return new LensResultSetMetadata() {
      @Override
      public List<ColumnDescriptor> getColumns() {
        List<ColumnDescriptor> descriptors = Lists.newArrayList();
        int i = 0;
        for (final ColumnSchema cs : columnSchema) {
          descriptors.add(
            new ColumnDescriptor(cs.getAliasName(), cs.getAliasName(), new TypeDescriptor(cs.getDataType()), i)
          );
          i++;
        }
        return descriptors;
      }
    };
  }

  public static DruidResultSetTransformer getTransformer(DruidQuery.QueryType queryType, Object druidResultObject,
                                                         List<ColumnSchema> columnSchema) {
    if (queryType.equals(DruidQuery.QueryType.GROUPBY)) {
      return new GroupByResultTransformer(druidResultObject, columnSchema);
    } else if (queryType.equals(DruidQuery.QueryType.TOPN)) {
      return new TopNResultTransformer(druidResultObject, columnSchema);
    } else {
      throw new UnsupportedOperationException("This query type is not supported in Druid :" + queryType);
    }
  }

  public abstract DruidResultSet transform();

  protected List<Object> getEmptyRow() {
    List<Object> objects = Lists.newArrayList();
    int i = 0;
    while (i++ < columnSchema.size()) {
      objects.add(null);
    }
    return objects;
  }

}
