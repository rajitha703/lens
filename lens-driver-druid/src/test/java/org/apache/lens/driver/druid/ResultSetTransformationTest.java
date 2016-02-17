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
package org.apache.lens.driver.druid;

import java.util.*;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.TypeDescriptor;

import org.joda.time.DateTime;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.apache.lens.driver.druid.ColumnSchema;
import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.client.DruidResultSet;
import com.apache.lens.driver.druid.client.DruidResultSetTransformer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedRow;
import junit.framework.Assert;

public class ResultSetTransformationTest extends DruidInitDriverTest {

  private static final ImmutableMap<Result, DruidResultSet> VALID_TRANSFORMATIONS;
  private static final ImmutableMap<Result, DruidResultSet> IN_VALID_TRANSFORMATIONS;

  static {
    ImmutableMap.Builder<Result, DruidResultSet> responsesBuilder = ImmutableMap.builder();

    /**
     * Sample aggregate query transformation
     */
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    ColumnSchema col1 = new ColumnSchema("col1");
    col1.setAliasName("col1_alias");
    col1.setDimension(true);
    col1.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col1);
    ColumnSchema col2 = new ColumnSchema("col2");
    col2.setAliasName("col2");
    col2.setDimension(true);
    col2.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col2);
    ColumnSchema aggCol = new ColumnSchema("agg_col");
    aggCol.setAliasName("agg_alias");
    aggCol.setMetric(true);
    aggCol.setDataType(Type.DOUBLE_TYPE);

    columnSchemas.add(aggCol);
    List<MapBasedRow> rows = Lists.newArrayList();
    MapBasedRow mapBasedRow1 = new MapBasedRow(new DateTime("2016-01-20T00:00:00.000Z"), new LinkedHashMap<String,
      Object>() {
      {
        put("agg_alias", 10.0);
        put("col2", "g2v1");
        put("col1_alias", "g1v1");
      }
    });
    MapBasedRow mapBasedRow2 = new MapBasedRow(new DateTime("2016-01-20T00:00:00.000Z"), new LinkedHashMap<String,
      Object>() {
      {
        put("agg_alias", 8.0);
        put("col2", "g2v2");
        put("col1_alias", "g1v1");
      }
    });
    MapBasedRow mapBasedRow3 = new MapBasedRow(new DateTime("2016-01-20T00:00:00.000Z"), new LinkedHashMap<String,
      Object>() {
      {
        put("agg_alias", 15.0);
        put("col2", "g2v3");
        put("col1_alias", "g1v2");
      }
    });
    rows.add(mapBasedRow1);
    rows.add(mapBasedRow2);
    rows.add(mapBasedRow3);
    responsesBuilder.put(
      new Result(
        columnSchemas,
        rows
      ),
      new DruidResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v1", 10.0f)),
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v2", 8.0f)),
          new ResultRow(Lists.<Object>newArrayList("g1v2", "g2v3", 15.0f))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1),
              new ColumnDescriptor("agg_alias", "", new TypeDescriptor(Type.DOUBLE_TYPE), 2)
            );
          }
        })
    );
    /**
     * Sample aggregate query transformation with order changed
     */

    columnSchemas = new ArrayList<>();
    columnSchemas.add(col1);
    columnSchemas.add(aggCol);
    columnSchemas.add(col2);

    responsesBuilder.put(
      new Result(
        columnSchemas,
        rows
      ),
      new DruidResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", 10.0f, "g2v1")),
          new ResultRow(Lists.<Object>newArrayList("g1v1", 8.0f, "g2v2")),
          new ResultRow(Lists.<Object>newArrayList("g1v2", 15.0f, "g2v3"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("agg_alias", "", new TypeDescriptor(Type.DOUBLE_TYPE), 1),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 2)
            );
          }
        })
    );


    VALID_TRANSFORMATIONS = responsesBuilder.build();

    ImmutableMap.Builder<Result, DruidResultSet> invalidResponsesBuilder = ImmutableMap.builder();

    /**
     * No Aggregation
     */

    columnSchemas = new ArrayList<>();
    columnSchemas.add(col1);
    columnSchemas.add(col2);

    responsesBuilder.put(
      new Result(
        columnSchemas,
        rows
      ),
      new DruidResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v1", 10.0f)),
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v2", 8.0f)),
          new ResultRow(Lists.<Object>newArrayList("g1v2", "g2v3", 15.0f))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1),
              new ColumnDescriptor("agg_alias", "", new TypeDescriptor(Type.DOUBLE_TYPE), 2)
            );
          }
        })
    );
    /**
     * Sample aggregate query transformation with missing alias
     */
    columnSchemas = new ArrayList<>();
    col1 = new ColumnSchema("col1");
    col1.setAliasName("col1");
    col1.setDimension(true);
    col1.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col1);
    col2 = new ColumnSchema("col2");
    col2.setAliasName("col2");
    col2.setDimension(true);
    col2.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col2);
    aggCol = new ColumnSchema("agg_col");
    aggCol.setAliasName("agg_col");
    aggCol.setMetric(true);
    aggCol.setDataType(Type.DOUBLE_TYPE);

    columnSchemas.add(aggCol);
    columnSchemas = new ArrayList<>();
    columnSchemas.add(col1);
    columnSchemas.add(aggCol);
    columnSchemas.add(col2);

    responsesBuilder.put(
      new Result(
        columnSchemas,
        rows
      ),
      new DruidResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", 10.0f, "g2v1")),
          new ResultRow(Lists.<Object>newArrayList("g1v1", 8.0f, "g2v2")),
          new ResultRow(Lists.<Object>newArrayList("g1v2", 15.0f, "g2v3"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("agg_alias", "", new TypeDescriptor(Type.DOUBLE_TYPE), 1),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 2)
            );
          }
        })
    );

    /**
     * Sample aggregate query transformation with order changed
     */
    columnSchemas = new ArrayList<>();
    col1 = new ColumnSchema("col1");
    col1.setAliasName("col1_alias");
    col1.setDimension(true);
    col1.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col1);
    col2 = new ColumnSchema("col2");
    col2.setAliasName("col2");
    col2.setDimension(true);
    col2.setDataType(Type.STRING_TYPE);

    columnSchemas.add(col2);
    aggCol = new ColumnSchema("agg_col");
    aggCol.setAliasName("agg_alias");
    aggCol.setMetric(true);
    aggCol.setDataType(Type.DOUBLE_TYPE);

    columnSchemas.add(aggCol);
    columnSchemas = new ArrayList<>();
    columnSchemas.add(col1);
    columnSchemas.add(col2);
    columnSchemas.add(aggCol);

    responsesBuilder.put(
      new Result(
        columnSchemas,
        rows
      ),
      new DruidResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", 10.0f, "g2v1")),
          new ResultRow(Lists.<Object>newArrayList("g1v1", 8.0f, "g2v2")),
          new ResultRow(Lists.<Object>newArrayList("g1v2", 15.0f, "g2v3"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("agg_alias", "", new TypeDescriptor(Type.DOUBLE_TYPE), 1),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 2)
            );
          }
        })
    );

    IN_VALID_TRANSFORMATIONS = invalidResponsesBuilder.build();
  }

  @BeforeTest
  @Override
  public void beforeTest() throws Exception {
    super.beforeTest();
  }

  @Override
  protected void initializeConfig(Configuration config) {
    config.setInt(DruidDriverConfig.QUERY_TIME_OUT_LENS_KEY, 10000);
    config.setStrings(DruidDriverConfig.CLIENT_CLASS_KEY, MockClientDruid.class.getCanonicalName());
  }

  private void assertResultsAreEqual(DruidResultSet resultSet1, DruidResultSet resultSet2) throws LensException {
    final Collection<ColumnDescriptor> columns1 = resultSet1.getMetadata().getColumns();
    final Collection<ColumnDescriptor> columns2 = resultSet2.getMetadata().getColumns();
    Assert.assertEquals(columns1.size(), columns2.size());
    final Iterator<ColumnDescriptor> iterator1 = columns1.iterator();
    final Iterator<ColumnDescriptor> iterator2 = columns2.iterator();
    while (iterator1.hasNext()) {
      final ColumnDescriptor column1 = iterator1.next();
      final ColumnDescriptor column2 = iterator2.next();
      Assert.assertEquals("Column aliases are different! " + column1.getName() + " " + column1.getName(),
        column1.getName(), column2.getName());
      Assert.assertEquals("Column positions are different! " + column1.getName() + " " + column1.getName(),
        column1.getOrdinalPosition(), column2.getOrdinalPosition());
      Assert.assertEquals("Column types are different! " + column1.getName() + " " + column1.getName(),
        column1.getType(), column2.getType());
    }

    Assert.assertEquals(resultSet1.size(), resultSet2.size());
    while (resultSet1.hasNext()) {
      final ResultRow row1 = resultSet1.next();
      final ResultRow row2 = resultSet2.next();
      Assert.assertEquals("Row length is different", row1.getValues().size(), row2.getValues().size());
      Iterator<Object> values1 = row1.getValues().iterator();
      Iterator<Object> values2 = row2.getValues().iterator();
      while (values1.hasNext()) {
        final Object column1 = values1.next();
        final Object column2 = values2.next();
        Assert.assertEquals("Values are different", column1, column2);
      }
    }

  }

  @Test
  public void testTransformations() throws LensException {
    for (Map.Entry<Result, DruidResultSet> entry : VALID_TRANSFORMATIONS.entrySet()) {
      final Result rawResult = entry.getKey();
      DruidResultSet resultSet =
        DruidResultSetTransformer
          .getTransformer(DruidQuery.QueryType.GROUPBY, rawResult.mapBasedRows, rawResult.columnSchema).transform();
      assertResultsAreEqual(resultSet, entry.getValue());
    }
  }

  @Test
  public void testInvalidTranformations() {
    for (Map.Entry<Result, DruidResultSet> entry : IN_VALID_TRANSFORMATIONS.entrySet()) {
      boolean failed = false;
      try {
        final Result rawResult = entry.getKey();
        DruidResultSet resultSet =
          DruidResultSetTransformer
            .getTransformer(DruidQuery.QueryType.GROUPBY, rawResult.mapBasedRows, rawResult.columnSchema).transform();
        assertResultsAreEqual(resultSet, entry.getValue());
        failed = true;
        throw new RuntimeException("Result sets are equal - ");
      } catch (Throwable e) {
        if (failed) {
          Assert.fail("Results sets are equal Expected - not equal" + e.getMessage());
        }
      }
    }
  }

  static class Result {
    final List<ColumnSchema> columnSchema;
    final List<MapBasedRow> mapBasedRows;

    Result(List<ColumnSchema> columnSchema, List<MapBasedRow> mapBasedRows) {
      this.columnSchema = columnSchema;
      this.mapBasedRows = mapBasedRows;
    }
  }


}
