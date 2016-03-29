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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.driver.ColumnSchema;
import org.apache.lens.server.api.driver.ast.ASTCriteriaVisitor;
import org.apache.lens.server.api.driver.ast.ASTVisitor;
import org.apache.lens.server.api.driver.ast.exception.InvalidQueryException;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hive.service.cli.Type;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.apache.lens.driver.druid.ASTTraverserForDruid;
import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.exceptions.DruidRewriteException;
import com.apache.lens.driver.druid.grammar.Aggregator;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public abstract class DruidVisitor implements ASTVisitor {

  @NonNull
  protected String dataSource;
  @NonNull
  protected final DruidDriverConfig config;
  @NonNull
  protected final HiveConf hiveConf;
  protected Integer limit;
  protected DimFilter filter;
  protected QueryGranularity granularity = QueryGranularity.ALL;

  protected final List<AggregatorFactory> aggregatorFactories = new ArrayList<>();
  protected final List<PostAggregator> postAggregators = new ArrayList<>();
  protected final List<ColumnSchema> columnSchemas = Lists.newArrayList();

  @NonNull
  protected org.joda.time.DateTime startInstant;
  @NonNull
  protected org.joda.time.DateTime endInstant;
  @NonNull
  protected final DateTimeFormatter formatter;

  public static final String DRUID_TABLE_TIME_DIMENSION = "druid.table.time.dimension";

  protected DruidVisitor(DruidDriverConfig config, HiveConf hiveConf) {
    this.config = config;
    this.hiveConf = hiveConf;
    this.formatter = DateTimeFormat.forPattern(config.getDateFormat());
  }

  public static DruidQuery rewrite(DruidDriverConfig config, HiveConf hiveConf, String hql) throws LensException {
    ASTNode rootQueryNode;
    try {
      rootQueryNode = HQLParser.parseHQL(hql, new HiveConf());
    } catch (Exception e) {
      throw new DruidRewriteException(e);
    }
    return rewrite(config, hiveConf, rootQueryNode);
  }

  public static DruidQuery rewrite(DruidDriverConfig config, HiveConf hiveConf, ASTNode rootQueryNode) throws
    LensException {
    try {
      DruidVisitor visitor = null;
      if (DruidVisitor.hasGroupBy(rootQueryNode)) {
        final ASTNode groupByNode = HQLParser.findNodeByPath(rootQueryNode,
          HiveParser.TOK_INSERT, HiveParser.TOK_GROUPBY);

        int noOfChildren = 0;
        if (null != groupByNode) {
          noOfChildren = groupByNode.getChildCount();
        }

        if (noOfChildren == 1 && DruidVisitor.hasLimit(rootQueryNode)) {
          visitor = new TopNVisitor(config, hiveConf);
        } else {
          visitor = new GroupByVisitor(config, hiveConf);
        }
      } else {
        throw new UnsupportedOperationException("This query is not supported in Druid");
      }
      new ASTTraverserForDruid(
        rootQueryNode,
        visitor,
        new DruidCriteriaVisitorFactory(),
        new DruidHavingVisitorFactory()
      ).accept();
      return visitor.getQuery();
    } catch (Exception e) {
      throw new DruidRewriteException(e);
    }
  }

  @Override
  public void visitAggregation(String aggregationType, String columnName, String alias) throws InvalidQueryException {

    columnName = visitColumn(columnName);
    final String aliasName = alias == null ? columnName : alias;

    ColumnSchema columnSchema = new ColumnSchema(columnName);
    columnSchema.setColumnName(columnName);
    columnSchema.setAliasName(aliasName);
    columnSchema.setMetric(true);
    columnSchema.setDataType(Type.DOUBLE_TYPE);
    columnSchemas.add(columnSchema);

    AggregatorFactory aggregatorFactory =
      Aggregator.valueOf(aggregationType.toUpperCase()).getAggregatorFactory(columnName, alias);
    if (null != aggregatorFactory) {
      this.getAggregatorFactories().add(aggregatorFactory);
    } else {
      throw new UnsupportedOperationException("This aggregation is not accepted in Druid .." + aggregationType);
    }
  }

  @Override
  public void visitFrom(String database, String table) {
    this.dataSource = table;
  }

  @Override
  public void visitCriteria(ASTCriteriaVisitor visitedSubTree) {
    this.filter = ((DruidCriteriaVisitor) visitedSubTree).getDimFilter();
  }

  @Override
  public void completeVisit() {

  }

  @Override
  public void visitAllCols() {
    throw new UnsupportedOperationException("'*' is not supported in druid, select the columns required");
  }

  public abstract DruidQuery getQuery();

  public boolean isTimeDimension(String colName) throws HiveException {
    Table tbl = CubeMetastoreClient.getInstance(hiveConf).getHiveTable(dataSource);
    return tbl != null && tbl.getProperty(DRUID_TABLE_TIME_DIMENSION).equals(colName);
  }

  public Type getDataType(String columnName) {
    Type hiveType = Type.NULL_TYPE;
    try {
      Table tbl = CubeMetastoreClient.getInstance(hiveConf).getHiveTable(dataSource);

      List<FieldSchema> fieldSchemas = tbl.getCols();
      for (FieldSchema fs : fieldSchemas) {
        if (fs.getName().equals(columnName)) {
          String type = fs.getType();
          if (type.equalsIgnoreCase("boolean")) {
            hiveType = Type.BOOLEAN_TYPE;
          } else if (type.equalsIgnoreCase("string")) {
            hiveType = Type.STRING_TYPE;
          } else if (type.equalsIgnoreCase("int")) {
            hiveType = Type.INT_TYPE;
          }
          break;
        }
      }
    } catch (HiveException e) {
      log.error("Exception while fetching fieldschema ", e);
    }
    return hiveType;
  }

  public void visitStartTimeInstant(String startTimeInstant) {
    this.startInstant = this.formatter.parseDateTime(startTimeInstant);
  }

  public void visitEndTimeInstant(String endTimeInstant) {
    this.endInstant = this.formatter.parseDateTime(endTimeInstant);
  }

  protected static String visitColumn(String cannonicalColName) {
    final String[] colParts = cannonicalColName.split("\\.");
    return colParts[colParts.length - 1];
  }

  public static boolean hasGroupBy(ASTNode rootQueryNode) {
    return HQLParser.findNodeByPath(rootQueryNode, TOK_INSERT, TOK_GROUPBY) != null;
  }

  public static boolean hasLimit(ASTNode rootQueryNode) {
    return HQLParser.findNodeByPath(rootQueryNode, TOK_INSERT, TOK_LIMIT) != null;
  }

  public static String trimValue(String value) {
    return value.replaceAll("'", "");
  }
}
