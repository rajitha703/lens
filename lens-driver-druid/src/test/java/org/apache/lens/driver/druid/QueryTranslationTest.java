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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.translator.DruidVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryTranslationTest extends DruidInitDriverTest {

  private static class ValidQuery {
    private final String name;
    private final String hql;
    private final JsonNode expectedJson;

    @JsonCreator
    ValidQuery(
      @JsonProperty("name") String name,
      @JsonProperty("hql") String hql,
      @JsonProperty("expectedJson") JsonNode expectedJson) {
      this.name = name;
      this.hql = hql;
      this.expectedJson = expectedJson;
    }
  }

  private static class InvalidQuery {
    private final String name;
    private final String hql;

    @JsonCreator
    InvalidQuery(
      @JsonProperty("name") String name,
      @JsonProperty("hql") String hql) {
      this.name = name;
      this.hql = hql;
    }
  }

  private static <T> T loadResource(String resourcePath, Class<T> type) {
    try {
      return OBJECT_MAPPER.readValue(
        QueryTranslationTest.class.getClassLoader()
          .getResourceAsStream(resourcePath),
        type
      );
    } catch (IOException e) {
      throw new RuntimeException("FATAL! Cannot initialize test resource : " + resourcePath);
    }
  }

  public static final String VALID_QUERIES_RESOURCE_PATH = "valid-queries.data";
  public static final String INVALID_QUERIES_RESOURCE_PATH = "invalid-queries.data";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ImmutableList<ValidQuery> VALID_QUERIES;
  private static final ImmutableList<InvalidQuery> IN_VALID_QUERIES;

  static {
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true); // Jackson 1.2+
    VALID_QUERIES = ImmutableList.copyOf(loadResource(VALID_QUERIES_RESOURCE_PATH, ValidQuery[].class));
    IN_VALID_QUERIES = ImmutableList.copyOf(loadResource(INVALID_QUERIES_RESOURCE_PATH, InvalidQuery[].class));
  }

  @BeforeTest
  @Override
  public void beforeTest() throws Exception {
    super.beforeTest();
    List<FieldSchema> factColumns = new ArrayList<>();
    factColumns.add(new FieldSchema("continent", "int", ""));
    factColumns.add(new FieldSchema("country", "int", ""));
    factColumns.add(new FieldSchema("region", "int", ""));
    factColumns.add(new FieldSchema("city", "int", ""));
    factColumns.add(new FieldSchema("count", "double", ""));
    factColumns.add(new FieldSchema("added", "double", ""));
    try {
      HiveConf hiveConf = new HiveConf();
      SessionState.start(hiveConf);
      Hive.get().dropTable("default.wikipedia");
      createHiveTable("default", "wikipedia", factColumns);
      Table tbl = CubeMetastoreClient.getInstance(hiveConf).getHiveTable("wikipedia");
      tbl.setProperty("druid.table.time.dimension", "time");
    } catch (HiveException e) {
      log.error("Exception while creating hive table", e);
    }
  }

  @Override
  protected void initializeConfig(Configuration config) {
    config.setStrings(DruidDriverConfig.CLIENT_CLASS_KEY, MockClientDruid.class.getCanonicalName());
    config.setStrings(DruidDriverConfig.DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
  }

  @Test
  public void testQueryTranslation() throws LensException {

    String[] timeInstants = getTimeInstants();
    String inputTimeRange = getLastTwoDaysTimeRange(timeInstants[0], timeInstants[1]);
    String outputTimeRange = getLastTwoDaysJodaTimeRange(timeInstants[0], timeInstants[1]);

    for (final ValidQuery query : VALID_QUERIES) {

      String formattedActualQuery = String.format(query.hql, inputTimeRange);
      String formattedExpectedQuery = String.format(query.expectedJson.toString().replace("\"",
        ""), outputTimeRange);
      String actualQuery = DruidVisitor.rewrite(druidDriverConfig, hiveConf, formattedActualQuery).toString();

      log.info("Actual : " + actualQuery);
      log.info("Expected : " + formattedExpectedQuery);
      Assert.assertEquals(actualQuery, formattedExpectedQuery, "Test case '" + query.name + "' failed.");
    }
  }

  @Test
  public void testInvalidQueries() {

    String[] timeInstants = getTimeInstants();
    String inputTimeRange = getLastTwoDaysTimeRange(timeInstants[0], timeInstants[1]);

    for (final InvalidQuery invalidQuery : IN_VALID_QUERIES) {
      String formattedActualQuery = String.format(invalidQuery.hql, inputTimeRange);
      try {
        DruidVisitor.rewrite(druidDriverConfig, hiveConf, formattedActualQuery);
        Assert.fail("The invalid query" + invalidQuery.name + "did NOT suffer any exception");
      } catch (Throwable e) {
        continue;
      }
    }
  }

  public static void createHiveTable(String db, String table, List<FieldSchema> columns) throws Exception {
    Table tbl1 = new Table(db, table);
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    Assert.assertNotNull(Hive.get().getTable(db, table));
    log.info("Created table : " + table);
  }

  public String[] getTimeInstants() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Calendar calendar = Calendar.getInstance();
    String endTime = dateFormat.format(calendar.getTime());
    calendar.add(Calendar.DAY_OF_MONTH, -2);
    String startTime = dateFormat.format(calendar.getTime());
    return new String[]{startTime, endTime};
  }

  public static String getLastTwoDaysTimeRange(String startTimeInstant, String endTimeInstant) {
    return "'" + startTimeInstant + "'" + " AND " + "'" + endTimeInstant + "'";
  }

  public static String getLastTwoDaysJodaTimeRange(String startTimeInstant, String endTimeInstant) {
    DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    DateTime startTime = dateFormat.parseDateTime(startTimeInstant);
    DateTime endTime = dateFormat.parseDateTime(endTimeInstant);
    return new Interval(startTime, endTime).toString();
  }
}
