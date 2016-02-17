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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;

import com.apache.lens.driver.druid.DruidDriver;
import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.client.DruidClientImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidDriverTest {

  private DruidDriverTest() {
  }

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.setInt(DruidDriverConfig.QUERY_TIME_OUT_LENS_KEY, 10000);
    config.setStrings(DruidDriverConfig.CLIENT_CLASS_KEY, DruidClientImpl.class.getCanonicalName());
    config.setStrings(DruidDriverConfig.DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");

    DruidDriver druidDriver = new DruidDriver();
    druidDriver.configure(config, "druid", "druid");

    List<FieldSchema> factColumns = new ArrayList<>();
    factColumns.add(new FieldSchema("continent", "int", ""));
    factColumns.add(new FieldSchema("country", "int", ""));
    factColumns.add(new FieldSchema("region", "int", ""));
    factColumns.add(new FieldSchema("city", "int", ""));
    factColumns.add(new FieldSchema("count", "double", ""));
    factColumns.add(new FieldSchema("added", "double", ""));

    HiveConf hiveConf = new HiveConf();
    SessionState.start(hiveConf);
    Hive.get().dropTable("default.wikipedia");
    createHiveTable("default", "wikipedia", factColumns);
    Table tbl = CubeMetastoreClient.getInstance(hiveConf).getHiveTable("wikipedia");
    tbl.setProperty("druid.table.time.dimension", "time");

    String userQuery =
      "select country, sum(added) as added from db.wikipedia where time BETWEEN %s group by country";

    List<LensDriver> drivers = new ArrayList<>();
    drivers.add(druidDriver);

    QueryContext queryContext =
      new QueryContext(String.format(userQuery, getSampleTimeRange()), "rajitha", new LensConf(),
        new Configuration(hiveConf), drivers);

    druidDriver.execute(queryContext);
  }

  private static String getSampleTimeRange() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    Calendar cal = Calendar.getInstance();
    String endDate = dateFormat.format(cal.getTime());
    cal.add(Calendar.DAY_OF_MONTH, -2);
    String startDate = dateFormat.format(cal.getTime());
    return "'" + startDate + "'" + " AND " + "'" + endDate + "'";
  }

  public static void createHiveTable(String db, String table, List<FieldSchema> columns) throws Exception {
    Table tbl1 = new Table(db, table);
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    Assert.assertNotNull(Hive.get().getTable(db, table));
    // System.out.println("Created table : " + table);
  }
}
