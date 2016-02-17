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
 * "AS IS" BASIS, WITHOUT WARRANTIDruid OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.druid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.annotations.BeforeTest;

import com.apache.lens.driver.druid.DruidDriver;
import com.apache.lens.driver.druid.DruidDriverConfig;

public abstract class DruidInitDriverTest {

  protected Configuration config = new Configuration();
  protected DruidDriverConfig druidDriverConfig;
  protected HiveConf hiveConf = new HiveConf();
  protected DruidDriver driver = new DruidDriver();
  protected MockClientDruid mockClientDruid;

  @BeforeTest
  public void beforeTest() throws Exception {
    initializeConfig(config);
    druidDriverConfig = new DruidDriverConfig(config);
    driver.configure(config, "druid", "druid");
    mockClientDruid = (MockClientDruid) driver.getDruidClient();
  }

  protected abstract void initializeConfig(Configuration config);

}
