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

import org.apache.lens.server.api.driver.DefaultResultSet;

import org.apache.hadoop.conf.Configuration;

import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.client.DruidClient;

class MockClientDruid extends DruidClient {

  public MockClientDruid(DruidDriverConfig druidDriverConfig, Configuration conf) {
    super(druidDriverConfig, conf);
  }

  @Override
  public DefaultResultSet executeImpl(DruidQuery druidQuery) {
    return null;
  }

}
