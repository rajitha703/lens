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

import org.apache.hadoop.conf.Configuration;

public final class DruidDriverConfig {

  public static final String CLIENT_CLASS_KEY = "lens.driver.druid.client.class";
  public static final String DATE_FORMAT = "lens.driver.druid.date.format";

  private static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";

  private final String dateFormat;

  public DruidDriverConfig(Configuration conf) {
    dateFormat = conf.get(DATE_FORMAT, DATE_FORMAT_DEFAULT);
  }

  public String getDateFormat() {
    return dateFormat;
  }
}
