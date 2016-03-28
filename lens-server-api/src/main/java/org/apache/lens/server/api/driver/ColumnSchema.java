/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * OR more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may NOT use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law OR agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express OR implied.  See the License for the
 * specific language governing permissions AND limitations
 * under the License.
 */
package org.apache.lens.server.api.driver;

import org.apache.hive.service.cli.Type;

import lombok.Data;
import lombok.NonNull;

@Data
public class ColumnSchema {
  @NonNull
  private String columnName;
  private String aliasName;
  private boolean isDimension;
  private boolean isMetric;
  private Type dataType;
}
