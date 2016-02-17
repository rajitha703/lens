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

import org.apache.hadoop.conf.Configuration;

import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.exceptions.DruidClientException;
import io.druid.query.Query;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DruidClient {

  @NonNull
  protected final DruidDriverConfig druidDriverConfig;
  @NonNull
  protected final Configuration config;

  public DruidClient(DruidDriverConfig druidDriverConfig, Configuration config) {
    this.druidDriverConfig = druidDriverConfig;
    this.config = config;
  }

  protected abstract DruidResultSet executeImpl(DruidQuery druidQuery) throws DruidClientException;

  public DruidResultSet execute(DruidQuery druidQuery) throws DruidClientException {
    return getQueryType(druidQuery).executeInternal();
  }

  private QueryMode getQueryType(DruidQuery druidQuery) {
    String type = druidQuery.getQuery().getType();
    if (type.equals(Query.GROUP_BY)) {
      return new GroupByQueryType(druidQuery);
    }
    if (type.equals(Query.TOPN)) {
      return new TopNQueryType(druidQuery);
    } else {
      log.error("Received query of type : " + type +"and this is not currently supported..");
      throw new UnsupportedOperationException("Query Type not supported in druid driver : " + type);
    }
  }

  private abstract static class QueryMode {

    @NonNull
    protected final DruidQuery druidQuery;

    QueryMode(DruidQuery druidQuery) {
      this.druidQuery = druidQuery;
    }

    abstract DruidResultSet executeInternal() throws DruidClientException;
  }

  private class GroupByQueryType extends QueryMode {

    GroupByQueryType(DruidQuery druidQuery) {
      super(druidQuery);
    }

    @Override
    DruidResultSet executeInternal() throws DruidClientException {
      return executeImpl(druidQuery);
    }
  }

  private class TopNQueryType extends QueryMode {

    TopNQueryType(DruidQuery druidQuery) {
      super(druidQuery);
    }

    @Override
    DruidResultSet executeInternal() throws DruidClientException {
      return executeImpl(druidQuery);
    }
  }
}
