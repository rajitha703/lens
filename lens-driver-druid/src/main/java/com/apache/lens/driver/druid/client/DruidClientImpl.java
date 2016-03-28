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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.lens.server.api.driver.DefaultResultSet;
import org.apache.hadoop.conf.Configuration;

import com.apache.lens.driver.druid.DruidDriverConfig;
import com.apache.lens.driver.druid.DruidQuery;
import com.apache.lens.driver.druid.exceptions.DruidClientException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.metamx.common.StringUtils;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Result;
import io.druid.query.topn.TopNResultValue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidClientImpl extends DruidClient {

  private static final String DRUID_BROKER_URL = "lens.driver.druid.broker.url";

  public DruidClientImpl(DruidDriverConfig druidDriverConfig, Configuration conf) {
    super(druidDriverConfig, conf);
  }

  @Override
  public DefaultResultSet executeImpl(DruidQuery druidQuery) throws DruidClientException {
    log.info("Executing query on Druid client : " + druidQuery.toString());
    ObjectMapper objectMapper = new DefaultObjectMapper();
    ObjectWriter jsonWriter = objectMapper.writerWithDefaultPrettyPrinter();
    String queryStr = null;
    BufferedReader stdInput = null;
    HttpURLConnection urlConnection = null;
    try {
      queryStr = jsonWriter.writeValueAsString(druidQuery.getQuery());

      URL url = new URL(config.get(DRUID_BROKER_URL));
      urlConnection = (HttpURLConnection) url.openConnection();
      urlConnection.setDoInput(true);
      urlConnection.setDoOutput(true);
      urlConnection.setRequestMethod("POST");
      urlConnection.addRequestProperty("content-type", MediaType.APPLICATION_JSON);
      urlConnection.getOutputStream().write(StringUtils.toUtf8(queryStr));
      stdInput = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), Charsets.UTF_8));

      TypeReference typeRef = null;

      if (druidQuery.getQueryType().equals(DruidQuery.QueryType.TOPN)) {
        typeRef = new TypeReference<List<Result<TopNResultValue>>>() {
        };
      } else {
        typeRef = new TypeReference<List<Row>>() {
        };
      }

      Object result = objectMapper.readValue(stdInput, typeRef);

      return DruidResultSetTransformer.getTransformer(druidQuery.getQueryType(), result, druidQuery.getColumnSchema())
        .transform();

    } catch (Exception e) {
      throw new DruidClientException("Druid client execution failed, ", e);
    } finally {
      if (null != stdInput) {
        try {
          stdInput.close();
        } catch (IOException ioe) {
          log.error("Exception while closing inputStream ", ioe);
        }
      }
      if (null != urlConnection) {
        urlConnection.disconnect();
      }
    }
  }
}
