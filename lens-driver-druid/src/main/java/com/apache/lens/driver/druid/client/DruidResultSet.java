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

import java.util.Iterator;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.error.LensException;

import lombok.NonNull;

public class DruidResultSet extends InMemoryResultSet {

  @NonNull
  final Iterator<ResultRow> resultSetIterator;
  @NonNull
  final LensResultSetMetadata resultSetMetadata;
  final Integer size;

  public DruidResultSet(int size, final Iterable<ResultRow> resultSetIterable, final LensResultSetMetadata metadata) {
    this.size = size;
    this.resultSetIterator = resultSetIterable.iterator();
    this.resultSetMetadata = metadata;
  }

  @Override
  public boolean hasNext() throws LensException {
    return resultSetIterator.hasNext();
  }

  @Override
  public ResultRow next() throws LensException {
    return resultSetIterator.next();
  }

  @Override
  public void setFetchSize(int i) throws LensException {

  }

  @Override
  public Integer size() throws LensException {
    return this.size;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return this.resultSetMetadata;
  }
}
