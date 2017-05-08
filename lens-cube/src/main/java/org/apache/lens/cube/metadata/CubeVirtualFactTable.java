/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata;

import java.util.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;

import lombok.Getter;
import lombok.Setter;

public class CubeVirtualFactTable extends AbstractCubeTable implements FactTable {

  @Getter
  @Setter
  private CubeFactTable sourceCubeFactTable;
  private String cubeName;
  private static final List<FieldSchema> COLUMNS = new ArrayList<FieldSchema>();
  @Getter
  private Optional<Double> virtualFactWeight = Optional.absent();

  static {
    COLUMNS.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public CubeVirtualFactTable(Table hiveTable, Table sourceHiveTable) {
    super(hiveTable);
    this.cubeName = getFactCubeName(getName(), getProperties());
    this.sourceCubeFactTable = new CubeFactTable(sourceHiveTable);

    this.virtualFactWeight = Optional.absent();
    String wtStr = getProperties().get(MetastoreUtil.getCubeTableWeightKey(getName()));
    if (wtStr != null) {
      this.virtualFactWeight = Optional.of(Double.parseDouble(wtStr));
    }
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, Optional<Double> weight,
    Map<String, String> properties, CubeFactTable sourceFact) {
    super(virtualFactName, COLUMNS, properties, weight.isPresent() ? weight.get() : sourceFact.weight());
    this.cubeName = cubeName;
    this.virtualFactWeight = weight;
    this.sourceCubeFactTable = sourceFact;
    addProperties();
  }

  /**
   * Alters the weight of table
   *
   * @param weight Weight of the table.
   */
  @Override
  public void alterWeight(double weight) {
    this.virtualFactWeight = Optional.of(weight);
    this.addProperties();
  }

  @Override
  protected void addProperties() {
    getProperties().put(MetastoreConstants.TABLE_TYPE_KEY, getTableType().name());
    if (virtualFactWeight.isPresent()) {
      getProperties().put(MetastoreUtil.getCubeTableWeightKey(this.getName()), String.valueOf(virtualFactWeight.get()));
    }
    addCubeNames(getName(), getProperties(), cubeName);
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.VIRTUAL_FACT;
  }

  @Override
  public List<String> getValidColumns() {
    return this.sourceCubeFactTable.getValidColumns();
  }

  @Override
  public Set<String> getStorages() {
    return this.sourceCubeFactTable.getStorages();
  }

  @Override
  public Map<String, Set<UpdatePeriod>> getUpdatePeriods() {
    return this.sourceCubeFactTable.getUpdatePeriods();
  }

  @Override
  public String getCubeName() {
    return this.cubeName;
  }

  @Override
  public String getDataCompletenessTag() {
    return this.sourceCubeFactTable.getDataCompletenessTag();
  }

  @Override
  public boolean isAggregated() {
    return this.sourceCubeFactTable.isAggregated();
  }

  @Override
  public List<FieldSchema> getColumns() {
    return this.sourceCubeFactTable.getColumns();
  }

  @Override
  public double weight() {
    return virtualFactWeight.isPresent() ? virtualFactWeight.get() : sourceCubeFactTable.weight();
  }
}