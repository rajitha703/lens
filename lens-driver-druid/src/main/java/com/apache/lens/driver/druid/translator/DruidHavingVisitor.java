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
package com.apache.lens.driver.druid.translator;

import java.util.List;

import org.apache.lens.server.api.driver.lib.ASTCriteriaVisitor;
import org.apache.lens.server.api.driver.lib.exception.InvalidQueryException;

import org.apache.commons.lang.StringUtils;

import com.apache.lens.driver.druid.grammar.having.HavingLogicalOperators;
import com.apache.lens.driver.druid.grammar.having.HavingPredicates;
import com.google.common.collect.Lists;
import io.druid.query.groupby.having.HavingSpec;
import lombok.Getter;

public class DruidHavingVisitor implements ASTCriteriaVisitor {

  @Getter
  private HavingSpec havingSpec;

  @Override
  public void visitLogicalOp(String logicalOp, List<ASTCriteriaVisitor> visitedSubTrees) throws InvalidQueryException {
    this.havingSpec = HavingLogicalOperators.getFor(logicalOp)
      .build(collectFiltersFromVisitors(visitedSubTrees));
  }

  @Override
  public void visitPredicate(String predicateOp, String leftCanonical, List<String> rightExps) throws
    InvalidQueryException {
    final String leftCol = visitColumn(leftCanonical);
    this.havingSpec = HavingPredicates.getFor(predicateOp)
      .build(leftCol, trimValue(StringUtils.join(rightExps, "")));
  }

  private static String visitColumn(String cannonicalColName) {
    final String[] colParts = cannonicalColName.split("\\.");
    return colParts[colParts.length - 1];
  }

  private List<HavingSpec> collectFiltersFromVisitors(List<ASTCriteriaVisitor> visitedSubTrees) {
    final List<HavingSpec> subTrees = Lists.newArrayList();
    for (ASTCriteriaVisitor visitor : visitedSubTrees) {
      subTrees.add(((DruidHavingVisitor) visitor).getHavingSpec());
    }
    return subTrees;
  }

  private static String trimValue(String value) {
    return value.replaceAll("'", "");
  }

}

