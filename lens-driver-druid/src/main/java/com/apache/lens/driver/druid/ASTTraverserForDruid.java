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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.driver.lib.ASTCriteriaVisitor;
import org.apache.lens.server.api.driver.lib.ASTVisitor;
import org.apache.lens.server.api.driver.lib.CriteriaVisitorFactory;
import org.apache.lens.server.api.driver.lib.exception.InvalidQueryException;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import com.apache.lens.driver.druid.translator.DruidVisitor;
import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ASTTraverserForDruid {

  /**
   * the root node of the ASTNode
   */
  @NonNull
  private final ASTNode rootQueryNode;
  /**
   * The basic query visitor
   */
  @NonNull
  private final ASTVisitor visitor;
  /**
   * the criteria visitor factory,
   * traversal has to create multiple criteria visitor objects for
   * nested criteria. The impl of factory would determine the type of
   * criteria visitor
   */
  @NonNull
  private final CriteriaVisitorFactory criteriaVisitorFactory;
  /**
   * the having criteria visitor factory,
   * traversal has to create multiple criteria visitor objects for
   * nested criteria. The impl of factory would determine the type of
   * criteria visitor
   */
  @NonNull
  private final CriteriaVisitorFactory havingCriteriaVisitorFactory;

  public void accept() throws InvalidQueryException {
    traverseTableName();
    traverseSelects();
    traverseCriteria();
    traverseGroupBy();
    traverseLimit();
    traverseOrderBy();
    traverseHavingCriteria();
    visitor.completeVisit();
  }

  /**
   * Visit select expressions
   */
  public void traverseSelects() throws InvalidQueryException {
    final ASTNode selectNode = HQLParser.findNodeByPath(rootQueryNode, HiveParser.TOK_INSERT, HiveParser.TOK_SELECT);
    if (selectNode == null) {
      throw new InvalidQueryException("No columns are selected!");
    }
    try {
      for (Node selectExp : selectNode.getChildren()) {
        final Node innerNode = Helper.getFirstChild(selectExp);
        final String alias = Helper.getAliasFromSelectExpr(selectExp);
        if (innerNode.getName().equals(String.valueOf(HiveParser.TOK_FUNCTION))) {
          Validate.isTrue(innerNode.getChildren().size() == 2);
          visitor.visitAggregation(
            Helper.getFirstChild(innerNode).toString(),
            Helper.getColumnNameFrom(innerNode.getChildren().get(1)),
            alias
          );
        } else if (innerNode.getName().equals(String.valueOf(HiveParser.TOK_ALLCOLREF))) {
          visitor.visitAllCols();
        } else if (innerNode.getName().equals(String.valueOf(HiveParser.TOK_TABLE_OR_COL))
          || innerNode.toString().equals(".")) {
          visitor.visitSimpleSelect(
            Helper.getColumnNameFrom(innerNode),
            alias
          );
        } else {
          throw new InvalidQueryException(selectExp.getName() + " seems to be invalid");
        }
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Exception while traversing select expressions", e);
    }
  }

  /**
   * Visit table name
   */
  private void traverseTableName() throws InvalidQueryException {
    try {
      final ASTNode dbSchemaTable = HQLParser.findNodeByPath(
        rootQueryNode,
        HiveParser.TOK_FROM,
        HiveParser.TOK_TABREF,
        HiveParser.TOK_TABNAME);
      Validate.notNull(dbSchemaTable, "Index and type not present");
      Validate.isTrue(dbSchemaTable.getChildren().size() == 2, "Index and type not present");
      final String dbSchema = dbSchemaTable.getChild(0).getText();
      final String tableName = dbSchemaTable.getChild(1).getText();
      visitor.visitFrom(dbSchema, tableName);
    } catch (Exception e) {
      throw new InvalidQueryException("Error while traversing table name "
        + "- Expected grammar .. from <index>.<type>", e);
    }
  }

  /**
   * Visit criteria
   */
  private void traverseCriteria() throws InvalidQueryException {
    try {
      final ASTNode criteriaNode = HQLParser.findNodeByPath(rootQueryNode,
        HiveParser.TOK_INSERT, HiveParser.TOK_WHERE);
      if (criteriaNode != null) {
        visitor.visitCriteria(traverseCriteriaRecursively(criteriaVisitorFactory, Helper.getFirstChild(criteriaNode)));
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Exception while traversing criteria", e);
    }
  }

  /**
   * Visit having criteria
   */
  private void traverseHavingCriteria() throws InvalidQueryException {
    try {
      final ASTNode criteriaNode = HQLParser.findNodeByPath(rootQueryNode,
        HiveParser.TOK_INSERT, HiveParser.TOK_HAVING);
      if (criteriaNode != null) {
        visitor
          .visitHaving(traverseCriteriaRecursively(havingCriteriaVisitorFactory, Helper.getFirstChild(criteriaNode)));
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Exception while traversing criteria", e);
    }
  }

  private ASTCriteriaVisitor traversePredicate(CriteriaVisitorFactory visitorfactory, Node whereClause, PredicateInfo
    predicateInfo)
    throws InvalidQueryException, HiveException {
    final ASTCriteriaVisitor childVisitor = visitorfactory.getInstance();
    final ArrayList<String> rightExpressions = Lists.newArrayList();
    final List<? extends Node> rightExpList = whereClause.getChildren();
    String leftCol;
    switch (predicateInfo.predicateType) {
    case BETWEEN:
      Validate.isTrue(rightExpList.size() == 5, "Atleast one right expression needed");
      leftCol = whereClause.getChildren().get(2).getChildren().get(0).toString();
      if (((DruidVisitor) visitor).isTimeDimension(leftCol)) {
        String startInstant = whereClause.getChildren().get(3).toString();
        String endInstant = whereClause.getChildren().get(4).toString();
        ((DruidVisitor) visitor).visitStartTimeInstant(DruidVisitor.trimValue(startInstant));
        ((DruidVisitor) visitor).visitEndTimeInstant(DruidVisitor.trimValue(endInstant));

      } else {
        throw new UnsupportedOperationException("Between not supported except for timedimension in Druid");
      }
      break;
    case SIMPLE:
      Validate.isTrue(rightExpList.size() > 1, "Atleast one right expression needed");
      for (Node rightExp : rightExpList.subList(1, rightExpList.size())) {
        rightExpressions.add(rightExp.toString());
      }
      leftCol = Helper.getLeftColFromPredicate(whereClause);
      childVisitor.visitPredicate(predicateInfo.predicateOp, leftCol, rightExpressions);
      break;
    default:
      throw new InvalidQueryException("No handlers for predicate " + predicateInfo.predicateType);
    }
    return childVisitor;
  }

  private ASTCriteriaVisitor traverseLogicalOperator(CriteriaVisitorFactory visitorFactory, Node whereClause,
                                                     LogicalOpInfo
                                                       logicalOpInfo) throws InvalidQueryException, HiveException {
    final ASTCriteriaVisitor childVisitor = visitorFactory.getInstance();
    final List<ASTCriteriaVisitor> childVisitors = Lists.newArrayList();
    for (Node node : whereClause.getChildren()) {
      childVisitors.add(traverseCriteriaRecursively(visitorFactory, node));
    }
    childVisitor.visitLogicalOp(logicalOpInfo.logicalOperator, childVisitors);
    return childVisitor;
  }

  private ASTCriteriaVisitor traverseCriteriaRecursively(CriteriaVisitorFactory visitorFactory, Node whereClause)
    throws InvalidQueryException, HiveException {
    final CriteriaInfo criteriaInfo = Helper.getCriteriaInfo(whereClause);
    switch (criteriaInfo.criteriaType) {
    case PREDICATE:
      return traversePredicate(visitorFactory, whereClause, (PredicateInfo) criteriaInfo);
    case LOGICAL:
      return traverseLogicalOperator(visitorFactory, whereClause, (LogicalOpInfo) criteriaInfo);
    default:
      throw new InvalidQueryException("Expecting a predicate or logical operator but got this "
        + whereClause.toString());
    }
  }

  /**
   * Visit group by
   */
  private void traverseGroupBy() throws InvalidQueryException {
    try {
      final ASTNode groupByNode = HQLParser.findNodeByPath(rootQueryNode,
        HiveParser.TOK_INSERT, HiveParser.TOK_GROUPBY);
      if (groupByNode != null) {
        for (Node groupBy : groupByNode.getChildren()) {
          visitor.visitGroupBy(Helper.getColumnNameFrom(groupBy));
        }
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Exception while parsing group by", e);
    }
  }

  /**
   * Visit order by
   */
  private void traverseOrderBy() throws InvalidQueryException {
    try {
      final ASTNode orderByNode = HQLParser.findNodeByPath(rootQueryNode,
        HiveParser.TOK_INSERT, HiveParser.TOK_ORDERBY);
      if (orderByNode != null) {
        for (Node orderBy : orderByNode.getChildren()) {
          visitor.visitOrderBy(
            Helper.getColumnNameFrom(Helper.getFirstChild(orderBy)),
            orderBy.getName().equals(String.valueOf(HiveParser.TOK_TABSORTCOLNAMEDESC))
              ?
              ASTVisitor.OrderBy.DESC
              :
              ASTVisitor.OrderBy.ASC
          );
        }
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Exception while parsing order by", e);
    }
  }

  /**
   * Visit limit
   */
  private void traverseLimit() throws InvalidQueryException {
    try {
      final ASTNode limitNode = HQLParser.findNodeByPath(rootQueryNode,
        HiveParser.TOK_INSERT, HiveParser.TOK_LIMIT);
      if (limitNode != null) {
        visitor.visitLimit(Integer.parseInt(Helper.getFirstChild(limitNode).toString()));
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Error while parsing limit, format should be limit <int>", e);
    }
  }

  private enum PredicateType {SIMPLE, BETWEEN}

  private enum CriteriaType {PREDICATE, LOGICAL}

  private enum LogicalOpType {UNARY, BINARY}

  private static class CriteriaInfo {
    final CriteriaType criteriaType;

    public CriteriaInfo(CriteriaType criteriaType) {
      this.criteriaType = criteriaType;
    }
  }

  private static class LogicalOpInfo extends CriteriaInfo {
    final String logicalOperator;
    final LogicalOpType logicalOpType;

    public LogicalOpInfo(String logicalOperator, LogicalOpType logicalOpType) {
      super(CriteriaType.LOGICAL);
      this.logicalOperator = logicalOperator;
      this.logicalOpType = logicalOpType;
    }
  }

  private static class PredicateInfo extends CriteriaInfo {
    final PredicateType predicateType;
    final String predicateOp;


    public PredicateInfo(String operator, PredicateType predicateType) {
      super(CriteriaType.PREDICATE);
      this.predicateType = predicateType;
      this.predicateOp = operator;
    }
  }

  private static class Helper {

    private static List<String> predicates
      = Lists.newArrayList("!=", "=", ">", "<");
    private static List<String> unaryLogicalOps = Lists.newArrayList("not", "!");
    private static List<String> binaryLogicalOps = Lists.newArrayList("and", "or", "&", "|", "&&", "||", "AND",
      "OR");
    private static List<String> logicalOps = Lists.newArrayList();

    static {
      logicalOps.addAll(unaryLogicalOps);
      logicalOps.addAll(binaryLogicalOps);
    }

    private static String getAliasFromSelectExpr(Node selectExp) {
      return selectExp.getChildren().size() == 2
        ?
        selectExp.getChildren().get(1).toString()
        :
        null;
    }

    private static CriteriaInfo getCriteriaInfo(Node whereClause) throws InvalidQueryException {
      String whereRoot = whereClause.toString();
      if (Helper.unaryLogicalOps.contains(whereRoot)) {
        return new LogicalOpInfo(whereRoot, LogicalOpType.UNARY);
      } else if (Helper.binaryLogicalOps.contains(whereRoot)) {
        return new LogicalOpInfo(whereRoot, LogicalOpType.BINARY);
      } else if (Helper.predicates.contains(whereRoot)) {
        return new PredicateInfo(whereRoot, PredicateType.SIMPLE);
      } else if (whereRoot.equals("TOK_FUNCTION") && whereClause.getChildren().get(0).toString().equals("between")) {
        return new PredicateInfo("between", PredicateType.BETWEEN);
      } else {
        throw new InvalidQueryException("Could not get criteria info for where clause " + whereRoot);
      }
    }

    private static Node getFirstChild(Node node) throws LensException {
      try {
        return node.getChildren().get(0);
      } catch (Exception e) {
        throw new LensException("Expecting a non empty first child for " + node.toString(), e);
      }
    }

    private static String getLeftColFromPredicate(Node predicateNode) throws InvalidQueryException {
      try {
        return getColumnNameFrom(getFirstChild(predicateNode));
      } catch (Exception e) {
        throw new InvalidQueryException("Only simple predicates of the grammar <col>=<val> is supported as of now", e);
      }
    }

    private static String getColumnNameFrom(Node columnNode) {
      final StringBuilder stringBuilder = new StringBuilder();
      HQLParser.toInfixString((ASTNode) columnNode, stringBuilder);
      return stringBuilder.toString().replaceAll("[() ]", "");
    }
  }
}
