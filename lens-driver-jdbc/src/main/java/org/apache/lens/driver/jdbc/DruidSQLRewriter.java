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
package org.apache.lens.driver.jdbc;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.parse.CubeSemanticAnalyzer;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.rewrite.QueryRewriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.antlr.runtime.CommonToken;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidSQLRewriter implements QueryRewriter {

  /** The ast. */
  protected ASTNode ast;

  /** The query. */
  protected String query;

  /** The limit. */
  private String limit;

  /** The rewritten query. */
  protected StringBuilder rewrittenQuery = new StringBuilder();

  /** The having ast. */
  @Getter
  private ASTNode havingAST;

  /** The select ast. */
  @Getter
  private ASTNode selectAST;

  /** The where ast. */
  @Getter
  private ASTNode whereAST;

  /** The order by ast. */
  @Getter
  private ASTNode orderByAST;

  /** The group by ast. */
  @Getter
  private ASTNode groupByAST;

  /** The from ast. */
  @Getter
  protected ASTNode fromAST;

  private HashMap<String, String> regexReplaceMap = new HashMap<>();
  /**
   * Whether to resolve native tables or not. In case the query has sub query, the outer query may not
   * require native table resolution
   *
   */
  boolean resolveNativeTables;

  /**
   * Instantiates a new columnar sql rewriter.
   */
  public DruidSQLRewriter() {
  }

  @Override
  public void init(Configuration conf) {
    if (conf.get(JDBCDriverConfConstants.REGEX_REPLACEMENT_VALUES) != null) {
      for (String kv : conf.get(JDBCDriverConfConstants.REGEX_REPLACEMENT_VALUES).split("(?<!\\\\),")) {
        String[] kvArray = kv.split("=");
        String key = kvArray[0].replaceAll("\\\\,", ",").trim();
        String value = kvArray[1].replaceAll("\\\\,", ",").trim();
        regexReplaceMap.put(key, value);
      }
    }
  }

  /**
   * Analyze internal.
   *
   * @throws SemanticException the semantic exception
   */
  public void analyzeInternal(Configuration conf, HiveConf hconf) throws SemanticException {
    CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf, hconf);

    QB qb = new QB(null, null, false);

    if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx(), null)) {
      return;
    }

    if (!qb.getSubqAliases().isEmpty()) {
      log.warn("Subqueries in from clause is not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Subqueries in from clause is not supported by " + this + " Query : " + this.query);
    }

    // Get clause name
    TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
    /* The clause name. */
    String clauseName = ks.first();

    if (qb.getParseInfo().getJoinExpr() != null) {
      log.warn("Join queries not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Join queries not supported by " + this + " Query : " + this.query);
    }
    // Split query into trees
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }

    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }

    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }
    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }

    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    this.fromAST = HQLParser.findNodeByPath(ast, TOK_FROM);

  }

  /**
   * Gets the limit clause.
   *
   * @param node the node
   * @return the limit clause
   */
  public String getLimitClause(ASTNode node) {

    if (node.getToken().getType() == HiveParser.TOK_LIMIT) {
      ASTNode limitNode = HQLParser.findNodeByPath(node, HiveParser.Number);
      if (null != limitNode) {
        limit = limitNode.toString();
      }
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getLimitClause(child);
    }
    return limit;
  }

  /*
   * Reset the instance variables if input query is union of multiple select queries
   */
  public void reset() {
    selectAST = null;
    fromAST = null;
    whereAST = null;
    groupByAST = null;
    havingAST = null;
    orderByAST = null;
    limit = null;
  }

  /*
   * Check the incompatible hive udf and replace it with database udf.
   */

  /**
   * Replace udf for db.
   *
   * @param query the query
   * @return the string
   */
  public String replaceUDFForDB(String query) {
    for (Map.Entry<String, String> entry : regexReplaceMap.entrySet()) {
      query = query.replaceAll(entry.getKey(), entry.getValue());
    }
    return query;
  }

  /*
   * Construct the rewritten query using trees
   */

  /**
   * Builds the query.
   *
   * @throws SemanticException
   */
  public void buildQuery(Configuration conf, HiveConf hconf) throws SemanticException, LensException {
    analyzeInternal(conf, hconf);
    if (resolveNativeTables) {
      replaceWithUnderlyingStorage(hconf);
    }

    // Get the limit clause
    String limit = getLimitClause(ast);

    ArrayList<String> filters = new ArrayList<>();
    getWhereString(whereAST, filters);

    // construct query with fact sub query
    constructQuery(HQLParser.getString(selectAST, new HQLParser.NochangeAppendMode()), filters,
      HQLParser.getString(groupByAST, new HQLParser.NochangeAppendMode()),
      HQLParser.getString(havingAST, new HQLParser.NochangeAppendMode()),
      HQLParser.getString(orderByAST, new HQLParser.NochangeAppendMode()), limit);

  }

  private ArrayList<String> getWhereString(ASTNode node, ArrayList<String> filters) throws LensException {

    if (node == null) {
      return null;
    }
    if (node.getToken().getType() == HiveParser.KW_AND) {
      // left child is "and" and right child is subquery
      if (node.getChild(0).getType() == HiveParser.KW_AND) {
        filters.add(getfilterSubquery(node, 1));
      } else if (node.getChildCount() > 1) {
        for (int i = 0; i < node.getChildCount(); i++) {
          filters.add(getfilterSubquery(node, i));
        }
      }
    } else if (node.getParent().getType() == HiveParser.TOK_WHERE
      && node.getToken().getType() != HiveParser.KW_AND) {
      filters.add(HQLParser.getString((ASTNode) node));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      return getWhereString(child, filters);
    }
    return filters;
  }

  private String getfilterSubquery(ASTNode node, int index) throws LensException {
    String filter;
    if (node.getChild(index).getType() == HiveParser.TOK_SUBQUERY_EXPR) {
      log.warn("Subqueries in where clause not supported by {} Query : {}", this, this.query);
      throw new LensException("Subqueries in where clause not supported by " + this + " Query : " + this.query);
    } else {
      filter = HQLParser.getString((ASTNode) node.getChild(index));
    }
    return filter;
  }

  /**
   * Construct final query using all trees
   *
   * @param selecttree  the selecttree
   * @param whereFilters   the wheretree
   * @param groupbytree the groupbytree
   * @param havingtree  the havingtree
   * @param orderbytree the orderbytree
   * @param limit       the limit
   */
  private void constructQuery(
    String selecttree, ArrayList<String> whereFilters, String groupbytree,
    String havingtree, String orderbytree, String limit) {

    log.info("In construct query ..");

    rewrittenQuery.append("select ").append(selecttree.replaceAll("`", "\"")).append(" from ");

    String factNameAndAlias = getFactNameAlias(fromAST);

    rewrittenQuery.append(factNameAndAlias);

    if (!whereFilters.isEmpty()) {
      rewrittenQuery.append(" where ").append(StringUtils.join(whereFilters, " and "));
    }
    if (StringUtils.isNotBlank(groupbytree)) {
      rewrittenQuery.append(" group by ").append(groupbytree);
    }
    if (StringUtils.isNotBlank(havingtree)) {
      rewrittenQuery.append(" having ").append(havingtree);
    }
    if (StringUtils.isNotBlank(orderbytree)) {
      rewrittenQuery.append(" order by ").append(orderbytree);
    }
    if (StringUtils.isNotBlank(limit)) {
      rewrittenQuery.append(" limit ").append(limit);
    }
  }

  /**
   * Gets the fact name alias.
   *
   * @param fromAST the from ast
   * @return the fact name alias
   */
  public String getFactNameAlias(ASTNode fromAST) {
    String factTable;
    String factAlias;
    ArrayList<String> allTables = new ArrayList<>();
    getAllTablesfromFromAST(fromAST, allTables);

    String[] keys = allTables.get(0).trim().split(" +");
    if (keys.length == 2) {
      factTable = keys[0];
      factAlias = keys[1];
      return factTable + " " + factAlias;
    } else {
      factTable = keys[0];
    }
    return factTable;
  }

  /**
   * Gets the all tablesfrom from ast.
   *
   * @param from       the from
   * @param fromTables the from tables
   * @return the all tablesfrom from ast
   */
  private void getAllTablesfromFromAST(ASTNode from, ArrayList<String> fromTables) {
    String table;
    if (TOK_TABREF == from.getToken().getType()) {
      ASTNode tabName = (ASTNode) from.getChild(0);
      if (tabName.getChildCount() == 2) {
        table = tabName.getChild(0).getText() + "." + tabName.getChild(1).getText();
      } else {
        table = tabName.getChild(0).getText();
      }
      if (from.getChildCount() > 1) {
        table = table + " " + from.getChild(1).getText();
      }
      fromTables.add(table);
    }

    for (int i = 0; i < from.getChildCount(); i++) {
      ASTNode child = (ASTNode) from.getChild(i);
      getAllTablesfromFromAST(child, fromTables);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.QueryRewriter#rewrite(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public String rewrite(String query, Configuration conf, HiveConf metastoreConf) throws LensException {
    this.query = query;
    String reWritten = rewrite(HQLParser.parseHQL(query, metastoreConf), conf, metastoreConf, true);

    log.info("Rewritten : {}", reWritten);
    String queryReplacedUdf = replaceUDFForDB(reWritten);
    log.info("Input Query : {}", query);
    log.info("Rewritten Query : {}", queryReplacedUdf);
    return queryReplacedUdf;
  }

  public String rewrite(ASTNode currNode, Configuration conf, HiveConf metastoreConf, boolean resolveNativeTables)
    throws LensException {
    this.resolveNativeTables = resolveNativeTables;
    rewrittenQuery.setLength(0);
    reset();
    this.ast = currNode;

    ASTNode fromNode = HQLParser.findNodeByPath(currNode, TOK_FROM);
    if (fromNode != null) {
      if (fromNode.getChild(0).getType() == TOK_SUBQUERY) {
        log.warn("Subqueries in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Subqueries in from clause not supported by " + this + " Query : " + this.query);
      } else if (isOfTypeJoin(fromNode.getChild(0).getType())) {
        log.warn("Join in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Join in from clause not supported by " + this + " Query : " + this.query);
      }
    }

    if (currNode.getToken().getType() == TOK_UNIONALL) {
      log.warn("Union queries are not supported by {} Query : {}", this, this.query);
      throw new LensException("Union queries are not supported by " + this + " Query : " + this.query);
    }

    String rewritternQueryText = rewrittenQuery.toString();
    if (currNode.getToken().getType() == TOK_QUERY) {
      try {
        buildQuery(conf, metastoreConf);
        rewritternQueryText = rewrittenQuery.toString();
        log.info("Rewritten query from build : " + rewritternQueryText);
      } catch (SemanticException e) {
        throw new LensException(e);
      }
    }
    return rewritternQueryText;
  }

  private boolean isOfTypeJoin(int type) {
    return (type == TOK_JOIN || type == TOK_LEFTOUTERJOIN || type == TOK_RIGHTOUTERJOIN
      || type == TOK_FULLOUTERJOIN || type == TOK_LEFTSEMIJOIN || type == TOK_UNIQUEJOIN);
  }

  @NoArgsConstructor
  private static class NativeTableInfo {
    private Map<String, String> columnMapping = new LinkedHashMap<>();

    NativeTableInfo(Table tbl) {
      String columnMappingProp = tbl.getProperty(LensConfConstants.NATIVE_TABLE_COLUMN_MAPPING);
      if (StringUtils.isNotBlank(columnMappingProp)) {
        String[] columnMapArray = StringUtils.split(columnMappingProp, ",");
        for (String columnMapEntry : columnMapArray) {
          String[] mapEntry = StringUtils.split(columnMapEntry, "=");
          columnMapping.put(mapEntry[0].trim(), mapEntry[1].trim());
        }
      }
    }

    String getNativeColumn(String col) {
      String retCol = columnMapping.get(col);
      return retCol != null ? retCol : col;
    }
  }

  private Map<String, NativeTableInfo> aliasToNativeTableInfo = new LinkedHashMap<>();

  /**
   * Replace with underlying storage.
   *
   * @param metastoreConf the metastore configuration
   */
  protected void replaceWithUnderlyingStorage(HiveConf metastoreConf) throws LensException {
    replaceDBAndTableNames(metastoreConf, fromAST);
    if (aliasToNativeTableInfo.isEmpty()) {
      return;
    }
    replaceColumnNames(selectAST);
    replaceColumnNames(fromAST);
    replaceColumnNames(whereAST);
    replaceColumnNames(groupByAST);
    replaceColumnNames(orderByAST);
    replaceColumnNames(havingAST);
  }

  // Replace Lens database names with storage's proper DB and table name based
  // on table properties.
  protected void replaceDBAndTableNames(HiveConf metastoreConf, ASTNode tree) throws LensException {
    if (tree == null) {
      return;
    }

    if (TOK_TABREF == tree.getToken().getType()) {
      // TOK_TABREF will have TOK_TABNAME as first child and alias as second child.
      String alias;
      String tblName = null;
      Table tbl = null;
      ASTNode tabNameChild = (ASTNode) tree.getChild(0);
      if (TOK_TABNAME == tabNameChild.getToken().getType()) {
        // If it has two children, the first one is the DB name and second one is
        // table identifier
        // Else, we have to add the DB name as the first child
        try {
          if (tabNameChild.getChildCount() == 2) {
            ASTNode dbIdentifier = (ASTNode) tabNameChild.getChild(0);
            ASTNode tableIdentifier = (ASTNode) tabNameChild.getChild(1);
            tblName = tableIdentifier.getText();
            String lensTable = dbIdentifier.getText() + "." + tblName;
            tbl = CubeMetastoreClient.getInstance(metastoreConf).getHiveTable(lensTable);
            String table = getUnderlyingTableName(tbl);
            String db = getUnderlyingDBName(tbl);

            // Replace both table and db names
            if ("default".equalsIgnoreCase(db)) {
              // Remove the db name for this case
              tabNameChild.deleteChild(0);
            } else if (StringUtils.isNotBlank(db)) {
              dbIdentifier.getToken().setText(db);
            } // If db is empty, then leave the tree untouched

            if (StringUtils.isNotBlank(table)) {
              tableIdentifier.getToken().setText(table);
            }
          } else {
            ASTNode tableIdentifier = (ASTNode) tabNameChild.getChild(0);
            tblName = tableIdentifier.getText();
            tbl = CubeMetastoreClient.getInstance(metastoreConf).getHiveTable(tblName);
            String table = getUnderlyingTableName(tbl);
            // Replace table name
            if (StringUtils.isNotBlank(table)) {
              tableIdentifier.getToken().setText(table);
            }

            // Add db name as a new child
            String dbName = getUnderlyingDBName(tbl);
            if (StringUtils.isNotBlank(dbName) && !"default".equalsIgnoreCase(dbName)) {
              ASTNode dbIdentifier = new ASTNode(new CommonToken(HiveParser.Identifier, dbName));
              dbIdentifier.setParent(tabNameChild);
              tabNameChild.insertChild(0, dbIdentifier);
            }
          }
        } catch (LensException | HiveException e) {
          log.warn("No corresponding table in metastore:", e);
        }
      }
      if (tree.getChildCount() == 2) {
        alias = tree.getChild(1).getText();
      } else {
        alias = tblName;
      }
      if (StringUtils.isNotBlank(alias)) {
        alias = alias.toLowerCase();
        if (!aliasToNativeTableInfo.containsKey(alias)) {
          if (tbl != null) {
            aliasToNativeTableInfo.put(alias, new NativeTableInfo(tbl));
            log.info("Put native table info in map : " + alias + "Table   :" + tbl);
          }
        }
      }
    } else {
      for (int i = 0; i < tree.getChildCount(); i++) {
        replaceDBAndTableNames(metastoreConf, (ASTNode) tree.getChild(i));
      }
    }
  }

  void replaceColumnNames(ASTNode node) {
    if (node == null) {
      return;
    }
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.DOT) {
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);
      String column = colIdent.getText().toLowerCase();
      String alias = tabident.getText().toLowerCase();
      log.info("Map : " + aliasToNativeTableInfo);
      if (aliasToNativeTableInfo.get(alias) != null) {
        log.info("Replacing  alias : " + alias + " col : " + column + "Rplacing by : " + aliasToNativeTableInfo
          .get(alias).getNativeColumn(column));
        colIdent.getToken().setText(aliasToNativeTableInfo.get(alias).getNativeColumn(column));
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceColumnNames(child);
      }
    }
  }

  /**
   * Gets the underlying db name.
   *
   * @param tbl the table
   * @return the underlying db name
   * @throws HiveException the hive exception
   */
  String getUnderlyingDBName(Table tbl) throws HiveException {
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_DB_NAME);
  }

  /**
   * Gets the underlying table name.
   *
   * @param tbl the table
   * @return the underlying table name
   * @throws HiveException the hive exception
   */
  String getUnderlyingTableName(Table tbl) throws HiveException {
    return tbl == null ? null : tbl.getProperty(LensConfConstants.NATIVE_TABLE_NAME);
  }

}
