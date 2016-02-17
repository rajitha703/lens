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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.apache.lens.driver.druid.client.DruidClient;
import com.apache.lens.driver.druid.client.DruidClientImpl;
import com.apache.lens.driver.druid.client.DruidResultSet;
import com.apache.lens.driver.druid.translator.DruidVisitor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class DruidDriver extends AbstractLensDriver {

  private static final AtomicInteger THID = new AtomicInteger();
  private static final double STREAMING_PARTITION_COST = 0;
  private static final QueryCost DRUID_DRIVER_COST = new FactPartitionBasedQueryCost(STREAMING_PARTITION_COST);

  /**
   * The conf.
   */
  private Configuration conf;

  private ExecutorService asyncQueryPool;
  private DruidDriverConfig druidDriverConfig;
  private HiveConf hiveConf;
  private DruidClient druidClient;
  private final Map<String, DruidQuery> rewrittenQueriesCache = Maps.newConcurrentMap();
  private final Map<QueryHandle, QueryCompletionListener> handleListenerMap = Maps.newConcurrentMap();
  private final Map<QueryHandle, Future<LensResultSet>> resultSetMap = Maps.newConcurrentMap();

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    this.conf = new Configuration(conf);
    this.conf.addResource("druiddriver-default.xml");

    druidDriverConfig = new DruidDriverConfig(this.conf);

    Class klass;
    try {
      klass = Class.forName(this.conf.get(DruidDriverConfig.CLIENT_CLASS_KEY));
      if (klass != null) {
        log.debug("Picked up class {}", klass);
        if (DruidClient.class.isAssignableFrom(klass)) {
          final Constructor constructor = klass.getConstructor(DruidDriverConfig.class, Configuration.class);
          constructor.setAccessible(true);
          druidClient = (DruidClient) constructor.newInstance(druidDriverConfig, this.conf);
          log.debug("Successfully instantiated druid client of type {}", klass);
        }
      } else {
        log.debug("Client class not provided, falling back to the default druid client");
        druidClient = new DruidClientImpl(druidDriverConfig, conf);
      }
    } catch (ClassNotFoundException
      | NoSuchMethodException
      | InstantiationException
      | IllegalAccessException
      | InvocationTargetException e) {
      log.error("Druid driver {} cannot start!", getFullyQualifiedName(), e);
      throw new LensException("Cannot start druid driver", e);
    }
    log.info("Druid Driver {} configured", getFullyQualifiedName());
    asyncQueryPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread th = new Thread(runnable);
        th.setName("lens-driver-druid-" + THID.incrementAndGet());
        return th;
      }
    });
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
    return DRUID_DRIVER_COST;
  }

  @Override
  public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
    throw new UnsupportedOperationException("Operation not supported in Druid");
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    rewrite(pContext);
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    throw new UnsupportedOperationException("Operation not supported in Druid");
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    throw new UnsupportedOperationException("Operation not supported in Druid");
  }

  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    log.info("Executing Druid query..");
    final DruidQuery rewrittenQuery = rewrite(context);
 //   final QueryHandle queryHandle = context.getQueryHandle();
    DruidResultSet druidResultSet = druidClient.execute(rewrittenQuery);
  //  notifyComplIfRegistered(queryHandle);
    return druidResultSet;
  }

  private void notifyComplIfRegistered(QueryHandle queryHandle) {
    try {
      handleListenerMap.get(queryHandle).onCompletion(queryHandle);
    } catch (NullPointerException e) {
      log.debug("There are no subscriptions for notification. Skipping for {}", queryHandle.getHandleIdString(), e);
    }
  }

  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    cancelQuery(handle);
    closeResultSet(handle);
    handleListenerMap.remove(handle);
  }

  private DruidQuery rewrite(AbstractQueryContext context) throws LensException {
    final String key = keyFor(context);
    if (rewrittenQueriesCache.containsKey(key)) {
      return rewrittenQueriesCache.get(key);
    } else {
      final ASTNode rootQueryNode = HQLParser.parseHQL(context.getDriverQuery(this), new HiveConf());
      final DruidQuery druidQuery = DruidVisitor.rewrite(druidDriverConfig, context.getHiveConf(), rootQueryNode);
      rewrittenQueriesCache.put(key, druidQuery);
      return druidQuery;
    }
  }

  private String keyFor(AbstractQueryContext context) {
    return String.valueOf(context.getFinalDriverQuery(this) != null) + ":" + context.getDriverQuery(this);
  }

  @Override
  public void executeAsync(final QueryContext context) {
    final Future<LensResultSet> futureResult
      = asyncQueryPool.submit(new DruidQueryExecuteCallable(context, SessionState.get()));
    resultSetMap.put(context.getQueryHandle(), futureResult);
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis,
                                                QueryCompletionListener listener) {
    handleListenerMap.put(handle, listener);
  }

  @Override
  public void updateStatus(QueryContext context) {
    final QueryHandle queryHandle = context.getQueryHandle();
    final Future<LensResultSet> lensResultSetFuture = resultSetMap.get(queryHandle);
    if (lensResultSetFuture == null) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.CLOSED);
      context.getDriverStatus().setStatusMessage(queryHandle + " closed");
      context.getDriverStatus().setResultSetAvailable(false);
    } else if (lensResultSetFuture.isDone()) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.SUCCESSFUL);
      context.getDriverStatus().setStatusMessage(queryHandle + " successful");
      context.getDriverStatus().setResultSetAvailable(true);
    } else if (lensResultSetFuture.isCancelled()) {
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.CANCELED);
      context.getDriverStatus().setStatusMessage(queryHandle + " cancelled");
      context.getDriverStatus().setResultSetAvailable(false);
    }
  }

  @Override
  public LensResultSet fetchResultSet(QueryContext context) throws LensException {
    try {
      /**
       * removing the result set as soon as the fetch is done
       */
      return resultSetMap.remove(context.getQueryHandle()).get();
    } catch (NullPointerException e) {
      throw new LensException("The results for the query "
        + context.getQueryHandleString()
        + "has already been fetched");
    } catch (InterruptedException | ExecutionException e) {
      throw new LensException("Error fetching result set!", e);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    try {
      resultSetMap.remove(handle);
    } catch (NullPointerException e) {
      throw new LensException("The query does not exist or was already purged", e);
    }
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    try {
      boolean cancelled = resultSetMap.get(handle).cancel(true);
      if (cancelled) {
        notifyQueryCancellation(handle);
      }
      return cancelled;
    } catch (NullPointerException e) {
      throw new LensException("The query does not exist or was already purged", e);
    }
  }

  private void notifyQueryCancellation(QueryHandle handle) {
    try {
      handleListenerMap.get(handle).onError(handle, handle + " cancelled");
    } catch (NullPointerException e) {
      log.debug("There are no subscriptions for notification. Skipping for {}", handle.getHandleIdString(), e);
    }
  }

  @Override
  public void close() throws LensException {
    for (QueryHandle handle : resultSetMap.keySet()) {
      try {
        closeQuery(handle);
      } catch (LensException e) {
        log.error("Error while closing query {}", handle.getHandleIdString(), e);
      }
    }
  }

  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public ImmutableSet<QueryLaunchingConstraint> getQueryConstraints() {
    return ImmutableSet.copyOf(Sets.<QueryLaunchingConstraint>newHashSet());
  }

  @Override
  public ImmutableSet<WaitingQueriesSelectionPolicy> getWaitingQuerySelectionPolicies() {
    return ImmutableSet.copyOf(Sets.<WaitingQueriesSelectionPolicy>newHashSet());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    /**
     * This flow could be abstracted out at the driver level
     */
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    /**
     * This flow could be abstracted out at the driver level
     */
  }

  protected class DruidQueryExecuteCallable implements Callable<LensResultSet> {

    private final QueryContext queryContext;
    private final SessionState sessionState;

    public DruidQueryExecuteCallable(QueryContext queryContext, SessionState sessionState) {
      this.queryContext = queryContext;
      this.sessionState = sessionState;
    }

    @Override
    public LensResultSet call() throws Exception {
      SessionState.setCurrentSessionState(sessionState);
      return execute(queryContext);
    }
  }
}
