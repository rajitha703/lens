package org.apache.lens.server.api.query.cost;

import org.apache.lens.api.query.QueryCostType;
import org.apache.lens.server.api.error.LensException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CostRangeQueryTypeDecider implements QueryTypeDecider {

  @NonNull
  private final CostToQueryTypeRangeConf costToQueryTypeRangeMap;

  @Override
  public QueryCostType decideCostType(@NonNull final QueryCost cost) throws LensException {
    QueryCostType q = costToQueryTypeRangeMap.get(cost.getEstimatedResourceUsage());
    log.info("cost was: {}, decided querytype: {}", cost, q);
    return q;
  }
}