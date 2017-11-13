package org.apache.lens.server.api.query.cost;


import org.apache.lens.api.query.QueryCostType;
import org.apache.lens.server.api.query.priority.RangeConf;

public class CostToQueryTypeRangeConf extends RangeConf<Double, QueryCostType> {
  /**
   * Super constructor
   *
   * @param confValue
   * @see RangeConf#RangeConf(String)
   */
  public CostToQueryTypeRangeConf(String confValue) {
    super(confValue);
  }

  /**
   * Parse key method
   *
   * @param s
   * @return parsed float from string s
   * @see RangeConf#parseKey(String)
   */
  @Override
  protected Double parseKey(String s) {
    return Double.parseDouble(s);
  }

  /**
   * Parse value method
   *
   * @param s
   * @return parsed QueryCostType from String s
   * @see RangeConf#parseValue(String)
   */
  @Override
  protected QueryCostType parseValue(String s) {
    return QueryCostType.valueOf(s);
  }

  /**
   * Default value is "HIGH".
   * @return "HIGH"
   */
  @Override
  protected String getDefaultConf() {
    return QueryCostType.HIGH.toString();
  }
}
