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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;

import java.text.DateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.server.api.error.LensException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBetweenTimeRangeWriter extends TestTimeRangeWriter {

  public static final String CLOSED = "CLOSED";
  public static final String OPEN = "OPEN";

  @Override
  public TimeRangeWriter getTimerangeWriter() {
    return new BetweenTimeRangeWriter();
  }

  @Override
  public boolean failDisjoint() {
    return true;
  }

  @Override
  public void validateDisjoint(String whereClause, DateFormat format) {
    Assert.fail();
  }

  @Override
  public void validateConsecutive(String whereClause, DateFormat format) {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", TWODAYS_BACK, NOW, DAILY.format());
    } else {
      expected = getBetweenClause("test", "dt", TWODAYS_BACK, NOW, format);
    }
    Assert.assertEquals(expected, whereClause);
  }

  public static String getBetweenClause(String alias, String colName, Date start, Date end, DateFormat format) {
    String first = format.format(start);
    String last = format.format(end);
    return " (" + alias + "." + colName + " BETWEEN '" + first + "' AND '" + last + "') ";
  }

  @Test
  public void testSinglePartBetweenOnly() throws LensException {
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(true), "test",
      answeringParts);
    validateBetweenOnlySingle(whereClause, null);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContext(true), "test", answeringParts);
    validateBetweenOnlySingle(whereClause, DB_FORMAT);

  }

  public void validateBetweenOnlySingle(String whereClause, DateFormat format) {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, -1), getDateWithOffset(DAILY, -1), DAILY.format());
    } else {
      expected = getBetweenClause("test", "dt", getDateWithOffset(DAILY, -1), getDateWithOffset(DAILY, -1), format);
    }
    Assert.assertEquals(expected, whereClause);
  }

  @Test
  public void testBetweenOpenOpen() throws LensException {
    int startDateOffset = -2;
    int endDateOffset = -1;
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, startDateOffset), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, endDateOffset), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(OPEN, OPEN),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, null, startDateOffset - 1, endDateOffset + 1);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(OPEN, OPEN),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, DB_FORMAT, startDateOffset - 1, endDateOffset + 1);
  }

  @Test
  public void testBetweenOpenClose() throws LensException {
    int startDateOffset = -2;
    int endDateOffset = -1;
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, startDateOffset), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, endDateOffset), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(OPEN, CLOSED),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, null, startDateOffset - 1, endDateOffset);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, startDateOffset), DAILY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, endDateOffset), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(OPEN, CLOSED),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, DB_FORMAT, startDateOffset - 1, endDateOffset);
  }

  @Test
  public void testBetweenCloseOpen() throws LensException {
    int startDateOffset = -2;
    int endDateOffset = -1;
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(CLOSED, OPEN),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, null, startDateOffset, endDateOffset + 1);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(CLOSED, OPEN),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, DB_FORMAT, startDateOffset, endDateOffset + 1);
  }

  @Test
  public void testBetweenCloseClose() throws LensException {
    int startDateOffset = -2;
    int endDateOffset = -1;
    Set<FactPartition> answeringParts = new LinkedHashSet<FactPartition>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, null));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, null));
    String whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(CLOSED, CLOSED),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, null, startDateOffset, endDateOffset);

    answeringParts = new LinkedHashSet<>();
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -2), DAILY, null, DB_FORMAT));
    answeringParts.add(new FactPartition("dt", getDateWithOffset(DAILY, -1), DAILY, null, DB_FORMAT));
    whereClause = getTimerangeWriter().getTimeRangeWhereClause(getMockedCubeContextForBounds(CLOSED, CLOSED),
      "test", answeringParts);
    validateBetweenOpenClose(whereClause, DB_FORMAT, startDateOffset, endDateOffset);
  }

  private void validateBetweenOpenClose(String whereClause, DateFormat format, int startOffset, int endOffset) {
    String expected = null;
    if (format == null) {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, startOffset), getDateWithOffset(DAILY, endOffset), DAILY
          .format());
    } else {
      expected =
        getBetweenClause("test", "dt", getDateWithOffset(DAILY, startOffset), getDateWithOffset(DAILY, endOffset),
          format);
    }
    Assert.assertEquals(expected, whereClause);
  }
}
