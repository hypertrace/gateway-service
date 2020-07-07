package org.hypertrace.gateway.service.common.util;

import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryExpressionUtilTest {

  @Test
  public void testAlignTimeToPeriod() {
    long time = 1568103141000L;
    {
      long alignedNextTime = QueryExpressionUtil.alignToPeriodBoundary(time, 30, true);
      Assertions.assertEquals(1568103150000L, alignedNextTime);
      long alignedPreviousTime = QueryExpressionUtil.alignToPeriodBoundary(time, 30, false);
      Assertions.assertEquals(1568103120000L, alignedPreviousTime);
      Assertions.assertEquals(30 * 1000, alignedNextTime - alignedPreviousTime);
    }

    {
      long alignedNextTime = QueryExpressionUtil.alignToPeriodBoundary(time, 60, true);
      Assertions.assertEquals(1568103180000L, alignedNextTime);
      long alignedPreviousTime = QueryExpressionUtil.alignToPeriodBoundary(time, 60, false);
      Assertions.assertEquals(1568103120000L, alignedPreviousTime);
      Assertions.assertEquals(60 * 1000, alignedNextTime - alignedPreviousTime);
    }
  }

  @Test
  public void testBooleanFilter() {
    Filter booleanFilter = QueryExpressionUtil.getBooleanFilter("API.is_external", true).build();
    Assertions.assertEquals(
        "API.is_external", booleanFilter.getLhs().getColumnIdentifier().getColumnName());
    Assertions.assertEquals(Operator.EQ, booleanFilter.getOperator());
    Assertions.assertTrue(booleanFilter.getRhs().getLiteral().getValue().getBoolean());
  }
}
