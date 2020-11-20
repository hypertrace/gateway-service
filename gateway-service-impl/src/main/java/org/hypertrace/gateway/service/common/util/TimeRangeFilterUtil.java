package org.hypertrace.gateway.service.common.util;

import static org.hypertrace.gateway.service.v1.common.Operator.AND;

import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class TimeRangeFilterUtil {
  public static Filter addTimeRangeFilter(String timestampAttribute, Filter filter, long startTime, long endTime) {
    Filter.Builder filterBuilder = Filter.newBuilder().setOperator(AND);
    if (!Filter.getDefaultInstance().equals(filter)) {
      filterBuilder.addChildFilter(filter);
    }
    filterBuilder.addChildFilter(getTimestampFilter(timestampAttribute, Operator.GE, startTime))
        .addChildFilter(getTimestampFilter(timestampAttribute, Operator.LT, endTime));
    return filterBuilder.build();
  }

  private static Filter getTimestampFilter(String colName, Operator operator, long timestamp) {
    return Filter.newBuilder().setOperator(operator)
        .setLhs(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(colName)))
        .setRhs(Expression.newBuilder().setLiteral(
            LiteralConstant.newBuilder().setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(timestamp))))
        .build();
  }
}
