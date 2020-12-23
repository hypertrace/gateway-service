package org.hypertrace.gateway.service.common.converter;

import static org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createBetweenTimesFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCompositeFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createLongFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Test;

class QueryAndGatewayDtoConverterTest {
  private static final String TIMESTAMP_ATTR_ID = "EVENT.startTime";
  private static final String SPACES_ATTR_ID = "EVENT.spaceIds";

  @Test
  void onlyAddsTimeRangeFilterOnlyIfMissing() {
    Filter result =
        addTimeAndSpaceFiltersAndConvertToQueryFilter(
            10,
            20,
            "",
            TIMESTAMP_ATTR_ID,
            SPACES_ATTR_ID,
            org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance());

    Filter expectedFilter = createBetweenTimesFilter(TIMESTAMP_ATTR_ID, 10, 20);

    assertEquals(expectedFilter, result);

    result =
        addTimeAndSpaceFiltersAndConvertToQueryFilter(
            10, 20, "", TIMESTAMP_ATTR_ID, SPACES_ATTR_ID, buildGatewayTimerangeFilter(10, 20));

    assertEquals(expectedFilter, result);
  }

  @Test
  void addsSpaceIdFilterIfProvided() {
    Filter result =
        addTimeAndSpaceFiltersAndConvertToQueryFilter(
            10,
            20,
            "my-space",
            TIMESTAMP_ATTR_ID,
            SPACES_ATTR_ID,
            buildLongFilter("EVENT.someCol", Operator.EQ, 25));

    Filter expectedFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                createBetweenTimesFilter(TIMESTAMP_ATTR_ID, 10, 20),
                createLongFilter(
                    "EVENT.someCol", org.hypertrace.core.query.service.api.Operator.EQ, 25),
                createStringFilter(
                    SPACES_ATTR_ID,
                    org.hypertrace.core.query.service.api.Operator.EQ,
                    "my-space")));

    assertEquals(expectedFilter, result);
  }

  org.hypertrace.gateway.service.v1.common.Filter buildGatewayTimerangeFilter(
      long start, long end) {
    return org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(buildLongFilter(TIMESTAMP_ATTR_ID, Operator.GE, start))
        .addChildFilter(buildLongFilter(TIMESTAMP_ATTR_ID, Operator.LT, end))
        .build();
  }

  org.hypertrace.gateway.service.v1.common.Filter buildLongFilter(
      String col, Operator operator, long value) {
    return org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
        .setLhs(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(col).build()))
        .setOperator(operator)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value))))
        .build();
  }
}
