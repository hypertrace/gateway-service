package org.hypertrace.gateway.service.common.converter;

import static org.hypertrace.core.attribute.service.v1.AttributeScope.API;
import static org.hypertrace.core.attribute.service.v1.AttributeScope.BACKEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.Test;

public class EntityServiceAndGatewayServiceConverterTest extends AbstractGatewayServiceTest {

  @Test
  public void testAddBetweenFilter() {
    int startTimeMillis = 1;
    int endTimeMillis = 2;
    String timestamp = "lastActivity";
    String timestampAttributeName = BACKEND.name() + "." + timestamp;

    Expression.Builder expectedStartTimeConstant =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(ValueType.LONG).setLong(startTimeMillis)));

    Expression.Builder expectedEndTimeConstant =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(ValueType.LONG).setLong(endTimeMillis)));

    Filter.Builder expectedStartTimeFilterBuilder = Filter.newBuilder();
    expectedStartTimeFilterBuilder.setOperator(Operator.GE);
    expectedStartTimeFilterBuilder.setLhs(
        EntityServiceAndGatewayServiceConverter.createColumnExpression(timestampAttributeName));
    expectedStartTimeFilterBuilder.setRhs(expectedStartTimeConstant);

    Filter.Builder expectedEndTimeFilterBuilder = Filter.newBuilder();
    expectedEndTimeFilterBuilder.setOperator(Operator.LT);
    expectedEndTimeFilterBuilder.setLhs(
        EntityServiceAndGatewayServiceConverter.createColumnExpression(timestampAttributeName));
    expectedEndTimeFilterBuilder.setRhs(expectedEndTimeConstant);

    Filter.Builder expectedFilterBuilder = Filter.newBuilder();
    expectedFilterBuilder.setOperator(Operator.AND);
    expectedFilterBuilder.addChildFilter(expectedStartTimeFilterBuilder);
    expectedFilterBuilder.addChildFilter(expectedEndTimeFilterBuilder);

    AttributeMetadataProvider mockProvider = mock(AttributeMetadataProvider.class);
    when(mockProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(BACKEND.name()), eq(timestamp)))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setId(timestampAttributeName).build()));

    EntityQueryRequest.Builder builder = EntityQueryRequest.newBuilder();
    EntitiesRequest request = EntitiesRequest.newBuilder().setEntityType(API.name()).build();

    EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
        startTimeMillis,
        endTimeMillis,
        mockProvider,
        request,
        builder,
        mock(EntitiesRequestContext.class));

    // no filter added for unsupported entity types
    assertEquals(Filter.getDefaultInstance(), builder.getFilter());

    // if it's an backend, filters will be added
    request = EntitiesRequest.newBuilder().setEntityType(BACKEND.name()).build();
    EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
        startTimeMillis,
        endTimeMillis,
        mockProvider,
        request,
        builder,
        mock(EntitiesRequestContext.class));
    assertEquals(expectedFilterBuilder.build(), builder.getFilter());

    // if there's an existing filter in EntitiesRequest, it will be preserved
    Expression.Builder scoreConstant =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setValueType(ValueType.INT).setInt(10)));

    Filter.Builder existingScoreFilter = Filter.newBuilder();
    existingScoreFilter.setOperator(Operator.GT);
    existingScoreFilter.setLhs(
        EntityServiceAndGatewayServiceConverter.createColumnExpression("TEST.score"));
    existingScoreFilter.setRhs(scoreConstant);

    expectedFilterBuilder.addChildFilter(existingScoreFilter);
    builder.setFilter(existingScoreFilter.build());
    EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
        startTimeMillis,
        endTimeMillis,
        mockProvider,
        request,
        builder,
        mock(EntitiesRequestContext.class));
    assertEquals(expectedFilterBuilder.build(), builder.getFilter());
  }
}
