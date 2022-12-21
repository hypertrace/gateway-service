package org.hypertrace.gateway.service.explore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.AttributeExpression;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TimeAggregationsWithGroupByRequestHandlerTest {
  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  @Mock private RequestHandler normalRequestHandler;
  @Mock private TimeAggregationsRequestHandler timeAggregationsRequestHandler;

  @Test
  public void testTimeAggregationsWithGroupBy_multiValuedField() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(0)
            .setEndTimeMillis(100)
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MAX)
                                    .setAlias("MAX_Duration")
                                    .addArguments(
                                        QueryExpressionUtil.buildAttributeExpression(
                                            "Api.Trace.metrics.duration_millis")))))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.labels").build())
                    .build())
            .build();

    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext(mock(RequestContext.class), exploreRequest);
    TimeAggregationsWithGroupByRequestHandler timeAggregationsWithGroupByRequestHandler =
        new TimeAggregationsWithGroupByRequestHandler(
            attributeMetadataProvider, normalRequestHandler, timeAggregationsRequestHandler);

    ExploreRequest groupByRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(0)
            .setEndTimeMillis(100)
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.MAX)
                            .setAlias("MAX_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setAttributeExpression(
                                        AttributeExpression.newBuilder()
                                            .setAttributeId("Api.Trace.metrics.duration_millis"))
                                    .build())
                            .build())
                    .build())
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.labels").build())
                    .build())
            .build();
    ExploreResponse.Builder groupByExploreResponseBuilder =
        ExploreResponse.newBuilder()
            .addRow(
                Row.newBuilder()
                    .putColumns(
                        "API.labels",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING_ARRAY)
                            .addAllStringArray(List.of("label1", "label2", "label3"))
                            .build())
                    .build())
            .addRow(
                Row.newBuilder()
                    .putColumns(
                        "API.labels",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING_ARRAY)
                            .addAllStringArray(List.of("label4", "label5", "label6"))
                            .build())
                    .build());
    when(normalRequestHandler.handleRequest(any(ExploreRequestContext.class), eq(groupByRequest)))
        .thenReturn(groupByExploreResponseBuilder);
    when(attributeMetadataProvider.getAttributesMetadata(
            any(ExploreRequestContext.class), eq("API")))
        .thenReturn(
            Map.of(
                "API.labels",
                AttributeMetadata.newBuilder()
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .build()));

    ExploreRequest timeAggregationsRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(0)
            .setEndTimeMillis(100)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        Filter.newBuilder()
                            .setLhs(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("API.labels")
                                            .build())
                                    .build())
                            .setOperator(Operator.IN)
                            .setRhs(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.STRING_ARRAY)
                                                    .addStringArray("label1")
                                                    .addStringArray("label2")
                                                    .addStringArray("label3")
                                                    .addStringArray("label4")
                                                    .addStringArray("label5")
                                                    .addStringArray("label6")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setValue(60).setUnit("SECONDS").build())
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MAX)
                                    .setAlias("MAX_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setAttributeExpression(
                                                AttributeExpression.newBuilder()
                                                    .setAttributeId(
                                                        "Api.Trace.metrics.duration_millis")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.labels").build())
                    .build())
            .build();

    // dummy mock
    ExploreResponse.Builder timeAggregationsResponseBuilder = ExploreResponse.newBuilder();
    when(timeAggregationsRequestHandler.handleRequest(
            any(ExploreRequestContext.class), eq(timeAggregationsRequest)))
        .thenReturn(timeAggregationsResponseBuilder);

    assertEquals(
        timeAggregationsResponseBuilder,
        timeAggregationsWithGroupByRequestHandler.handleRequest(
            exploreRequestContext, exploreRequest));
  }
}
