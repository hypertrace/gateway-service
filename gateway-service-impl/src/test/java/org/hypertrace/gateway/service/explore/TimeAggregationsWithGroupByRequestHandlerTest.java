package org.hypertrace.gateway.service.explore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.*;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class TimeAggregationsWithGroupByRequestHandlerTest {
  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  @Mock private RequestHandler normalRequestHandler;
  @Mock private TimeAggregationsRequestHandler timeAggregationsRequestHandler;
  @Mock private QueryServiceClient queryServiceClient;

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    normalRequestHandler = mock(RequestHandler.class);
    timeAggregationsRequestHandler = mock(TimeAggregationsRequestHandler.class);
    queryServiceClient = mock(QueryServiceClient.class);
  }

  @Test
  public void test() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
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
            queryServiceClient, attributeMetadataProvider);

    ExploreResponse.Builder groupByExploreResponseBuilder =
            ExploreResponse.newBuilder()
                    .addRow(Row.newBuilder().putColumns("", Value.newBuilder().build()).build());
    when(normalRequestHandler.handleRequest(
            any(ExploreRequestContext.class), any(ExploreRequest.class)))
            .thenReturn(groupByExploreResponseBuilder);

    ExploreResponse.Builder exploreResponseBuilder =
        timeAggregationsWithGroupByRequestHandler.handleRequest(
            exploreRequestContext, exploreRequest);
  }
}
