package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;

public class BaselineServiceQueryParserTest {

  private static final long ONE_HOUR_SECONDS = 60 * 60L;
  private final AttributeMetadataProvider attributeMetadataProvider =
      Mockito.mock(AttributeMetadataProvider.class);
  protected static final String TENANT_ID = "tenant1";

  @Test
  public void testGetQueryRequest() {
    BaselineServiceQueryParser baselineServiceQueryParser =
        new BaselineServiceQueryParser(attributeMetadataProvider);
    List<TimeAggregation> timeAggregationList = new ArrayList<>();
    long periodInSecs = ONE_HOUR_SECONDS;
    TimeAggregation timeAggregation =
        getTimeAggregationFor(
            getFunctionExpressionFor(FunctionType.AVG, "SERVICE.duration", "duration_ts"));
    timeAggregationList.add(timeAggregation);
    QueryRequest request =
        baselineServiceQueryParser.getQueryRequest(
            Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli(),
            Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli(),
            Collections.singletonList("entity-1"),
            "Service.StartTime",
            timeAggregationList,
            periodInSecs,
            Collections.singletonList("SERVICE.id"));
    Assertions.assertNotNull(request);
    Assertions.assertEquals(2, request.getGroupByCount());
    Assertions.assertEquals(3, request.getFilter().getChildFilterCount());
  }

  @Test
  public void testQueryResponse() {
    BaselineServiceQueryParser baselineServiceQueryParser =
        new BaselineServiceQueryParser(attributeMetadataProvider);
    List<ResultSetChunk> resultSetChunks =
        List.of(
            getResultSetChunk(
                List.of("SERVICE.id", "dateTimeConvert", "PERCENTILE_SERVICE.duration_[99]_PT30S"),
                new String[][] {
                  {"entity-id-1", "1608524400000", "14.0"},
                  {"entity-id-1", "1608525210000", "15.0"},
                  {"entity-id-1", "1608525840000", "16.0"},
                  {"entity-id-1", "1608525540000", "17.0"}
                }));
    TimeAggregation timeAggregation =
        getTimeAggregationFor(
            getFunctionExpressionFor(
                FunctionType.PERCENTILE,
                "SERVICE.Latency",
                "PERCENTILE_SERVICE.duration_[99]_PT30S"));
    BaselineRequestContext baselineRequestContext =
        new BaselineRequestContext(TENANT_ID, Collections.EMPTY_MAP);
    BaselineTimeAggregation baselineTimeAggregation =
        BaselineTimeAggregation.newBuilder()
            .setAggregation(timeAggregation.getAggregation().getFunction())
            .setPeriod(timeAggregation.getPeriod())
            .build();
    baselineRequestContext.mapAliasToTimeAggregation(
        "PERCENTILE_SERVICE.duration_[99]_PT30S", baselineTimeAggregation);
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    attributeMap.put(
        "SERVICE.Latency",
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build());
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);
    BaselineEntitiesResponse response =
        baselineServiceQueryParser.parseQueryResponse(
            resultSetChunks.iterator(),
            baselineRequestContext,
            1,
            "SERVICE",
            Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli(),
            Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli());
    Assertions.assertNotNull(response);
    Assertions.assertEquals(1, response.getBaselineEntity(0).getBaselineMetricSeriesCount());
  }

  private TimeAggregation getTimeAggregationFor(Expression expression) {
    return TimeAggregation.newBuilder()
        .setAggregation(expression)
        .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60).build())
        .build();
  }

  private Expression getFunctionExpressionFor(FunctionType type, String columnName, String alias) {
    return Expression.newBuilder()
        .setFunction(
            FunctionExpression.newBuilder()
                .setFunction(type)
                .setAlias(alias)
                .addArguments(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder()
                                .setColumnName(columnName)
                                .setAlias(alias)))
                .build())
        .build();
  }
}
