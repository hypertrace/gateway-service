package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntity;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hypertrace.gateway.service.baseline.BaselineServiceImpl.DAY_IN_MILLIS;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaselineServiceImplTest {

  protected static final String TENANT_ID = "tenant1";
  private static final long ONE_HOUR_MILLIS = 1000 * 60 * 60;
  private static final long TWENTY_HOUR_MILLIS = 20 * ONE_HOUR_MILLIS;
  private final AttributeMetadataProvider attributeMetadataProvider =
      Mockito.mock(AttributeMetadataProvider.class);
  private final BaselineServiceQueryExecutor baselineServiceQueryExecutor =
      Mockito.mock(BaselineServiceQueryExecutor.class);
  private final BaselineServiceQueryParser baselineServiceQueryParser =
      new BaselineServiceQueryParser(attributeMetadataProvider);
  private final EntityIdColumnsConfigs entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);

  @Test
  public void testBaselineForEntitiesForAggregates() {
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .addEntityIds("entity-1")
            .addBaselineAggregateRequest(
                getFunctionExpressionFor(FunctionType.AVG, "SERVICE.duration", "duration_ts"))
            .build();

    // Mock section
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.StartTime").build();
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                Mockito.anyMap(), Mockito.any(QueryRequest.class)))
        .thenReturn(getResultSet("duration_ts").iterator());
    when(entityIdColumnsConfigs.getIdKey("SERVICE")).thenReturn(Optional.of("id"));

    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    attributeMap.put(
        "duration_ts",
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build());
    attributeMap.put(
        "SERVICE.duration",
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build());
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);

    BaselineService baselineService =
        new BaselineServiceImpl(
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor,
            entityIdColumnsConfigs);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(TENANT_ID, baselineEntitiesRequest, Map.of());
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineAggregateMetricCount() > 0);
  }

  @Test
  public void testBaselineEntitiesForAggregatesForAvgRateFunction() {
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .addEntityIds("entity-1")
            .addBaselineAggregateRequest(
                getFunctionExpressionForAvgRate(
                    FunctionType.AVGRATE, "SERVICE.numCalls", "numCalls"))
            .build();

    // Mock section
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder().setFqn("Service.numCalls").setId("Service.Id").build();
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                Mockito.anyMap(), Mockito.any(QueryRequest.class)))
        .thenReturn(getResultSet("numCalls").iterator());
    when(entityIdColumnsConfigs.getIdKey("SERVICE")).thenReturn(Optional.of("id"));
    // Attribute Metadata map contains mapping between Attributes and ID to query data.
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    attributeMap.put(
        "SERVICE.numCalls",
        AttributeMetadata.newBuilder().setFqn("Service.numCalls").setId("Service.Id").build());
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);

    BaselineService baselineService =
        new BaselineServiceImpl(
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor,
            entityIdColumnsConfigs);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(TENANT_ID, baselineEntitiesRequest, Map.of());
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineAggregateMetricCount() > 0);
    BaselineEntity baselineEntity = baselineResponse.getBaselineEntityList().get(0);
    // verify the baseline for AVG RATE (medianValue/60)
    Assertions.assertEquals(1.0,
        baselineEntity.getBaselineAggregateMetricMap().get("numCalls").getValue().getDouble());
  }

  @Test
  public void testBaselineEntitiesForMetricSeries() {
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .addEntityIds("entity-1")
            .addBaselineMetricSeriesRequest(getBaselineTimeSeriesRequest())
            .build();
    // Mock section
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.StartTime").build();
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                Mockito.anyMap(), Mockito.any(QueryRequest.class)))
        .thenReturn(getResultSet("duration_ts").iterator());
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    AttributeMetadata attribute =
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build();
    attributeMap.put("duration_ts", attribute);
    attributeMap.put("SERVICE.duration", attribute);
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                Mockito.any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attribute));
    when(entityIdColumnsConfigs.getIdKey("SERVICE")).thenReturn(Optional.of("id"));

    BaselineService baselineService =
        new BaselineServiceImpl(
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor,
            entityIdColumnsConfigs);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(TENANT_ID, baselineEntitiesRequest, Map.of());
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineMetricSeriesCount() > 0);
  }

  private BaselineTimeAggregation getBaselineTimeSeriesRequest() {
    return BaselineTimeAggregation.newBuilder()
        .setAggregation(
            getFunctionExpressionFor(FunctionType.AVG, "SERVICE.duration", "duration_ts"))
        .setPeriod(Period.newBuilder().setUnit("MINUTES").setValue(1).build())
        .build();
  }

  private FunctionExpression getFunctionExpressionFor(
      FunctionType type, String columnName, String alias) {
    return FunctionExpression.newBuilder()
        .setFunction(type)
        .setAlias(alias)
        .addArguments(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias)))
        .build();
  }

  private FunctionExpression getFunctionExpressionForAvgRate(
      FunctionType type, String columnName, String alias) {
    return FunctionExpression.newBuilder()
        .setFunction(type)
        .setAlias(alias)
        .addArguments(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias)))
        .addArguments(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(Value.newBuilder().setLong(60).setValueType(ValueType.LONG))))
        .build();
  }

  public List<ResultSetChunk> getResultSet(String alias) {
    long time = Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli();

    return List.of(
        getResultSetChunk(
            List.of("SERVICE.id", "dateTimeConvert", alias),
            new String[][] {
              {"entity-1", String.valueOf(time), "20.0"},
              {"entity-1", String.valueOf(time - 60000), "40.0"},
              {"entity-1", String.valueOf(time - 120000), "60.0"},
              {"entity-1", String.valueOf(time - 180000), "80.0"},
              {"entity-1", String.valueOf(time - 180000), "100.0"},
            }));
  }

  @Test
  public void testStartTimeCalcGivenTimeRange() {
    BaselineServiceImpl baselineService =
        new BaselineServiceImpl(
            attributeMetadataProvider, baselineServiceQueryParser,
            baselineServiceQueryExecutor, entityIdColumnsConfigs);
    long endTimeInMillis = System.currentTimeMillis();
    long startTimeInMillis = endTimeInMillis - ONE_HOUR_MILLIS;
    long actualStartTime = baselineService.getUpdatedStartTime(startTimeInMillis, endTimeInMillis);
    Assertions.assertEquals(startTimeInMillis - DAY_IN_MILLIS, actualStartTime);

    startTimeInMillis = endTimeInMillis - TWENTY_HOUR_MILLIS;
    actualStartTime = baselineService.getUpdatedStartTime(startTimeInMillis, endTimeInMillis);
    Assertions.assertEquals(startTimeInMillis - (2 * TWENTY_HOUR_MILLIS), actualStartTime);
  }
}
