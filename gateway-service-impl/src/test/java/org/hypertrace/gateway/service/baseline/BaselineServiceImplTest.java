package org.hypertrace.gateway.service.baseline;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.hypertrace.gateway.service.baseline.BaselineServiceImpl.DAY_IN_MILLIS;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.GatewayServiceConfig;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfig;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntity;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

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
  private final EntityIdColumnsConfig entityIdColumnsConfig = mock(EntityIdColumnsConfig.class);
  private final GatewayServiceConfig gatewayServiceConfig = mock(GatewayServiceConfig.class);

  @BeforeEach
  public void setup() {
    when(gatewayServiceConfig.getEntityIdColumnsConfig()).thenReturn(entityIdColumnsConfig);
  }

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
                any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                any(RequestContext.class), any(QueryRequest.class)))
        .thenReturn(getResultSetForAvg("duration_ts").iterator());
    when(entityIdColumnsConfig.getIdKey("SERVICE")).thenReturn(Optional.of("id"));

    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    attributeMap.put(
        "duration_ts",
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build());
    attributeMap.put(
        "SERVICE.duration",
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build());
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);

    BaselineService baselineService =
        new BaselineServiceImpl(
            gatewayServiceConfig,
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(
            new RequestContext(forTenantId(TENANT_ID)), baselineEntitiesRequest);
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineAggregateMetricCount() > 0);
  }

  @ParameterizedTest
  @MethodSource("getFunctionExpressionForAvgRate")
  public void testBaselineEntitiesForAggregatesForAvgRateFunction(
      FunctionExpression functionExpression) {
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .addEntityIds("entity-1")
            .addBaselineAggregateRequest(functionExpression)
            .build();

    // Mock section
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder().setFqn("Service.numCalls").setId("Service.Id").build();
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                any(RequestContext.class), any(QueryRequest.class)))
        .thenReturn(getResultSetForAvgRate("numCalls").iterator());
    when(entityIdColumnsConfig.getIdKey("SERVICE")).thenReturn(Optional.of("id"));
    // Attribute Metadata map contains mapping between Attributes and ID to query data.
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    attributeMap.put(
        "SERVICE.numCalls",
        AttributeMetadata.newBuilder().setFqn("Service.numCalls").setId("Service.Id").build());
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);

    BaselineService baselineService =
        new BaselineServiceImpl(
            gatewayServiceConfig,
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(
            new RequestContext(forTenantId(TENANT_ID)), baselineEntitiesRequest);
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineAggregateMetricCount() > 0);
    BaselineEntity baselineEntity = baselineResponse.getBaselineEntityList().get(0);
    // verify the baseline for AVG RATE (medianValue/60)
    Assertions.assertEquals(
        1.0, baselineEntity.getBaselineAggregateMetricMap().get("numCalls").getValue().getDouble());
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
                any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attributeMetadata));
    Mockito.when(
            baselineServiceQueryExecutor.executeQuery(
                any(RequestContext.class), any(QueryRequest.class)))
        .thenReturn(getResultSetForAvg("duration_ts").iterator());
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    AttributeMetadata attribute =
        AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build();
    attributeMap.put("duration_ts", attribute);
    attributeMap.put("SERVICE.duration", attribute);
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                any(RequestContext.class), Mockito.anyString()))
        .thenReturn(attributeMap);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(RequestContext.class), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(Optional.of(attribute));
    when(entityIdColumnsConfig.getIdKey("SERVICE")).thenReturn(Optional.of("id"));

    BaselineService baselineService =
        new BaselineServiceImpl(
            gatewayServiceConfig,
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(
            new RequestContext(forTenantId(TENANT_ID)), baselineEntitiesRequest);
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

  private static Stream<Arguments> getFunctionExpressionForAvgRate() {

    FunctionType type = FunctionType.AVGRATE;
    String columnName = "SERVICE.numCalls";
    String alias = "numCalls";

    // for iso support
    FunctionExpression functionExpression1 =
        FunctionExpression.newBuilder()
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
                            .setValue(
                                Value.newBuilder()
                                    .setString("PT1M")
                                    .setValueType(ValueType.STRING))))
            .build();

    // for long support
    FunctionExpression functionExpression2 =
        FunctionExpression.newBuilder()
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

    return Stream.of(
        Arguments.arguments(functionExpression1), Arguments.arguments(functionExpression2));
  }

  public List<ResultSetChunk> getResultSetForAvg(String alias) {
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

  public List<ResultSetChunk> getResultSetForAvgRate(String alias) {
    long time = Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli();

    return List.of(
        getResultSetChunk(
            List.of("SERVICE.id", "dateTimeConvert", alias),
            new String[][] {
              {"entity-1", String.valueOf(time), "0.3333333333333333"},
              {"entity-1", String.valueOf(time - 60000), "0.6666666666666666"},
              {"entity-1", String.valueOf(time - 120000), "1.0"},
              {"entity-1", String.valueOf(time - 180000), "1.3333333333333333"},
              {"entity-1", String.valueOf(time - 180000), "1.6666666666666667"},
            }));
  }

  @Test
  public void testStartTimeCalcGivenTimeRange() {
    BaselineServiceImpl baselineService =
        new BaselineServiceImpl(
            gatewayServiceConfig,
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor);
    long endTimeInMillis = System.currentTimeMillis();
    long startTimeInMillis = endTimeInMillis - ONE_HOUR_MILLIS;
    long actualStartTime = baselineService.getUpdatedStartTime(startTimeInMillis, endTimeInMillis);
    Assertions.assertEquals(startTimeInMillis - DAY_IN_MILLIS, actualStartTime);

    startTimeInMillis = endTimeInMillis - TWENTY_HOUR_MILLIS;
    actualStartTime = baselineService.getUpdatedStartTime(startTimeInMillis, endTimeInMillis);
    Assertions.assertEquals(startTimeInMillis - (2 * TWENTY_HOUR_MILLIS), actualStartTime);
  }
}
