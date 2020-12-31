package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.common.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaselineServiceImplTest {

  protected static final String TENANT_ID = "tenant1";
  private final AttributeMetadataProvider attributeMetadataProvider =
      Mockito.mock(AttributeMetadataProvider.class);
  private final BaselineServiceQueryExecutor baselineServiceQueryExecutor =
      Mockito.mock(BaselineServiceQueryExecutor.class);
  private final BaselineServiceQueryParser baselineServiceQueryParser =
      new BaselineServiceQueryParser(attributeMetadataProvider);
  private final EntityIdColumnsConfigs entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);


  @Test
  public void testBaselineForEntitiesForAggregates() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000 * 60 * 5;
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
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
        .thenReturn(getResultSet().iterator());
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
            attributeMetadataProvider, baselineServiceQueryParser, baselineServiceQueryExecutor, entityIdColumnsConfigs);
    BaselineEntitiesResponse baselineResponse =
        baselineService.getBaselineForEntities(TENANT_ID, baselineEntitiesRequest, Map.of());
    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
    Assertions.assertTrue(
        baselineResponse.getBaselineEntityList().get(0).getBaselineAggregateMetricCount() > 0);
  }

  @Test
  public void testBaselineEntitiesForMetricSeries() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 100000 * 60 * 5;
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
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
        .thenReturn(getResultSet().iterator());
    Map<String, AttributeMetadata> attributeMap = new HashMap<>();
    AttributeMetadata attribute = AttributeMetadata.newBuilder().setFqn("Service.Latency").setId("Service.Id").build();
    attributeMap.put(
        "duration_ts",
            attribute);
    attributeMap.put(
        "SERVICE.duration",
            attribute);
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
            attributeMetadataProvider, baselineServiceQueryParser, baselineServiceQueryExecutor, entityIdColumnsConfigs);
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

  public List<ResultSetChunk> getResultSet() {
    long time = System.currentTimeMillis();

    List<ResultSetChunk> resultSetChunks =
        List.of(
            getResultSetChunk(
                List.of("SERVICE.id", "dateTimeConvert", "duration_ts"),
                new String[][] {
                  {"entity-1", String.valueOf(time), "14.0"},
                  {"entity-1", String.valueOf(time - 60000), "15.0"},
                  {"entity-1", String.valueOf(time - 120000), "16.0"},
                  {"entity-1", String.valueOf(time - 180000), "17.0"}
                }));
    return resultSetChunks;
  }
}
