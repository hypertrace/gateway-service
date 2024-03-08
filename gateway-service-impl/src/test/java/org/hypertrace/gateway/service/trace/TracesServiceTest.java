package org.hypertrace.gateway.service.trace;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createAttributeExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.GatewayServiceConfig;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorServiceFactory;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TracesServiceTest extends AbstractGatewayServiceTest {

  private static final String TENANT_ID = "tenant-id";

  private LogConfig logConfig;
  private AttributeMetadataProvider attributeMetadataProvider;
  private QueryServiceClient queryServiceClient;
  private TracesService tracesService;
  private ExecutorService queryExecutor;
  private GatewayServiceConfig gatewayServiceConfig;

  @BeforeEach
  public void setup() {
    super.setup();

    logConfig = Mockito.mock(LogConfig.class);
    when(logConfig.getQueryThresholdInMillis()).thenReturn(1500L);

    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    mockAttributeMetadata(attributeMetadataProvider);

    queryServiceClient = mock(QueryServiceClient.class);
    queryExecutor =
        QueryExecutorServiceFactory.buildExecutorService(
            QueryExecutorConfig.from(this.getConfig()));

    gatewayServiceConfig = mock(GatewayServiceConfig.class);
    when(gatewayServiceConfig.getLogConfig()).thenReturn(logConfig);
    when(gatewayServiceConfig.getScopeFilterConfigs())
        .thenReturn(new ScopeFilterConfigs(ConfigFactory.empty()));
    tracesService =
        new TracesService(
            gatewayServiceConfig, queryServiceClient, attributeMetadataProvider, queryExecutor);
  }

  @AfterEach
  public void clear() {
    queryExecutor.shutdown();
  }

  @Test
  public void test_getTraces_Without_Total() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    RequestContext requestContext = new RequestContext(forTenantId(TENANT_ID));

    TracesRequest tracesRequest =
        TracesRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setScope("API_TRACE")
            .addSelection(QueryExpressionUtil.buildAttributeExpression("API_TRACE.apiName"))
            .build();

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("API_TRACE.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter(
                    "API_TRACE.startTime", startTime, endTime))
            .build();

    QueryRequest expectedTotalQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(QueryRequestUtil.createCountByColumnSelection("API_TRACE.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter(
                    "API_TRACE.startTime", startTime, endTime))
            .setLimit(1)
            .build();

    when(queryServiceClient.executeQuery(any(), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of("API_TRACE.apiName"), new String[][] {{"apiName1"}, {"apiName2"}}))
                .iterator());

    when(queryServiceClient.executeQuery(any(), eq(expectedTotalQueryRequest)))
        .thenReturn(
            List.of(getResultSetChunk(List.of("COUNT_API_TRACE_apiName"), new String[][] {{"2"}}))
                .iterator());

    TracesResponse response = tracesService.getTracesByFilter(requestContext, tracesRequest);

    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedQueryRequest));
    verify(queryServiceClient, times(0)).executeQuery(any(), eq(expectedTotalQueryRequest));

    assertNotNull(response);
    assertEquals(0, response.getTotal());
  }

  @Test
  public void test_getTraces_With_Total() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    RequestContext requestContext = new RequestContext(forTenantId(TENANT_ID));

    TracesRequest tracesRequest =
        TracesRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setScope("API_TRACE")
            .addSelection(QueryExpressionUtil.buildAttributeExpression("API_TRACE.apiName"))
            .setFetchTotal(true)
            .build();

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("API_TRACE.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter(
                    "API_TRACE.startTime", startTime, endTime))
            .build();

    QueryRequest expectedTotalQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(QueryRequestUtil.createCountByColumnSelection("API_TRACE.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter(
                    "API_TRACE.startTime", startTime, endTime))
            .setLimit(1)
            .build();

    when(queryServiceClient.executeQuery(any(), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of("API_TRACE.apiName"), new String[][] {{"apiName1"}, {"apiName2"}}))
                .iterator());

    when(queryServiceClient.executeQuery(any(), eq(expectedTotalQueryRequest)))
        .thenReturn(
            List.of(getResultSetChunk(List.of("COUNT_API_TRACE_apiName"), new String[][] {{"2"}}))
                .iterator());

    TracesResponse response = tracesService.getTracesByFilter(requestContext, tracesRequest);

    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedQueryRequest));
    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedTotalQueryRequest));

    assertNotNull(response);
    assertEquals(2, response.getTotal());
  }

  private void mockAttributeMetadata(AttributeMetadataProvider attributeMetadataProvider) {
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name())))
        .thenReturn(
            Map.of(
                "API_TRACE.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("startTime")
                    .setFqn("API_TRACE.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API_TRACE.startTime")
                    .build(),
                "API_TRACE.apiId",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiId")
                    .setFqn("API_TRACE.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .addSources(AttributeSource.EDS)
                    .setId("API_TRACE.apiId")
                    .build(),
                "API_TRACE.apiName",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiName")
                    .setFqn("API_TRACE.name")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API_TRACE.name")
                    .build(),
                "API_TRACE.apiBoundaryType",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiBoundaryType")
                    .setFqn("API_TRACE.apiBoundaryType")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.EDS)
                    .setId("API_TRACE.apiBoundaryType")
                    .build(),
                "API_TRACE.spaceIds",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setId("API_TRACE.spaceIds")
                    .setKey("spaceIds")
                    .setFqn("API_TRACE.spaceIds")
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    // each value
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("startTime")
                    .setFqn("API_TRACE.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API_TRACE.startTime")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name()), eq("apiId")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiId")
                    .setFqn("API_TRACE.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .addSources(AttributeSource.EDS)
                    .setId("API_TRACE.apiId")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name()), eq("apiName")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiName")
                    .setFqn("API_TRACE.apiName")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API_TRACE.apiName")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name()), eq("apiBoundaryType")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiBoundaryType")
                    .setFqn("API_TRACE.apiBoundaryType")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.EDS)
                    .setId("API_TRACE.apiBoundaryType")
                    .build()));

    // mandatory
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE.name()), eq("spaceIds")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setId("API_TRACE.spaceIds")
                    .setKey("spaceIds")
                    .setFqn("API_TRACE.spaceIds")
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }
}
