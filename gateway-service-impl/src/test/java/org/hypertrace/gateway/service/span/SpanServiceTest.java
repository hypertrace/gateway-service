package org.hypertrace.gateway.service.span;

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
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorServiceFactory;
import org.hypertrace.gateway.service.v1.span.SpansRequest;
import org.hypertrace.gateway.service.v1.span.SpansResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SpanServiceTest extends AbstractGatewayServiceTest {

  private static final String TENANT_ID = "tenant-id";

  private LogConfig logConfig;
  private AttributeMetadataProvider attributeMetadataProvider;
  private QueryServiceClient queryServiceClient;
  private SpanService spanService;
  private ExecutorService queryExecutor;

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

    spanService = new SpanService(queryServiceClient, attributeMetadataProvider, queryExecutor);
  }

  @AfterEach
  public void clear() {
    queryExecutor.shutdown();
  }

  @Test
  public void test_getSpans_Without_Total() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    RequestContext requestContext = new RequestContext(forTenantId(TENANT_ID));

    SpansRequest spansRequest =
        SpansRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(QueryExpressionUtil.buildAttributeExpression("EVENT.apiName"))
            .build();

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("EVENT.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter("EVENT.startTime", startTime, endTime))
            .build();

    when(queryServiceClient.executeQuery(any(), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of("EVENT.apiName"), new String[][] {{"apiName1"}, {"apiName2"}}))
                .iterator());

    SpansResponse response = spanService.getSpansByFilter(requestContext, spansRequest);

    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedQueryRequest));

    assertNotNull(response);
    assertEquals(-1, response.getTotal());
  }

  @Test
  public void test_getSpans_With_Total() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    RequestContext requestContext = new RequestContext(forTenantId(TENANT_ID));

    SpansRequest spansRequest =
        SpansRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(QueryExpressionUtil.buildAttributeExpression("EVENT.apiName"))
            .setFetchTotal(true)
            .build();

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("EVENT.apiName"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter("EVENT.startTime", startTime, endTime))
            .build();

    QueryRequest expectedTotalQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(QueryRequestUtil.createCountByColumnSelection("EVENT.startTime"))
            .setFilter(
                QueryRequestUtil.createBetweenTimesFilter("EVENT.startTime", startTime, endTime))
            .setLimit(1)
            .build();

    when(queryServiceClient.executeQuery(any(), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of("EVENT.apiName"), new String[][] {{"apiName1"}, {"apiName2"}}))
                .iterator());

    when(queryServiceClient.executeQuery(any(), eq(expectedTotalQueryRequest)))
        .thenReturn(
            List.of(getResultSetChunk(List.of("COUNT_EVENT_apiName"), new String[][] {{"2"}}))
                .iterator());

    SpansResponse response = spanService.getSpansByFilter(requestContext, spansRequest);

    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedQueryRequest));
    verify(queryServiceClient, times(1)).executeQuery(any(), eq(expectedTotalQueryRequest));

    assertNotNull(response);
    assertEquals(2, response.getTotal());
  }

  private void mockAttributeMetadata(AttributeMetadataProvider attributeMetadataProvider) {
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name())))
        .thenReturn(
            Map.of(
                "EVENT.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setKey("startTime")
                    .setFqn("EVENT.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("EVENT.startTime")
                    .build(),
                "EVENT.apiName",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setKey("apiName")
                    .setFqn("EVENT.name")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("EVENT.name")
                    .build(),
                "EVENT.spaceIds",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setId("EVENT.spaceIds")
                    .setKey("spaceIds")
                    .setFqn("EVENT.spaceIds")
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    // each value
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setKey("startTime")
                    .setFqn("EVENT.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("EVENT.startTime")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name()), eq("apiName")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setKey("apiName")
                    .setFqn("EVENT.apiName")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("EVENT.apiName")
                    .build()));

    // mandatory
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name()), eq("spaceIds")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setId("EVENT.spaceIds")
                    .setKey("spaceIds")
                    .setFqn("EVENT.spaceIds")
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }
}
