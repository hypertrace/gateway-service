package org.hypertrace.gateway.service.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.transformer.ResponsePostProcessor;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class TracesRequestResponseProcessorTest {
  private final TracesRequest originalRequest =
      TracesRequest.newBuilder()
          .setScope(TraceScope.API_TRACE.name())
          .addSelection(QueryExpressionUtil.getColumnExpression("API_TRACE.domainId"))
          .setFilter(QueryExpressionUtil.getSimpleFilter("API_TRACE.domainId", "name1:::true"))
          .addOrderBy(QueryExpressionUtil.getOrderBy("API_TRACE.domainId", SortOrder.ASC))
          .build();
  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  private RequestPreProcessor requestPreProcessor;
  private ResponsePostProcessor responsePostProcessor;

  @BeforeEach
  public void setup() {
    mockAttributeMetadataProvider();
    mockDomainObjectConfigs();
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider);
    responsePostProcessor = new ResponsePostProcessor(attributeMetadataProvider);
  }

  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void testTracesRequestTransform() {
    TracesRequest transformedRequest =
        requestPreProcessor.transform(originalRequest, mock(RequestContext.class));
    List<Expression> expressionList = transformedRequest.getSelectionList();
    Assertions.assertEquals(2, expressionList.size());
    Assertions.assertTrue(
        expressionList.stream()
            .map(Expression::getColumnIdentifier)
            .map(ColumnIdentifier::getColumnName)
            .collect(Collectors.toList())
            .containsAll(List.of("SERVICE.id", "API.isExternal")));

    List<OrderByExpression> orderByExpressionList = transformedRequest.getOrderByList();
    Assertions.assertEquals(2, orderByExpressionList.size());
    Assertions.assertTrue(
        orderByExpressionList.stream()
            .map(OrderByExpression::getExpression)
            .map(Expression::getColumnIdentifier)
            .map(ColumnIdentifier::getColumnName)
            .collect(Collectors.toList())
            .containsAll(List.of("SERVICE.id", "API.isExternal")));

    Filter filter = transformedRequest.getFilter();
    Assertions.assertEquals(Operator.AND, filter.getOperator());
    Assertions.assertEquals(2, filter.getChildFilterCount());
    List<Filter> childFilters =
        List.of(
            QueryExpressionUtil.getSimpleFilter("SERVICE.id", "name1").build(),
            QueryExpressionUtil.getSimpleFilter("API.isExternal", "true").build());
    Assertions.assertTrue(filter.getChildFilterList().containsAll(childFilters));
  }

  @Test
  public void testTracesResponseTransform() {
    TracesResponse.Builder originalResponseBuilder =
        TracesResponse.newBuilder()
            .addTraces(
                Trace.newBuilder()
                    .putAttributes(
                        "SERVICE.id",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING)
                            .setString("name1")
                            .build())
                    .putAttributes(
                        "API.isExternal",
                        Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(true).build())
                    .build());

    RequestContext context = mock(RequestContext.class);

    TracesResponse.Builder transformedResponseBuilder =
        responsePostProcessor.transform(originalRequest, context, originalResponseBuilder);
    List<Trace> traces = transformedResponseBuilder.getTracesList();
    Assertions.assertEquals(1, traces.size());
    Assertions.assertEquals(
        "name1:::true", traces.get(0).getAttributesMap().get("API_TRACE.domainId").getString());
  }

  private void mockAttributeMetadataProvider() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API_TRACE), eq("domainId")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.API_TRACE)
                    .setKey("domainId")
                    .setFqn("Api.Trace.domainId")
                    .setId("API_TRACE.domainId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE), eq("id")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.SERVICE)
                    .setKey("id")
                    .setFqn("Service.id")
                    .setId("SERVICE.id")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API), eq("isExternal")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.API)
                    .setKey("isExternal")
                    .setFqn("API.is_external")
                    .setId("API.isExternal")
                    .setValueKind(AttributeKind.TYPE_BOOL)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }

  private void mockDomainObjectConfigs() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API_TRACE\n"
            + "    key = domainId\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = SERVICE\n"
            + "        key = id\n"
            + "      },\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = isExternal\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);
  }
}
