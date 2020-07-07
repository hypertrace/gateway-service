package org.hypertrace.gateway.service.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.transformer.ResponsePostProcessor;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/** Tests for {@link RequestPreProcessor} */
public class EntitiesRequestResponseProcessorTest {

  private final EntitiesRequest originalRequest =
      EntitiesRequest.newBuilder()
          .setEntityType("API")
          .addSelection(QueryExpressionUtil.getColumnExpression("API.duplicate"))
          .addSelection(QueryExpressionUtil.getColumnExpression("API.duplicate"))
          .addSelection(QueryExpressionUtil.getColumnExpression("API.apiId"))
          .addSelection(QueryExpressionUtil.getColumnExpression("API.apiName"))
          .setFilter(QueryExpressionUtil.getSimpleFilter("API.apiId", "name1"))
          .addOrderBy(QueryExpressionUtil.getOrderBy("API.apiId", SortOrder.ASC))
          .build();
  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  private RequestPreProcessor requestPreProcessor;
  private ResponsePostProcessor responsePostProcessor;

  @BeforeEach
  public void setup() {
    mockAttributeMetadataProvider();
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider);
    responsePostProcessor = new ResponsePostProcessor(attributeMetadataProvider);
  }

  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void testEntitiesRequestTransform() {
    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(originalRequest, mock(EntitiesRequestContext.class));
    List<Expression> expressionList = transformedRequest.getSelectionList();
    Assertions.assertEquals(3, expressionList.size());
    Assertions.assertTrue(
        expressionList.stream()
            .map(Expression::getColumnIdentifier)
            .map(ColumnIdentifier::getColumnName)
            .collect(Collectors.toList())
            .containsAll(List.of("API.duplicate", "API.apiName", "API.apiId")));

    List<OrderByExpression> orderByExpressionList = transformedRequest.getOrderByList();
    Assertions.assertEquals(1, orderByExpressionList.size());
    Assertions.assertTrue(
        orderByExpressionList.stream()
            .map(OrderByExpression::getExpression)
            .map(Expression::getColumnIdentifier)
            .map(ColumnIdentifier::getColumnName)
            .collect(Collectors.toList())
            .contains("API.apiId"));

    Filter filter = transformedRequest.getFilter();
    Assertions.assertEquals(Operator.EQ, filter.getOperator());
    Assertions.assertEquals(0, filter.getChildFilterCount());
  }

  @Test
  public void testUniqueColumnsTransform() {
    EntitiesRequest originalRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("API")
            .addSelection(QueryExpressionUtil.getColumnExpression("API.duplicate"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API.duplicate"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API.duplicate"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API.apiName"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "API.metrics.bytes_received", FunctionType.SUM, "SUM_bytes_received", true))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(originalRequest, mock(EntitiesRequestContext.class));
    List<Expression> expressionList = transformedRequest.getSelectionList();
    Assertions.assertEquals(3, expressionList.size());
  }

  @Test
  public void testEntitiesResponseTransform() {
    EntitiesResponse.Builder originalResponseBuilder =
        EntitiesResponse.newBuilder()
            .addEntity(
                Entity.newBuilder()
                    .putAttribute(
                        "API.duplicate",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING)
                            .setString("duplicate")
                            .build())
                    .putAttribute(
                        "API.apiName",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING)
                            .setString("name1")
                            .build())
                    .putAttribute(
                        "API.apiId",
                        Value.newBuilder()
                            .setValueType(ValueType.STRING)
                            .setString("name1")
                            .build())
                    .build());

    EntitiesRequestContext context = mock(EntitiesRequestContext.class);

    EntitiesResponse.Builder transformedResponseBuilder =
        responsePostProcessor.transform(originalRequest, context, originalResponseBuilder);
    List<Entity> entities = transformedResponseBuilder.getEntityList();
    Assertions.assertEquals(1, entities.size());
    Assertions.assertEquals(
        "duplicate", entities.get(0).getAttributeMap().get("API.duplicate").getString());
    Assertions.assertEquals(
        "name1", entities.get(0).getAttributeMap().get("API.apiId").getString());
    Assertions.assertEquals(
        "name1", entities.get(0).getAttributeMap().get("API.apiName").getString());
    Assertions.assertNull(entities.get(0).getAttributeMap().getOrDefault("API.isExternal", null));
  }

  private void mockAttributeMetadataProvider() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API), eq("apiId")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.API)
                    .setKey("apiId")
                    .setFqn("API.apiId")
                    .setId("API.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API), eq("apiName")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.API)
                    .setKey("apiName")
                    .setFqn("API.name")
                    .setId("API.apiName")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API), eq("isExternal")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScope(AttributeScope.API)
                    .setKey("isExternal")
                    .setFqn("API.external")
                    .setId("API.isExternal")
                    .setValueKind(AttributeKind.TYPE_BOOL)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }
}
