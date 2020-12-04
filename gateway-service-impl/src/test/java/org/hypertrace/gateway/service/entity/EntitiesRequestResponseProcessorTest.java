package org.hypertrace.gateway.service.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
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

  @BeforeEach
  public void setup() {
    mockAttributeMetadataProvider();
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider,
        new ScopeFilterConfigs(ConfigFactory.empty()));
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

    EntitiesRequestContext context = mock(EntitiesRequestContext.class);
    when(context.getTimestampAttributeId()).thenReturn("API.startTime");
    EntitiesRequest transformedRequest = requestPreProcessor.transformFilter(originalRequest, context);
    List<Expression> expressionList = transformedRequest.getSelectionList();
    Assertions.assertEquals(3, expressionList.size());
  }

  private void mockAttributeMetadataProvider() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API.name()), eq("apiId")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("apiId")
                    .setFqn("API.apiId")
                    .setId("API.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API.name()), eq("apiName")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("apiName")
                    .setFqn("API.name")
                    .setId("API.apiName")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(AttributeScope.API.name()), eq("isExternal")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("isExternal")
                    .setFqn("API.external")
                    .setId("API.isExternal")
                    .setValueKind(AttributeKind.TYPE_BOOL)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }
}
