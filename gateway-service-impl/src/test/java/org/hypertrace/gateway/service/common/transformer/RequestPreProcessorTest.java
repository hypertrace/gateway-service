package org.hypertrace.gateway.service.common.transformer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.testutils.GatewayExpressionCreator;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class RequestPreProcessorTest {
  private static final String TEST_TENANT_ID = "test-tenant-id";
  private RequestPreProcessor requestPreProcessor;

  @Mock private AttributeMetadataProvider attributeMetadataProvider;

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider);
  }

  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void testServiceEntitiesRequestDuplicateColumnSelectionIsRemoved() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/service-id-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "SERVICE", Map.of());
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE, "id");
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("SERVICE.name"),
                            Operator.LIKE,
                            QueryExpressionUtil.getLiteralExpression("log"))))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .addSelection(
                QueryExpressionUtil.getColumnExpression(
                    "SERVICE.id")) // Duplicate should be removed by the processor
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.name"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.duration", FunctionType.AVG, "AVG#SERVICE|duration"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.errorCount", FunctionType.SUM, "SUM#SERVICE|errorCount"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "SERVICE.duration", FunctionType.AVG, "AVG#SERVICE|duration", SortOrder.DESC))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should remove duplicate Service.Id selection.
    Assertions.assertEquals(
        EntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("SERVICE.name"),
                            Operator.LIKE,
                            QueryExpressionUtil.getLiteralExpression("log"))))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.name"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.duration", FunctionType.AVG, "AVG#SERVICE|duration"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.errorCount", FunctionType.SUM, "SUM#SERVICE|errorCount"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "SERVICE.duration", FunctionType.AVG, "AVG#SERVICE|duration", SortOrder.DESC))
            .build(),
        transformedRequest);
  }

  @Test
  public void testDomainEntitiesRequestWithLikeFilterIsTransformed() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "EVENT", Map.of());
    // Mock calls into attributeMetadataProvider
    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("EVENT")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("EVENT.name"),
                            Operator.LIKE,
                            QueryExpressionUtil.getLiteralExpression("log"))))
            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.id"))
            .addSelection(
                QueryExpressionUtil.getColumnExpression(
                    "EVENT.id")) // Duplicate should be removed by the processor
            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.name"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "EVENT.errorCount", FunctionType.SUM, "SUM#EVENT|errorCount"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should remove duplicate Service.Id selection.
    Assertions.assertEquals(
        EntitiesRequest.newBuilder()
            .setEntityType("EVENT")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("API.isExternal"),
                            Operator.EQ,
                            GatewayExpressionCreator.createLiteralExpression(true)))
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.AND)
                            .addChildFilter(
                                Filter.newBuilder()
                                    .setOperator(Operator.AND)
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.hostHeader"),
                                            Operator.LIKE,
                                            QueryExpressionUtil.getLiteralExpression("log"))))))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.hostHeader"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API.isExternal"))
            // Note that the aliases are not transformed
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.errorCount", FunctionType.SUM, "SUM#EVENT|errorCount"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
            .build(),
        transformedRequest);
  }

  @Test
  public void testDomainEntitiesRequestWithNeqFilterIsTransformed() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "EVENT", Map.of());
    // Mock calls into attributeMetadataProvider
    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("EVENT")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("EVENT.name"),
                            Operator.NEQ,
                            QueryExpressionUtil.getLiteralExpression("some-entity-name"))))
            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.id"))
            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.name"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should remove duplicate Service.Id selection.
    Assertions.assertEquals(
        EntitiesRequest.newBuilder()
            .setEntityType("EVENT")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("API.isExternal"),
                            Operator.EQ,
                            GatewayExpressionCreator.createLiteralExpression(true)))
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.AND)
                            .addChildFilter(
                                Filter.newBuilder()
                                    .setOperator(Operator.OR)
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.hostHeader"),
                                            Operator.NEQ,
                                            QueryExpressionUtil.getLiteralExpression(
                                                "some-entity-name"))))))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.hostHeader"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API.isExternal"))
            // Note that the aliases are not transformed
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration"))
            .addOrderBy(
                QueryExpressionUtil.getOrderBy(
                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
            .build(),
        transformedRequest);
  }

  private void mockAttributeMetadataForDomainAndMappings(
      EntitiesRequestContext entitiesRequestContext) {
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT, "name");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT, "id");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT, "duration");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT, "errorCount");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE, "hostHeader");
    mockAttributeMetadata(
        entitiesRequestContext, AttributeScope.API, "isExternal", AttributeKind.TYPE_BOOL);
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE, "duration");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE, "errorCount");
  }

  private void mockAttributeMetadata(
      EntitiesRequestContext entitiesRequestContext, AttributeScope attributeScope, String key) {
    when(attributeMetadataProvider.getAttributeMetadata(
            entitiesRequestContext, attributeScope, key))
        .thenReturn(createAttributeMetadata(attributeScope, key));
  }

  private void mockAttributeMetadata(
      EntitiesRequestContext entitiesRequestContext,
      AttributeScope attributeScope,
      String key,
      AttributeKind attributeKind) {
    when(attributeMetadataProvider.getAttributeMetadata(
            entitiesRequestContext, attributeScope, key))
        .thenReturn(createAttributeMetadata(attributeScope, key, attributeKind));
  }

  private Optional<AttributeMetadata> createAttributeMetadata(
      AttributeScope attributeScope, String key) {
    return Optional.of(
        AttributeMetadata.newBuilder()
            .setScope(attributeScope)
            .setKey(key)
            .setId(attributeScope + "." + key)
            .build());
  }

  private Optional<AttributeMetadata> createAttributeMetadata(
      AttributeScope attributeScope, String key, AttributeKind kind) {
    return Optional.of(
        AttributeMetadata.newBuilder(createAttributeMetadata(attributeScope, key).orElseThrow())
            .setValueKind(kind)
            .build());
  }

  private void initializeDomainObjectConfigs(String filePath) {
    String configFilePath =
        Thread.currentThread().getContextClassLoader().getResource(filePath).getPath();
    if (configFilePath == null) {
      throw new RuntimeException("Cannot find config file in the classpath: " + filePath);
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config config = ConfigFactory.load(fileConfig);
    DomainObjectConfigs.init(config);
  }
}
