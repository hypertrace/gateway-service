package org.hypertrace.gateway.service.common.transformer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.testutils.GatewayExpressionCreator;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
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
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider,
        new ScopeFilterConfigs(ConfigFactory.empty()));
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
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "id");
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

  @Test
  public void testDomainEntitiesRequestWithServiceIdFilterIsNotTransformed() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "SERVICE", Map.of());
    // Mock calls into attributeMetadataProvider
    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);

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
                            QueryExpressionUtil.getColumnExpression("SERVICE.id"),
                            Operator.IN,
                            createStringArrayLiteralExpressionBuilder(List.of("service1-id", "service2-id"))
                        )
                    )
            )
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // There should be no transformation
    Assertions.assertEquals(entitiesRequest, transformedRequest);
  }

  @Test
  public void testDomainEntitiesRequestWithInFilterIsTransformed() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "EVENT", Map.of());
    // Mock calls into attributeMetadataProvider
    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);

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
                            QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr1"),
                            Operator.IN,
                            createStringArrayLiteralExpressionBuilder(List.of("attr1_val100:::attr2_val200", "attr1_val101:::attr2_val201")))))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr1"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should transform SERVICE.mappedAttr1.
    Assertions.assertEquals(
        EntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(0L)
            .setEndTimeMillis(1L)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.OR)
                            .addChildFilter(
                                Filter.newBuilder()
                                    .setOperator(Operator.AND)
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.attr1"),
                                            Operator.EQ,
                                            QueryExpressionUtil.getLiteralExpression("attr1_val100")
                                        )
                                    )
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.attr2"),
                                            Operator.EQ,
                                            QueryExpressionUtil.getLiteralExpression("attr2_val200")
                                        )
                                    )
                            )
                            .addChildFilter(
                                Filter.newBuilder()
                                    .setOperator(Operator.AND)
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.attr1"),
                                            Operator.EQ,
                                            QueryExpressionUtil.getLiteralExpression("attr1_val101")
                                        )
                                    )
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression(
                                                "SERVICE.attr2"),
                                            Operator.EQ,
                                            QueryExpressionUtil.getLiteralExpression("attr2_val201")
                                        )
                                    )
                            )
                    )
            )
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.attr1"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.attr2"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .build(),
        transformedRequest);
  }

  @Test
  public void testDomainEntitiesRequestWithEqFilterIsTransformed() {
    initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, 0L, 1L, "EVENT", Map.of());
    // Mock calls into attributeMetadataProvider
    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);

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
                            QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
                            Operator.EQ,
                            GatewayExpressionCreator.createLiteralExpression("attr10_val20")
                        )
                    )
            )
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .build();

    EntitiesRequest transformedRequest =
        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should transform SERVICE.mappedAttr2.
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
                            QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
                            Operator.EQ,
                            GatewayExpressionCreator.createLiteralExpression("attr10_val")
                        )
                    )
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.AND)
                            .addChildFilter(
                                Filter.newBuilder()
                                    .setOperator(Operator.AND)
                                    .addChildFilter(
                                        GatewayExpressionCreator.createFilter(
                                            QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
                                            Operator.EQ,
                                            GatewayExpressionCreator.createLiteralExpression("attr10_val20")
                                        )
                                    )
                            )
                    )
            )
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"))
            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
            .build(),
        transformedRequest);
  }

  private Expression.Builder createStringArrayLiteralExpressionBuilder(List<String> values) {
    return Expression.newBuilder().setLiteral(
        LiteralConstant.newBuilder().setValue(
            Value.newBuilder()
                .setValueType(ValueType.STRING_ARRAY)
                .addAllStringArray(values)
        )
    );
  }

  private void mockAttributeMetadataForDomainAndMappings(
      EntitiesRequestContext entitiesRequestContext) {
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "name");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "id");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "duration");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "errorCount");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "hostHeader");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.API.name(), "isExternal", AttributeKind.TYPE_BOOL);
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "duration");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "errorCount");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "id");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "mappedAttr1");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr1");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr2");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "mappedAttr2",
        AttributeKind.TYPE_STRING);
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr10");
  }

  private void mockAttributeMetadata(
      EntitiesRequestContext entitiesRequestContext, String attributeScope, String key) {
    when(attributeMetadataProvider.getAttributeMetadata(
            entitiesRequestContext, attributeScope, key))
        .thenReturn(createAttributeMetadata(attributeScope, key));
  }

  private void mockAttributeMetadata(
      EntitiesRequestContext entitiesRequestContext,
      String attributeScope, String key, AttributeKind attributeKind) {
    when(attributeMetadataProvider.getAttributeMetadata(
            entitiesRequestContext, attributeScope, key))
        .thenReturn(createAttributeMetadata(attributeScope, key, attributeKind));
  }

  private Optional<AttributeMetadata> createAttributeMetadata(
      String attributeScope, String key) {
    return Optional.of(
        AttributeMetadata.newBuilder()
            .setScopeString(attributeScope)
            .setKey(key)
            .setId(attributeScope + "." + key)
            .build());
  }

  private Optional<AttributeMetadata> createAttributeMetadata(
      String attributeScope, String key, AttributeKind kind) {
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
