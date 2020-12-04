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
import org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilter;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfig;
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
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class RequestPreProcessorTest {
  private static final String TEST_TENANT_ID = "test-tenant-id";
  private RequestPreProcessor requestPreProcessor;

  @Mock
  private AttributeMetadataProvider attributeMetadataProvider;
  @Mock
  private ScopeFilterConfigs scopeFilterConfigs;

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    scopeFilterConfigs = initializeScopeFilterConfigs("configs/request-preprocessor-test/service-id-config.conf");
    requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider, scopeFilterConfigs);
  }

//  @AfterEach
//  public void teardown() {
//    DomainObjectConfigs.clearDomainObjectConfigs();
//  }

  @Test
  public void testServiceEntitiesRequestDuplicateColumnSelectionIsRemovedAndScopeFilterConfigsAdded() {
    //initializeDomainObjectConfigs("configs/request-preprocessor-test/service-id-config.conf");
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "SERVICE", "SERVICE.startTime", Map.of());
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "id");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "name");
    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "startTime");

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setFilter(
                GatewayExpressionCreator.createFilter(
                    QueryExpressionUtil.getColumnExpression("SERVICE.name"),
                    Operator.LIKE,
                    QueryExpressionUtil.getLiteralExpression("log")))
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
        requestPreProcessor.transformFilter(entitiesRequest, entitiesRequestContext);

    // RequestPreProcessor should remove duplicate Service.Id selection and add scope filters config.
    Assertions.assertEquals(
        EntitiesRequest.newBuilder()
            .setEntityType("SERVICE")
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        Filter.newBuilder().setOperator(Operator.AND)
                            .addChildFilter(
                                GatewayExpressionCreator.createFilter(
                                    QueryExpressionUtil.getColumnExpression("SERVICE.name"),
                                    Operator.LIKE,
                                    QueryExpressionUtil.getLiteralExpression("log")))
                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
                    )
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.AND)
                            .addChildFilter(
                                GatewayExpressionCreator.createFilter(
                                    QueryExpressionUtil.getColumnExpression("SERVICE.id"),
                                    Operator.NEQ,
                                    QueryExpressionUtil.getLiteralExpression("null")
                                )
                            ).addChildFilter(
                                GatewayExpressionCreator.createFilter(
                                    QueryExpressionUtil.getColumnExpression("SERVICE.name"),
                                    Operator.NEQ,
                                    QueryExpressionUtil.getLiteralExpression("foo")
                                )
                            )
                    )
            )
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
  public void testApiTracesRequestScopeFilterConfigsAdded() {
    //initializeDomainObjectConfigs("configs/request-preprocessor-test/service-id-config.conf");
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000L;
    RequestContext requestContext =
        new RequestContext(TEST_TENANT_ID, Map.of());
    mockAttributeMetadata(requestContext, AttributeScope.API_TRACE.name(), "apiBoundaryType");
    mockAttributeMetadata(requestContext, AttributeScope.API_TRACE.name(), "apiId");
    //mockAttributeMetadata(requestContext, AttributeScope.API_TRACE.name(), "startTime");
    TracesRequest tracesRequest = TracesRequest.newBuilder()
        .setStartTimeMillis(startTime)
        .setEndTimeMillis(endTime)
        .setScope("API_TRACE")
        .setFilter(
            GatewayExpressionCreator.createFilter(
                QueryExpressionUtil.getColumnExpression("API_TRACE.serviceName"),
                Operator.LIKE,
                QueryExpressionUtil.getLiteralExpression("log"))
        )
        .addSelection(QueryExpressionUtil.getColumnExpression("API_TRACE.id"))
        .addSelection(QueryExpressionUtil.getColumnExpression("API_TRACE.serviceName"))
        .build();

    TracesRequest transformedRequest =
        requestPreProcessor.transformFilter(tracesRequest, requestContext);

    // RequestPreProcessor should add scope filters config.
    Assertions.assertEquals(
        TracesRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setScope("API_TRACE")
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        GatewayExpressionCreator.createFilter(
                            QueryExpressionUtil.getColumnExpression("API_TRACE.serviceName"),
                            Operator.LIKE,
                            QueryExpressionUtil.getLiteralExpression("log"))
                    )
                    .addChildFilter(
                        Filter.newBuilder()
                            .setOperator(Operator.AND)
                            .addChildFilter(
                                GatewayExpressionCreator.createFilter(
                                    QueryExpressionUtil.getColumnExpression("API_TRACE.apiBoundaryType"),
                                    Operator.EQ,
                                    QueryExpressionUtil.getLiteralExpression("ENTRY")
                                )
                            ).addChildFilter(
                            GatewayExpressionCreator.createFilter(
                                QueryExpressionUtil.getColumnExpression("API_TRACE.apiId"),
                                Operator.NEQ,
                                QueryExpressionUtil.getLiteralExpression("null")
                            )
                        )
                    )
            )
            .addSelection(QueryExpressionUtil.getColumnExpression("API_TRACE.id"))
            .addSelection(QueryExpressionUtil.getColumnExpression("API_TRACE.serviceName"))
            .build(),
        transformedRequest);
  }

//  @Test
//  public void testDomainEntitiesRequestWithLikeFilterIsTransformed() {
//    //initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000L;
//    EntitiesRequestContext entitiesRequestContext =
//        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "EVENT", "SERVICE.startTime", Map.of());
//    // Mock calls into attributeMetadataProvider
//    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);
//
//    EntitiesRequest entitiesRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("EVENT")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                GatewayExpressionCreator.createFilter(
//                    QueryExpressionUtil.getColumnExpression("EVENT.name"),
//                    Operator.LIKE,
//                    QueryExpressionUtil.getLiteralExpression("log")))
//            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.id"))
//            .addSelection(
//                QueryExpressionUtil.getColumnExpression(
//                    "EVENT.id")) // Duplicate should be removed by the processor
//            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.name"))
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration"))
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "EVENT.errorCount", FunctionType.SUM, "SUM#EVENT|errorCount"))
//            .addOrderBy(
//                QueryExpressionUtil.getOrderBy(
//                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
//            .build();
//
//    EntitiesRequest transformedRequest =
//        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);
//
//    // RequestPreProcessor should remove duplicate Service.Id selection.
//    Assertions.assertEquals(
//        EntitiesRequest.newBuilder()
//            .setEntityType("EVENT")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                Filter.newBuilder()
//                    .setOperator(Operator.AND)
//                    .addChildFilter(
//                        GatewayExpressionCreator.createFilter(
//                            QueryExpressionUtil.getColumnExpression("API.isExternal"),
//                            Operator.EQ,
//                            GatewayExpressionCreator.createLiteralExpression(true)))
//                    .addChildFilter(
//                        Filter.newBuilder()
//                            .setOperator(Operator.AND)
//                            .addChildFilter(GatewayExpressionCreator.createFilter(
//                                QueryExpressionUtil.getColumnExpression("SERVICE.hostHeader"),
//                                Operator.LIKE,
//                                QueryExpressionUtil.getLiteralExpression("log")))
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
//                    )
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.hostHeader"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.isExternal"))
//            // Note that the aliases are not transformed
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration"))
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "SERVICE.errorCount", FunctionType.SUM, "SUM#EVENT|errorCount"))
//            .addOrderBy(
//                QueryExpressionUtil.getOrderBy(
//                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
//            .build(),
//        transformedRequest);
//  }
//
//  @Test
//  public void testDomainEntitiesRequestWithNeqFilterIsTransformed() {
//    //initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000L;
//    EntitiesRequestContext entitiesRequestContext =
//        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "EVENT", "SERVICE.startTime", Map.of());
//    // Mock calls into attributeMetadataProvider
//    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);
//
//    EntitiesRequest entitiesRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("EVENT")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                GatewayExpressionCreator.createFilter(
//                    QueryExpressionUtil.getColumnExpression("EVENT.name"),
//                    Operator.NEQ,
//                    QueryExpressionUtil.getLiteralExpression("some-entity-name")))
//            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.id"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("EVENT.name"))
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration"))
//            .addOrderBy(
//                QueryExpressionUtil.getOrderBy(
//                    "EVENT.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
//            .build();
//
//    EntitiesRequest transformedRequest =
//        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);
//
//    // RequestPreProcessor should remove duplicate Service.Id selection.
//    EntitiesRequest expectedRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("EVENT")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                Filter.newBuilder()
//                    .setOperator(Operator.AND)
//                    .addChildFilter(
//                        GatewayExpressionCreator.createFilter(
//                            QueryExpressionUtil.getColumnExpression("API.isExternal"),
//                            Operator.EQ,
//                            GatewayExpressionCreator.createLiteralExpression(true))
//                    )
//                    .addChildFilter(
//                        Filter.newBuilder()
//                            .setOperator(Operator.AND)
//                            .addChildFilter(GatewayExpressionCreator.createFilter(
//                                QueryExpressionUtil.getColumnExpression(
//                                    "SERVICE.hostHeader"),
//                                Operator.NEQ,
//                                QueryExpressionUtil.getLiteralExpression(
//                                    "some-entity-name")))
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
//                    )
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.hostHeader"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.isExternal"))
//            // Note that the aliases are not transformed
//            .addSelection(
//                QueryExpressionUtil.getAggregateFunctionExpression(
//                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration"))
//            .addOrderBy(
//                QueryExpressionUtil.getOrderBy(
//                    "SERVICE.duration", FunctionType.AVG, "AVG#EVENT|duration", SortOrder.DESC))
//            .build();
//    Assertions.assertEquals(expectedRequest, transformedRequest);
//  }
//
//  @Test
//  public void testDomainEntitiesRequestWithServiceIdFilterIsNotTransformed() {
//    //initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000L;
//    EntitiesRequestContext entitiesRequestContext =
//        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "SERVICE", "SERVICE.startTime", Map.of());
//    // Mock calls into attributeMetadataProvider
//    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);
//
//    EntitiesRequest entitiesRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                GatewayExpressionCreator.createFilter(
//                    QueryExpressionUtil.getColumnExpression("SERVICE.id"),
//                    Operator.IN,
//                    createStringArrayLiteralExpressionBuilder(List.of("service1-id", "service2-id"))
//                )
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build();
//
//    // There should be no transformation except addition of time range filter.
//    EntitiesRequest expectedRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                Filter.newBuilder()
//                    .setOperator(Operator.AND)
//                    .addChildFilter(
//                        GatewayExpressionCreator.createFilter(
//                            QueryExpressionUtil.getColumnExpression("SERVICE.id"),
//                            Operator.IN,
//                            createStringArrayLiteralExpressionBuilder(List.of("service1-id", "service2-id"))
//                        )
//                    )
//                    .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
//                    .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build();
//
//    EntitiesRequest transformedRequest =
//        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);
//
//    Assertions.assertEquals(expectedRequest, transformedRequest);
//  }
//
//  @Test
//  public void testDomainEntitiesRequestWithInFilterIsTransformed() {
//    //initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000L;
//    EntitiesRequestContext entitiesRequestContext =
//        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "EVENT", "SERVICE.startTime", Map.of());
//    // Mock calls into attributeMetadataProvider
//    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);
//
//    EntitiesRequest entitiesRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                GatewayExpressionCreator.createFilter(
//                    QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr1"),
//                    Operator.IN,
//                    createStringArrayLiteralExpressionBuilder(List.of("attr1_val100:::attr2_val200", "attr1_val101:::attr2_val201"))))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr1"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build();
//
//    EntitiesRequest transformedRequest =
//        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);
//
//    // RequestPreProcessor should transform SERVICE.mappedAttr1.
//    Assertions.assertEquals(
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                Filter.newBuilder()
//                    .setOperator(Operator.AND)
//                    .addChildFilter(
//                        Filter.newBuilder()
//                            .setOperator(Operator.OR)
//                            .addChildFilter(
//                                Filter.newBuilder()
//                                    .setOperator(Operator.AND)
//                                    .addChildFilter(
//                                        GatewayExpressionCreator.createFilter(
//                                            QueryExpressionUtil.getColumnExpression(
//                                                "SERVICE.attr1"),
//                                            Operator.EQ,
//                                            QueryExpressionUtil.getLiteralExpression("attr1_val100")
//                                        )
//                                    )
//                                    .addChildFilter(
//                                        GatewayExpressionCreator.createFilter(
//                                            QueryExpressionUtil.getColumnExpression(
//                                                "SERVICE.attr2"),
//                                            Operator.EQ,
//                                            QueryExpressionUtil.getLiteralExpression("attr2_val200")
//                                        )
//                                    )
//                            )
//                            .addChildFilter(
//                                Filter.newBuilder()
//                                    .setOperator(Operator.AND)
//                                    .addChildFilter(
//                                        GatewayExpressionCreator.createFilter(
//                                            QueryExpressionUtil.getColumnExpression(
//                                                "SERVICE.attr1"),
//                                            Operator.EQ,
//                                            QueryExpressionUtil.getLiteralExpression("attr1_val101")
//                                        )
//                                    )
//                                    .addChildFilter(
//                                        GatewayExpressionCreator.createFilter(
//                                            QueryExpressionUtil.getColumnExpression(
//                                                "SERVICE.attr2"),
//                                            Operator.EQ,
//                                            QueryExpressionUtil.getLiteralExpression("attr2_val201")
//                                        )
//                                    )
//                            )
//                    )
//                    .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
//                    .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.attr1"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.attr2"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build(),
//        transformedRequest);
//  }
//
//  @Test
//  public void testTransformFilter() {
//
//  }
//
//  @Test
//  public void testDomainEntitiesRequestWithEqFilterIsTransformed() {
//    //initializeDomainObjectConfigs("configs/request-preprocessor-test/domains-config.conf");
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000L;
//    EntitiesRequestContext entitiesRequestContext =
//        new EntitiesRequestContext(TEST_TENANT_ID, startTime, endTime, "EVENT", "SERVICE.startTime", Map.of());
//    // Mock calls into attributeMetadataProvider
//    mockAttributeMetadataForDomainAndMappings(entitiesRequestContext);
//
//    EntitiesRequest entitiesRequest =
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                GatewayExpressionCreator.createFilter(
//                    QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
//                    Operator.EQ,
//                    GatewayExpressionCreator.createLiteralExpression("attr10_val20")
//                )
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build();
//
//    EntitiesRequest transformedRequest =
//        requestPreProcessor.transform(entitiesRequest, entitiesRequestContext);
//
//    // RequestPreProcessor should transform SERVICE.mappedAttr2.
//    Assertions.assertEquals(
//        EntitiesRequest.newBuilder()
//            .setEntityType("SERVICE")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .setFilter(
//                Filter.newBuilder()
//                    .setOperator(Operator.AND)
//                    .addChildFilter(
//                        GatewayExpressionCreator.createFilter(
//                            QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
//                            Operator.EQ,
//                            GatewayExpressionCreator.createLiteralExpression("attr10_val")
//                        )
//                    )
//                    .addChildFilter(
//                        Filter.newBuilder()
//                            .setOperator(Operator.AND)
//                            .addChildFilter(
//                                GatewayExpressionCreator.createFilter(
//                                    QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"),
//                                    Operator.EQ,
//                                    GatewayExpressionCreator.createLiteralExpression("attr10_val20")
//                                )
//                            )
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.GE, startTime))
//                            .addChildFilter(EntitiesRequestAndResponseUtils.getTimestampFilter("SERVICE.startTime", Operator.LT, endTime))
//                    )
//            )
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.mappedAttr2"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("SERVICE.id"))
//            .build(),
//        transformedRequest);
//  }

//  private Expression.Builder createStringArrayLiteralExpressionBuilder(List<String> values) {
//    return Expression.newBuilder().setLiteral(
//        LiteralConstant.newBuilder().setValue(
//            Value.newBuilder()
//                .setValueType(ValueType.STRING_ARRAY)
//                .addAllStringArray(values)
//        )
//    );
//  }

//  private void mockAttributeMetadataForDomainAndMappings(
//      EntitiesRequestContext entitiesRequestContext) {
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "name");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "id");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "duration");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "errorCount");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.EVENT.name(), "startTime");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "hostHeader");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.API.name(), "isExternal", AttributeKind.TYPE_BOOL);
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "duration");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "errorCount");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "id");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "startTime");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "mappedAttr1");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr1");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr2");
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "mappedAttr2",
//        AttributeKind.TYPE_STRING);
//    mockAttributeMetadata(entitiesRequestContext, AttributeScope.SERVICE.name(), "attr10");
//  }

  private void mockAttributeMetadata(RequestContext requestContext, String attributeScope, String key) {
    when(attributeMetadataProvider.getAttributeMetadata(requestContext, attributeScope, key))
        .thenReturn(createAttributeMetadata(attributeScope, key));
  }

//  private void mockAttributeMetadata(RequestContext requestContext,
//                                     String attributeScope,
//                                     String key,
//                                     AttributeKind attributeKind) {
//    when(attributeMetadataProvider.getAttributeMetadata(
//        requestContext, attributeScope, key))
//        .thenReturn(createAttributeMetadata(attributeScope, key, attributeKind));
//  }

  private Optional<AttributeMetadata> createAttributeMetadata(
      String attributeScope, String key) {
    return Optional.of(
        AttributeMetadata.newBuilder()
            .setScopeString(attributeScope)
            .setKey(key)
            .setId(attributeScope + "." + key)
            .build());
  }

//  private Optional<AttributeMetadata> createAttributeMetadata(
//      String attributeScope, String key, AttributeKind kind) {
//    return Optional.of(
//        AttributeMetadata.newBuilder(createAttributeMetadata(attributeScope, key).orElseThrow())
//            .setValueKind(kind)
//            .build());
//  }

//  private void initializeDomainObjectConfigs(String filePath) {
//    String configFilePath =
//        Thread.currentThread().getContextClassLoader().getResource(filePath).getPath();
//    if (configFilePath == null) {
//      throw new RuntimeException("Cannot find config file in the classpath: " + filePath);
//    }
//
//    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
//    Config config = ConfigFactory.load(fileConfig);
//    DomainObjectConfigs.init(config);
//  }

  private ScopeFilterConfigs initializeScopeFilterConfigs(String filePath) {
    String configFilePath =
        Thread.currentThread().getContextClassLoader().getResource(filePath).getPath();
    if (configFilePath == null) {
      throw new RuntimeException("Cannot find config file in the classpath: " + filePath);
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config config = ConfigFactory.load(fileConfig);
    return new ScopeFilterConfigs(config);
  }
}
