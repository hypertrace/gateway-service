package org.hypertrace.gateway.service.entity;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsAggregationExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsDefaultRequestFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createAttributeExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createBetweenTimesFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCompositeFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCountByColumnSelection;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringArrayLiteralExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;
import static org.hypertrace.gateway.service.common.util.QueryExpressionUtil.buildAttributeExpression;
import static org.hypertrace.gateway.service.common.util.QueryExpressionUtil.getAggregateFunctionExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorServiceFactory;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.EntityInteraction;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class EntityServiceInteractionRequestTest extends AbstractGatewayServiceTest {
  private QueryServiceClient queryServiceClient;
  private EntityQueryServiceClient entityQueryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private EntityIdColumnsConfigs entityIdColumnsConfigs;
  private LogConfig logConfig;
  private ScopeFilterConfigs scopeFilterConfigs;
  private ExecutorService queryExecutor;

  @BeforeEach
  public void setup() {
    super.setup();
    mockEntityIdColumnConfigs();
    queryServiceClient = Mockito.mock(QueryServiceClient.class);
    entityQueryServiceClient = Mockito.mock(EntityQueryServiceClient.class);
    attributeMetadataProvider = Mockito.mock(AttributeMetadataProvider.class);
    mock(attributeMetadataProvider);
    logConfig = Mockito.mock(LogConfig.class);
    when(logConfig.getQueryThresholdInMillis()).thenReturn(1500L);
    scopeFilterConfigs = new ScopeFilterConfigs(ConfigFactory.empty());
    queryExecutor =
        QueryExecutorServiceFactory.buildExecutorService(
            QueryExecutorConfig.from(this.getConfig()));
  }

  @AfterEach
  public void clear() {
    queryExecutor.shutdown();
  }

  private void mockEntityIdColumnConfigs() {
    String entityIdColumnConfigStr =
        "entity.idcolumn.config = [\n"
            + "  {\n"
            + "    scope = SERVICE\n"
            + "    key = id\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BACKEND\n"
            + "    key = id\n"
            + "  }\n"
            + "]";
    Config config = ConfigFactory.parseString(entityIdColumnConfigStr);
    entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(config);
  }

  private void mock(AttributeMetadataProvider attributeMetadataProvider) {
    // interaction related attributes
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.INTERACTION.name())))
        .thenReturn(
            Map.of(
                "INTERACTION.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("startTime")
                    .setFqn("Interaction.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.startTime")
                    .build(),
                "INTERACTION.fromEntityType",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("fromEntityType")
                    .setFqn("Interaction.attributes.from_entity_type")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.fromEntityType")
                    .build(),
                "INTERACTION.toEntityType",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("toEntityType")
                    .setFqn("Interaction.attributes.to_entity_type")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.toEntityType")
                    .build(),
                "INTERACTION.numCalls",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("numCalls")
                    .setFqn("Interaction.metrics.num_calls")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.METRIC)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.numCalls")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.INTERACTION.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("startTime")
                    .setFqn("Interaction.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.startTime")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.INTERACTION.name()), eq("fromEntityType")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("fromEntityType")
                    .setFqn("Interaction.attributes.from_entity_type")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.fromEntityType")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.INTERACTION.name()), eq("numCalls")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("numCalls")
                    .setFqn("Interaction.attributes.to_entity_type")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.numCalls")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.INTERACTION.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.INTERACTION.name())
                    .setKey("startTime")
                    .setFqn("Interaction.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("INTERACTION.startTime")
                    .build()));

    // service scope related attributes
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE.name())))
        .thenReturn(
            Map.of(
                "SERVICE.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.SERVICE.name())
                    .setKey("startTime")
                    .setFqn("SERVICE.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("SERVICE.startTime")
                    .build(),
                "SERVICE.id",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.SERVICE.name())
                    .setKey("id")
                    .setFqn("SERVICE.id")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .addSources(AttributeSource.EDS)
                    .setId("SERVICE.id")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.SERVICE.name())
                    .setKey("startTime")
                    .setFqn("SERVICE.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("SERVICE.startTime")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE.name()), eq("id")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.SERVICE.name())
                    .setKey("id")
                    .setFqn("SERVICE.id")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setId("SERVICE.id")
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    // backend related attributes
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.BACKEND.name())))
        .thenReturn(
            Map.of(
                "BACKEND.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.BACKEND.name())
                    .setKey("startTime")
                    .setFqn("BACKEND.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("BACKEND.startTime")
                    .build(),
                "BACKEND.id",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.BACKEND.name())
                    .setKey("id")
                    .setFqn("BACKEND.id")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .addSources(AttributeSource.EDS)
                    .setId("BACKEND.id")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.BACKEND.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.BACKEND.name())
                    .setKey("startTime")
                    .setFqn("BACKEND.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("BACKEND.startTime")
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.BACKEND.name()), eq("id")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.BACKEND.name())
                    .setKey("id")
                    .setFqn("BACKEND.id")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setId("BACKEND.id")
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name()), eq("spaceIds")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.EVENT.name())
                    .setId("EVENT.spaceIds")
                    .setKey("spaceIds")
                    .setFqn("EVENT.spaceIds")
                    .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .build()));
  }

  private void mockQueryServiceRequestForServiceCount(long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter queryServiceFilter =
        createQsDefaultRequestFilter("SERVICE.startTime", "SERVICE.id", startTime, endTime);

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("SERVICE.id"))
            .addSelection(createQsAggregationExpression("COUNT", "SERVICE.id"))
            .setFilter(queryServiceFilter)
            .addGroupBy(createAttributeExpression("SERVICE.id"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor("SERVICE.id", "COUNT_service"))
                        .addRow(generateRowFor("test_service_1", "10.0"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceRequestForServiceCountWithInteractionFilters(
      long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter serviceIdFilter =
        createFilter(
            "SERVICE.id",
            org.hypertrace.core.query.service.api.Operator.IN,
            createStringArrayLiteralExpression(
                List.of("test_service_2", "test_service_1", "test_service_3")));

    org.hypertrace.core.query.service.api.Filter filter =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
            .addChildFilter(
                createFilter(
                    createAttributeExpression("SERVICE.id"),
                    org.hypertrace.core.query.service.api.Operator.NEQ,
                    createStringNullLiteralExpression()))
            .addChildFilter(createBetweenTimesFilter("SERVICE.startTime", startTime, endTime))
            .addChildFilter(
                createCompositeFilter(
                    org.hypertrace.core.query.service.api.Operator.AND,
                    List.of(serviceIdFilter, serviceIdFilter, serviceIdFilter)))
            .build();

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("SERVICE.id"))
            .addSelection(createQsAggregationExpression("COUNT", "SERVICE.id"))
            .setFilter(filter)
            .addGroupBy(createAttributeExpression("SERVICE.id"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor("SERVICE.id", "COUNT_service"))
                        .addRow(generateRowFor("test_service_1", "10.0"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceRequestForIncomingServiceInteraction(long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter timesFilter =
        QueryRequestUtil.createBetweenTimesFilter("INTERACTION.startTime", startTime, endTime);

    org.hypertrace.core.query.service.api.Filter toServiceIdFilter =
        QueryRequestUtil.createFilter(
            "INTERACTION.toServiceId",
            org.hypertrace.core.query.service.api.Operator.IN,
            createStringArrayLiteralExpression(List.of("test_service_1")));

    org.hypertrace.core.query.service.api.Filter fromServiceIdFilter =
        QueryRequestUtil.createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                createFilter(
                    "INTERACTION.fromServiceId",
                    org.hypertrace.core.query.service.api.Operator.NEQ,
                    createStringNullLiteralExpression())));

    org.hypertrace.core.query.service.api.Filter interactionQueryFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(timesFilter, toServiceIdFilter, fromServiceIdFilter));

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("INTERACTION.toServiceId"))
            .addSelection(createAttributeExpression("INTERACTION.fromServiceId"))
            .addSelection(
                createQsAggregationExpression("SUM", "INTERACTION.numCalls", "SUM_num_calls"))
            .setFilter(interactionQueryFilter)
            .addGroupBy(createAttributeExpression("INTERACTION.toServiceId"))
            .addGroupBy(createAttributeExpression("INTERACTION.fromServiceId"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor(
                                "INTERACTION.toServiceId",
                                "INTERACTION.fromServiceId",
                                "SUM_num_calls"))
                        .addRow(generateRowFor("test_service_1", "from_test_service_1", "40.0"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceRequestForIncomingServiceWithInteractionFilters(
      long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter timesFilter =
        QueryRequestUtil.createBetweenTimesFilter("INTERACTION.startTime", startTime, endTime);

    org.hypertrace.core.query.service.api.Filter toServiceIdFilter =
        QueryRequestUtil.createFilter(
            "INTERACTION.toServiceId",
            org.hypertrace.core.query.service.api.Operator.IN,
            createStringArrayLiteralExpression(List.of("test_service_1")));

    org.hypertrace.core.query.service.api.Filter fromServiceIdFilter =
        QueryRequestUtil.createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                QueryRequestUtil.createCompositeFilter(
                    org.hypertrace.core.query.service.api.Operator.AND,
                    List.of(
                        createFilter(
                            "INTERACTION.fromServiceId",
                            org.hypertrace.core.query.service.api.Operator.NEQ,
                            createStringNullLiteralExpression()))),
                createFilter(
                    "INTERACTION.otherAttribute",
                    org.hypertrace.core.query.service.api.Operator.IN,
                    createStringArrayLiteralExpression(List.of("attributeId")))));

    org.hypertrace.core.query.service.api.Filter interactionQueryFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(timesFilter, toServiceIdFilter, fromServiceIdFilter));

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("INTERACTION.toServiceId"))
            .addSelection(createAttributeExpression("INTERACTION.fromServiceId"))
            .addSelection(
                createQsAggregationExpression("SUM", "INTERACTION.numCalls", "SUM_num_calls"))
            .setFilter(interactionQueryFilter)
            .addGroupBy(createAttributeExpression("INTERACTION.toServiceId"))
            .addGroupBy(createAttributeExpression("INTERACTION.fromServiceId"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor(
                                "INTERACTION.toServiceId",
                                "INTERACTION.fromServiceId",
                                "SUM_num_calls"))
                        .addRow(generateRowFor("test_service_1", "from_test_service_1", "40.0"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceIdsRequestForInteractionFilterRequest(long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter timesFilter =
        QueryRequestUtil.createBetweenTimesFilter("INTERACTION.startTime", startTime, endTime);

    org.hypertrace.core.query.service.api.Filter fromServiceIdFilter =
        QueryRequestUtil.createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                QueryRequestUtil.createCompositeFilter(
                    org.hypertrace.core.query.service.api.Operator.AND,
                    List.of(
                        createFilter(
                            "INTERACTION.fromServiceId",
                            org.hypertrace.core.query.service.api.Operator.NEQ,
                            createStringNullLiteralExpression()))),
                createFilter(
                    "INTERACTION.otherAttribute",
                    org.hypertrace.core.query.service.api.Operator.IN,
                    createStringArrayLiteralExpression(List.of("attributeId")))));

    org.hypertrace.core.query.service.api.Filter interactionQueryFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(timesFilter, fromServiceIdFilter));

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("INTERACTION.toServiceId"))
            .addSelection(createCountByColumnSelection("INTERACTION.toServiceId"))
            .setFilter(interactionQueryFilter)
            .addGroupBy(createAttributeExpression("INTERACTION.toServiceId"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor("INTERACTION.toServiceId"))
                        .addRow(generateRowFor("test_service_1"))
                        .addRow(generateRowFor("test_service_2"))
                        .addRow(generateRowFor("test_service_3"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceRequestForOutgoingServiceInteraction(long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter timesFilter =
        QueryRequestUtil.createBetweenTimesFilter("INTERACTION.startTime", startTime, endTime);

    org.hypertrace.core.query.service.api.Filter fromServiceIdFilter =
        QueryRequestUtil.createFilter(
            "INTERACTION.fromServiceId",
            org.hypertrace.core.query.service.api.Operator.IN,
            createStringArrayLiteralExpression(List.of("test_service_1")));

    org.hypertrace.core.query.service.api.Filter toServiceIdFilter =
        QueryRequestUtil.createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                createFilter(
                    "INTERACTION.toServiceId",
                    org.hypertrace.core.query.service.api.Operator.NEQ,
                    createStringNullLiteralExpression())));

    org.hypertrace.core.query.service.api.Filter interactionQueryFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(timesFilter, fromServiceIdFilter, toServiceIdFilter));

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("INTERACTION.fromServiceId"))
            .addSelection(createAttributeExpression("INTERACTION.toServiceId"))
            .addSelection(
                createQsAggregationExpression("SUM", "INTERACTION.numCalls", "SUM_num_calls"))
            .setFilter(interactionQueryFilter)
            .addGroupBy(createAttributeExpression("INTERACTION.fromServiceId"))
            .addGroupBy(createAttributeExpression("INTERACTION.toServiceId"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor(
                                "INTERACTION.fromServiceId",
                                "INTERACTION.toServiceId",
                                "SUM_num_calls"))
                        .addRow(generateRowFor("test_service_1", "to_test_service_1", "20.0"))
                        .build())
                .iterator());
  }

  private void mockQueryServiceRequestForOutgoingBackendInteraction(long startTime, long endTime) {

    org.hypertrace.core.query.service.api.Filter timesFilter =
        QueryRequestUtil.createBetweenTimesFilter("INTERACTION.startTime", startTime, endTime);

    org.hypertrace.core.query.service.api.Filter fromServiceIdFilter =
        QueryRequestUtil.createFilter(
            "INTERACTION.fromServiceId",
            org.hypertrace.core.query.service.api.Operator.IN,
            createStringArrayLiteralExpression(List.of("test_service_1")));

    org.hypertrace.core.query.service.api.Filter toServiceIdFilter =
        QueryRequestUtil.createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(
                createFilter(
                    "INTERACTION.toBackendId",
                    org.hypertrace.core.query.service.api.Operator.NEQ,
                    createStringNullLiteralExpression())));

    org.hypertrace.core.query.service.api.Filter interactionQueryFilter =
        createCompositeFilter(
            org.hypertrace.core.query.service.api.Operator.AND,
            List.of(timesFilter, fromServiceIdFilter, toServiceIdFilter));

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createAttributeExpression("INTERACTION.fromServiceId"))
            .addSelection(createAttributeExpression("INTERACTION.toBackendId"))
            .addSelection(
                createQsAggregationExpression("SUM", "INTERACTION.numCalls", "SUM_num_calls"))
            .setFilter(interactionQueryFilter)
            .addGroupBy(createAttributeExpression("INTERACTION.fromServiceId"))
            .addGroupBy(createAttributeExpression("INTERACTION.toBackendId"))
            .setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    when(queryServiceClient.executeQuery(any(RequestContext.class), eq(expectedQueryRequest)))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor(
                                "INTERACTION.fromServiceId",
                                "INTERACTION.toBackendId",
                                "SUM_num_calls"))
                        .addRow(
                            generateRowFor("test_service_1", "to_backend_test_service_1", "30.0"))
                        .build())
                .iterator());
  }

  private ResultSetMetadata generateResultSetMetadataFor(String... columnNames) {
    ResultSetMetadata.Builder builder = ResultSetMetadata.newBuilder();
    Arrays.stream(columnNames)
        .forEach(
            columnName ->
                builder.addColumnMetadata(
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
                        .build()));
    return builder.build();
  }

  private Row generateRowFor(String... columnValues) {
    Row.Builder rowBuilder = Row.newBuilder();
    Arrays.stream(columnValues)
        .forEach(
            columnValue ->
                rowBuilder.addColumn(
                    org.hypertrace.core.query.service.api.Value.newBuilder()
                        .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
                        .setString(columnValue)));
    return rowBuilder.build();
  }

  private InteractionsRequest buildIncomingInteractionWithFilterRequest() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE");

    Filter entityTypeFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(buildAttributeExpression("INTERACTION.fromEntityType"))
                    .setOperator(Operator.IN)
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setValueType(ValueType.STRING_ARRAY)
                                            .addAllStringArray(entityTypes))
                                    .build())
                            .build()))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(buildAttributeExpression("INTERACTION.otherAttribute"))
                    .setOperator(Operator.IN)
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setValueType(ValueType.STRING_ARRAY)
                                            .addAllStringArray(List.of("attributeId")))
                                    .build())
                            .build()))
            .build();
    InteractionsRequest fromInteraction =
        InteractionsRequest.newBuilder()
            .setFilter(entityTypeFilter)
            .addSelection(buildAttributeExpression("INTERACTION.fromEntityType", "fromEntityType"))
            .addSelection(buildAttributeExpression("INTERACTION.fromEntityId", "fromEntityId"))
            .addSelection(
                getAggregateFunctionExpression(
                    "INTERACTION.numCalls", FunctionType.SUM, "SUM_num_calls"))
            .build();

    return fromInteraction;
  }

  private InteractionsRequest buildIncomingInteractionRequest() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE");

    Filter.Builder entityTypeFilter =
        Filter.newBuilder()
            .setLhs(buildAttributeExpression("INTERACTION.fromEntityType"))
            .setOperator(Operator.IN)
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING_ARRAY)
                                    .addAllStringArray(entityTypes))));
    InteractionsRequest fromInteraction =
        InteractionsRequest.newBuilder()
            .setFilter(entityTypeFilter)
            .addSelection(buildAttributeExpression("INTERACTION.fromEntityType", "fromEntityType"))
            .addSelection(buildAttributeExpression("INTERACTION.fromEntityId", "fromEntityId"))
            .addSelection(
                getAggregateFunctionExpression(
                    "INTERACTION.numCalls", FunctionType.SUM, "SUM_num_calls"))
            .build();

    return fromInteraction;
  }

  private InteractionsRequest buildOutgoingInteractionRequest() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE", "BACKEND");

    Filter.Builder entityTypeFilter =
        Filter.newBuilder()
            .setLhs(buildAttributeExpression("INTERACTION.toEntityType"))
            .setOperator(Operator.IN)
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING_ARRAY)
                                    .addAllStringArray(entityTypes))));
    InteractionsRequest toInteraction =
        InteractionsRequest.newBuilder()
            .setFilter(entityTypeFilter)
            .addSelection(buildAttributeExpression("INTERACTION.toEntityType", "toEntityType"))
            .addSelection(buildAttributeExpression("INTERACTION.toEntityId", "toEntityId"))
            .addSelection(
                getAggregateFunctionExpression(
                    "INTERACTION.numCalls", FunctionType.SUM, "SUM_num_calls"))
            .build();

    return toInteraction;
  }

  @Test
  public void testGetEntitiesForMultipleTypeInteractionQuery() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - TimeUnit.DAYS.toMillis(30);

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildAttributeExpression("SERVICE.id"))
            .setIncomingInteractions(buildIncomingInteractionRequest())
            .setOutgoingInteractions(buildOutgoingInteractionRequest())
            .build();

    mockQueryServiceRequestForServiceCount(startTime, endTime);
    mockQueryServiceRequestForIncomingServiceInteraction(startTime, endTime);
    mockQueryServiceRequestForOutgoingServiceInteraction(startTime, endTime);
    mockQueryServiceRequestForOutgoingBackendInteraction(startTime, endTime);

    EntityService entityService =
        new EntityService(
            queryServiceClient,
            entityQueryServiceClient,
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            scopeFilterConfigs,
            logConfig,
            queryExecutor);
    EntitiesResponse response =
        entityService.getEntities(new RequestContext(forTenantId(TENANT_ID)), request);

    // validate we have one incoming edge, and two outgoing edge
    assertNotNull(response);
    assertEquals(1, response.getTotal());
    assertEquals(1, response.getEntity(0).getIncomingInteractionCount());
    assertEquals(2, response.getEntity(0).getOutgoingInteractionCount());

    // validate incoming edge
    EntityInteraction incomingInteraction = response.getEntity(0).getIncomingInteraction(0);
    assertEquals(
        "from_test_service_1",
        incomingInteraction.getAttributeMap().get("fromEntityId").getString());
    assertEquals(
        "SERVICE", incomingInteraction.getAttributeMap().get("fromEntityType").getString());
    assertEquals(40, incomingInteraction.getMetricsMap().get("SUM_num_calls").getValue().getLong());

    // validate outgoing service edge
    EntityInteraction outGoingServiceInteraction =
        response.getEntity(0).getOutgoingInteractionList().stream()
            .filter(
                i ->
                    i.getAttributeMap().containsKey("toEntityType")
                        && i.getAttributeMap().get("toEntityType").getString().equals("SERVICE"))
            .findFirst()
            .orElse(null);
    assertNotNull(outGoingServiceInteraction);
    assertEquals(
        "to_test_service_1",
        outGoingServiceInteraction.getAttributeMap().get("toEntityId").getString());
    assertEquals(
        20, outGoingServiceInteraction.getMetricsMap().get("SUM_num_calls").getValue().getLong());

    // validate outgoing backend edge
    EntityInteraction outGoingBackendInteraction =
        response.getEntity(0).getOutgoingInteractionList().stream()
            .filter(
                i ->
                    i.getAttributeMap().containsKey("toEntityType")
                        && i.getAttributeMap().get("toEntityType").getString().equals("BACKEND"))
            .findFirst()
            .orElse(null);
    assertNotNull(outGoingBackendInteraction);
    assertEquals(
        "to_backend_test_service_1",
        outGoingBackendInteraction.getAttributeMap().get("toEntityId").getString());
    assertEquals(
        30, outGoingBackendInteraction.getMetricsMap().get("SUM_num_calls").getValue().getLong());
  }

  @Test
  public void testGetEntitiesWithInteractionFilters() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - TimeUnit.DAYS.toMillis(30);

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildAttributeExpression("SERVICE.id"))
            .setIncomingInteractions(buildIncomingInteractionWithFilterRequest())
            .build();

    mockQueryServiceRequestForServiceCountWithInteractionFilters(startTime, endTime);
    mockQueryServiceRequestForIncomingServiceWithInteractionFilters(startTime, endTime);
    mockQueryServiceIdsRequestForInteractionFilterRequest(startTime, endTime);

    EntityService entityService =
        new EntityService(
            queryServiceClient,
            entityQueryServiceClient,
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            scopeFilterConfigs,
            logConfig,
            queryExecutor);
    EntitiesResponse response =
        entityService.getEntities(new RequestContext(forTenantId(TENANT_ID)), request);

    // validate we have one incoming edge, and two outgoing edge
    assertNotNull(response);
    assertEquals(1, response.getTotal());
    assertEquals(1, response.getEntity(0).getIncomingInteractionCount());

    // validate incoming edge
    EntityInteraction incomingInteraction = response.getEntity(0).getIncomingInteraction(0);
    assertEquals(
        "from_test_service_1",
        incomingInteraction.getAttributeMap().get("fromEntityId").getString());
    assertEquals(
        "SERVICE", incomingInteraction.getAttributeMap().get("fromEntityType").getString());
    assertEquals(40, incomingInteraction.getMetricsMap().get("SUM_num_calls").getValue().getLong());
  }
}
