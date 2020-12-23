package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createBetweenTimesFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.util.QueryRequestUtil;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.Builder;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

/** Unit tests for {@link EntityInteractionsFetcher} */
public class EntityInteractionsFetcherTest extends AbstractGatewayServiceTest {
  private final InteractionsRequest fromServiceInteractions =
      InteractionsRequest.newBuilder()
          .setFilter(getSimpleFilter("INTERACTION.fromEntityType", "SERVICE"))
          .addSelection(
              getAggregateFunctionExpression(
                  "INTERACTION.bytesReceived", FunctionType.SUM, "SUM_bytes_received"))
          .build();

  private final InteractionsRequest toServiceInteractions =
      InteractionsRequest.newBuilder()
          .setFilter(getSimpleFilter("INTERACTION.toEntityType", "SERVICE"))
          .addSelection(
              getAggregateFunctionExpression(
                  "INTERACTION.bytesSent", FunctionType.SUM, "SUM_bytes_sent"))
          .addSelection(
              QueryExpressionUtil.getAggregateFunctionExpression(
                  "INTERACTION.bytesSent",
                  FunctionType.AVGRATE,
                  "AVGRATE_bytes_sent_60",
                  ImmutableList.of(
                      QueryExpressionUtil.getLiteralExpression(TimeUnit.MINUTES.toSeconds(1))
                          .build()),
                  false))
          .addSelection(
              QueryExpressionUtil.getAggregateFunctionExpression(
                  "INTERACTION.bytesSent",
                  FunctionType.PERCENTILE,
                  "p99_bytes_sent",
                  ImmutableList.of(
                      QueryExpressionUtil.getLiteralExpression(Long.valueOf(99)).build()),
                  false))
          .build();

  private final InteractionsRequest toApiInteractions =
      InteractionsRequest.newBuilder()
          .setFilter(getSimpleFilter("Interaction.attributes.to_entity_type", "API"))
          .addSelection(
              getAggregateFunctionExpression(
                  "Interaction.metrics.bytes_sent", FunctionType.SUM, "SUM_bytes_sent"))
          .build();

  @Mock private AttributeMetadataProvider attributeMetadataProvider;

  private Builder getColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
  }

  private Expression.Builder getLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)));
  }

  private Expression.Builder getAggregateFunctionExpression(
      String columnName, FunctionType function, String alias) {
    return Expression.newBuilder()
        .setFunction(
            FunctionExpression.newBuilder()
                .setFunction(function)
                .setAlias(alias)
                .addArguments(getColumnExpression(columnName)));
  }

  private Filter.Builder getSimpleFilter(String columnName, String value) {
    return Filter.newBuilder()
        .setLhs(getColumnExpression(columnName))
        .setOperator(Operator.EQ)
        .setRhs(getLiteralExpression(value));
  }

  @Test
  public void testServiceToServiceEdgeQueryRequests() {
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
            .setEndTimeMillis(System.currentTimeMillis())
            .addSelection(getColumnExpression("SERVICE.name"))
            .setIncomingInteractions(fromServiceInteractions)
            .setOutgoingInteractions(toServiceInteractions)
            .build();

    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(), Mockito.eq(AttributeScope.INTERACTION.name()), Mockito.eq("startTime")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("dummy").build()));

    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);
    Map<String, QueryRequest> queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getIncomingInteractions(),
            Collections.singleton(EntityKey.from("test_id1")),
            true,
            null);
    assertEquals(1, queryRequests.size());
    QueryRequest queryRequest = queryRequests.get("SERVICE");
    assertNotNull(queryRequest);
    assertEquals(2, queryRequest.getGroupByCount());
    assertEquals(fromServiceInteractions.getSelectionCount() + 2, queryRequest.getSelectionCount());
    System.out.println(queryRequest);

    queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getOutgoingInteractions(),
            Collections.singleton(EntityKey.from("test_id1")),
            false,
            null);
    assertEquals(1, queryRequests.size());
    queryRequest = queryRequests.get("SERVICE");
    assertNotNull(queryRequest);
    assertEquals(2, queryRequest.getGroupByCount());
    assertEquals(toServiceInteractions.getSelectionCount() + 2, queryRequest.getSelectionCount());
    System.out.println(queryRequest);
  }

  @Test
  public void testFromMultipleEntitiesQuery() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE", "API");
    Set<String> entityIds = ImmutableSet.of("service_id_1", "api_id_1");

    Filter.Builder entityTypeFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("INTERACTION.fromEntityType")))
            .setOperator(Operator.IN)
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING_ARRAY)
                                    .addAllStringArray(entityTypes))));
    Filter.Builder entityIdFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("INTERACTION.fromEntityId")))
            .setOperator(Operator.IN)
            .setRhs(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setValueType(ValueType.STRING_ARRAY)
                                    .addAllStringArray(entityIds))));
    InteractionsRequest fromInteraction =
        InteractionsRequest.newBuilder()
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(entityTypeFilter)
                    .addChildFilter(entityIdFilter))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "INTERACTION.bytesReceived", FunctionType.SUM, "SUM_bytes_received"))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "INTERACTION.bytesReceived",
                    FunctionType.AVGRATE,
                    "AVGRATE_bytes_received_60",
                    ImmutableList.of(
                        QueryExpressionUtil.getLiteralExpression(TimeUnit.MINUTES.toSeconds(1))
                            .build()),
                    false))
            .addSelection(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "INTERACTION.bytesReceived",
                    FunctionType.PERCENTILE,
                    "p99_bytes_received",
                    ImmutableList.of(
                        QueryExpressionUtil.getLiteralExpression(Long.valueOf(99)).build()),
                    false))
            .build();
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
            .setEndTimeMillis(System.currentTimeMillis())
            .addSelection(getColumnExpression("SERVICE.name"))
            .setIncomingInteractions(fromInteraction)
            .build();

    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(), Mockito.eq(AttributeScope.INTERACTION.name()), Mockito.eq("startTime")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("dummy").build()));

    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);
    Map<String, QueryRequest> queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getIncomingInteractions(),
            Collections.singleton(EntityKey.from("test_id1")),
            true,
            null);
    assertEquals(entityTypes.size(), queryRequests.size());

    for (QueryRequest queryRequest : queryRequests.values()) {
      assertNotNull(queryRequest);
      assertEquals(2, queryRequest.getGroupByCount());
      assertEquals(fromInteraction.getSelectionCount() + 2, queryRequest.getSelectionCount());
      System.out.println(queryRequest);
    }
  }

  @Test
  public void testToMultipleEntityTypesQuery() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE", "BACKEND");
    Set<String> entityIds = ImmutableSet.of("service_id_1");

    Filter.Builder entityTypeFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("INTERACTION.toEntityType")))
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
            .addSelection(
                getAggregateFunctionExpression(
                    "INTERACTION.bytesReceived", FunctionType.SUM, "SUM_bytes_received"))
            .build();
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
            .setEndTimeMillis(System.currentTimeMillis())
            .addSelection(getColumnExpression("SERVICE.name"))
            .addSelection(getColumnExpression("INTERACTION.fromEntityType"))
            .addSelection(getColumnExpression("INTERACTION.toEntityType"))
            .setOutgoingInteractions(toInteraction)
            .build();

    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(), Mockito.eq(AttributeScope.INTERACTION.name()), Mockito.eq("startTime")))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setId("INTERACTION.startTime").build()));

    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);
    Map<String, QueryRequest> queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getOutgoingInteractions(),
            Collections.singleton(EntityKey.from("test_id1")),
            false,
            null);
    assertEquals(entityTypes.size(), queryRequests.size());

    for (QueryRequest queryRequest : queryRequests.values()) {
      assertNotNull(queryRequest);
      assertEquals(2, queryRequest.getGroupByCount());
      assertEquals(toInteraction.getSelectionCount() + 2, queryRequest.getSelectionCount());
      System.out.println(queryRequest);
    }

    // Fully assert the QueryService requests.

    // One request should query for Service -> Service edges.
    org.hypertrace.core.query.service.api.Filter filter = queryRequests.get("SERVICE").getFilter();
    org.hypertrace.core.query.service.api.Filter expectedFilter =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
            .addChildFilter(
                createBetweenTimesFilter(
                    "INTERACTION.startTime",
                    request.getStartTimeMillis(),
                    request.getEndTimeMillis()))
            .addChildFilter(
                createStringArrayFilter(
                    org.hypertrace.core.query.service.api.Operator.IN,
                    "INTERACTION.fromServiceId",
                    List.of("test_id1")))
            .addChildFilter(
                org.hypertrace.core.query.service.api.Filter.newBuilder()
                    .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
                    .addChildFilter(
                        createFilter(
                            "INTERACTION.toServiceId",
                            org.hypertrace.core.query.service.api.Operator.NEQ,
                            createStringNullLiteralExpression())))
            .build();
    assertEquals(expectedFilter, filter);

    // The other request should query for Service -> Backend edges.
    filter = queryRequests.get("BACKEND").getFilter();
    expectedFilter =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
            .addChildFilter(
                createBetweenTimesFilter(
                    "INTERACTION.startTime",
                    request.getStartTimeMillis(),
                    request.getEndTimeMillis()))
            .addChildFilter(
                createStringArrayFilter(
                    org.hypertrace.core.query.service.api.Operator.IN,
                    "INTERACTION.fromServiceId",
                    List.of("test_id1")))
            .addChildFilter(
                org.hypertrace.core.query.service.api.Filter.newBuilder()
                    .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
                    .addChildFilter(
                        createFilter(
                            "INTERACTION.toBackendId",
                            org.hypertrace.core.query.service.api.Operator.NEQ,
                            createStringNullLiteralExpression())))
            .build();
    assertEquals(expectedFilter, filter);
  }

  @Test
  public void testEntityWithInteractionMappingToMultipleAttributes() {
    Set<String> entityTypes = ImmutableSet.of("SERVICE");
    List<String> entityIdInteractionMappings = List.of("INTERACTION.fromNamespaceName", "INTERACTION.fromNamespaceType");

    Filter.Builder entityTypeFilter =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("INTERACTION.toEntityType")))
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
            .addSelection(
                getAggregateFunctionExpression(
                    "INTERACTION.bytesReceived", FunctionType.SUM, "SUM_bytes_received"))
            .build();
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.NAMESPACE.name())
            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
            .setEndTimeMillis(System.currentTimeMillis())
            .setOutgoingInteractions(toInteraction)
            .build();

    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
        attributeMetadataProvider.getAttributeMetadata(
            any(), Mockito.eq(AttributeScope.INTERACTION.name()), Mockito.eq("startTime")))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setId("INTERACTION.startTime").build()));

    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);
    LinkedHashSet<EntityKey> entityKeys = new LinkedHashSet<>();
    entityKeys.add(EntityKey.of("test_name1", "test_type1"));
    entityKeys.add(EntityKey.of("test_name2", "test_type2"));
    Map<String, QueryRequest> queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getOutgoingInteractions(),
            entityKeys,
            false,
            null);
    assertEquals(entityTypes.size(), queryRequests.size());

    // Should select and group on INTERACTION.toServiceId, INTERACTION.fromNamespaceName, INTERACTION.fromNamespaceType
    for (QueryRequest queryRequest : queryRequests.values()) {
      assertNotNull(queryRequest);
      assertEquals(3, queryRequest.getGroupByCount());
      assertEquals(toInteraction.getSelectionCount() + 3, queryRequest.getSelectionCount());
      System.out.println(queryRequest);
    }

    // Fully assert the QueryService requests.

    // The one request should query for Namespace -> Service edges.
    org.hypertrace.core.query.service.api.Filter filter = queryRequests.get("SERVICE").getFilter();
    org.hypertrace.core.query.service.api.Filter expectedFilter =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
            .addChildFilter(
                createBetweenTimesFilter(
                    "INTERACTION.startTime",
                    request.getStartTimeMillis(),
                    request.getEndTimeMillis()))
            .addChildFilter(
                org.hypertrace.core.query.service.api.Filter.newBuilder()
                    .setOperator(org.hypertrace.core.query.service.api.Operator.OR)
                    .addChildFilter(
                        QueryRequestUtil.createValueEQFilter(
                            entityIdInteractionMappings, List.of("test_name1", "test_type1")))
                    .addChildFilter(
                        QueryRequestUtil.createValueEQFilter(
                            entityIdInteractionMappings, List.of("test_name2", "test_type2"))))
            .addChildFilter(
                org.hypertrace.core.query.service.api.Filter.newBuilder()
                    .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
                    .addChildFilter(
                        createFilter(
                            "INTERACTION.toServiceId",
                            org.hypertrace.core.query.service.api.Operator.NEQ,
                            createStringNullLiteralExpression())))
            .build();
    assertEquals(expectedFilter, filter);
  }

  private org.hypertrace.core.query.service.api.Filter createStringArrayFilter(
      org.hypertrace.core.query.service.api.Operator operator, String columnName, List<String> valueList) {
    return org.hypertrace.core.query.service.api.Filter.newBuilder()
        .setOperator(operator)
        .setLhs(
            org.hypertrace.core.query.service.api.Expression.newBuilder()
                .setColumnIdentifier(
                    org.hypertrace.core.query.service.api.ColumnIdentifier.newBuilder()
                        .setColumnName(columnName)
                )
        )
        .setRhs(
            org.hypertrace.core.query.service.api.Expression.newBuilder()
                .setLiteral(
                    org.hypertrace.core.query.service.api.LiteralConstant.newBuilder()
                        .setValue(
                            org.hypertrace.core.query.service.api.Value.newBuilder()
                                .addAllStringArray(valueList)
                                .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_ARRAY)
                        )
                )
        ).build();
  }

  private EntitiesRequest.Builder getValidServiceEntitiesRequest() {
    return EntitiesRequest.newBuilder()
        .setEntityType(DomainEntityType.SERVICE.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(getColumnExpression("Service.name"));
  }

  private EntitiesRequest.Builder getValidBackendEntitiesRequest() {
    return EntitiesRequest.newBuilder()
        .setEntityType(DomainEntityType.BACKEND.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(getColumnExpression("Backend.name"));
  }

  @Test
  public void testCornerCases() {
    // No selections
    InteractionsRequest interactionsRequest =
        InteractionsRequest.newBuilder()
            .setFilter(getSimpleFilter("INTERACTION.fromEntityType", "SERVICE"))
            .build();

    EntitiesRequest request =
        getValidServiceEntitiesRequest().setIncomingInteractions(interactionsRequest).build();
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(), Mockito.eq(AttributeScope.INTERACTION.name()), Mockito.eq("startTime")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("dummy").build()));

    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);
    Map<String, QueryRequest> queryRequests =
        aggregator.buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            request.getIncomingInteractions(),
            Collections.singleton(EntityKey.from("test_id1")),
            true,
            null);
    assertEquals(1, queryRequests.size());

    for (QueryRequest queryRequest : queryRequests.values()) {
      assertNotNull(queryRequest);
      assertEquals(2, queryRequest.getGroupByCount());

      // The selection will have two entity ids and COUNT(*)
      assertEquals(3, queryRequest.getSelectionCount());
      System.out.println(queryRequest);
    }
  }

  private Collection<EntitiesRequest> getInvalidRequests() {
    return ImmutableSet.of(
        getValidServiceEntitiesRequest()
            .setIncomingInteractions(
                // Empty request with only limit set
                InteractionsRequest.newBuilder().setLimit(1).build())
            .build(),
        getValidServiceEntitiesRequest()
            .setIncomingInteractions(
                // No filter but selections available.
                InteractionsRequest.newBuilder().addSelection(getColumnExpression("test")))
            .build(),
        getValidServiceEntitiesRequest()
            .setOutgoingInteractions(
                // No filter but filter available
                InteractionsRequest.newBuilder()
                    .setFilter(getSimpleFilter("test", "value"))
                    .build())
            .build(),
        // Backend to service interactions
        getValidBackendEntitiesRequest().setOutgoingInteractions(toServiceInteractions).build());
  }

  @Test
  public void testInvalidRequests() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(EntitiesRequestContext.class),
                Mockito.eq(AttributeScope.API.name()),
                Mockito.eq("apiId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setFqn("API.apiId").build()));
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(EntitiesRequestContext.class),
                Mockito.eq(AttributeScope.SERVICE.name()),
                Mockito.eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setFqn("Service.id").build()));
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(EntitiesRequestContext.class),
                Mockito.eq(AttributeScope.BACKEND.name()),
                Mockito.eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setFqn("Backend.id").build()));
    Mockito.when(
            attributeMetadataProvider.getAttributeMetadata(
                any(RequestContext.class),
                Mockito.eq(AttributeScope.INTERACTION.name()),
                Mockito.eq("startTime")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setFqn("dummy").build()));
    EntityInteractionsFetcher aggregator =
        new EntityInteractionsFetcher(null, 500, attributeMetadataProvider);

    for (EntitiesRequest request : getInvalidRequests()) {
      try {
        aggregator.populateEntityInteractions(
            new RequestContext(TENANT_ID, new HashMap<>()), request, Collections.emptyMap());
        fail("Expected the request to fail. Request: " + request);
      } catch (IllegalArgumentException ignore) {
        // Ignore.
      }
    }
  }
}
