package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringArrayLiteralExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.InteractionConfig;
import org.hypertrace.gateway.service.entity.config.InteractionConfigs;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.hypertrace.gateway.service.v1.entity.EntityInteraction;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Business logic to get entity interactions data and aggregate it as per the requests coming into
 * the EntityGateway. As much as possible, this class should be agnostic to the entity type so that
 * all interactions can be modeled similarly in a generic fashion.
 */
public class EntityInteractionsFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(EntityInteractionsFetcher.class);

  // Extracting the incoming/outgoing flag as constant for readability.
  private static final boolean INCOMING = true;
  private static final boolean OUTGOING = false;

  private static final String SCOPE = AttributeScope.INTERACTION.name();
  private static final String FROM_SPACE_ATTRIBUTE_KEY = "fromSpaceIds";
  private static final String TO_SPACE_ATTRIBUTE_KEY = "toSpaceIds";
  // TODO reference by key instead of ID
  private static final String FROM_ENTITY_TYPE_ATTRIBUTE_ID = "INTERACTION.fromEntityType";
  private static final String TO_ENTITY_TYPE_ATTRIBUTE_ID = "INTERACTION.toEntityType";
  private static final String FROM_ENTITY_ID_ATTRIBUTE_ID = "INTERACTION.fromEntityId";
  private static final String TO_ENTITY_ID_ATTRIBUTE_ID = "INTERACTION.toEntityId";
  private static final Set<String> SELECTIONS_TO_IGNORE =
      ImmutableSet.of(
          FROM_ENTITY_ID_ATTRIBUTE_ID,
          FROM_ENTITY_TYPE_ATTRIBUTE_ID,
          TO_ENTITY_ID_ATTRIBUTE_ID,
          TO_ENTITY_TYPE_ATTRIBUTE_ID);

  private static final String COUNT_COLUMN_NAME = "COUNT";

  private final QueryServiceClient queryServiceClient;
  private final AttributeMetadataProvider metadataProvider;
  private final ExecutorService queryExecutor;

  public EntityInteractionsFetcher(
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider metadataProvider,
      ExecutorService queryExecutor) {
    this.queryServiceClient = queryServiceClient;
    this.metadataProvider = metadataProvider;
    this.queryExecutor = queryExecutor;
  }

  private List<String> getEntityIdColumnsFromInteraction(
      DomainEntityType entityType, boolean incoming) {
    InteractionConfig interactionConfig =
        InteractionConfigs.getInteractionAttributeConfig(entityType.name());
    if (interactionConfig == null) {
      throw new IllegalArgumentException("Unhandled entityType: " + entityType);
    }
    List<String> columnNames =
        incoming
            ? interactionConfig.getCallerSideAttributeIds()
            : interactionConfig.getCalleeSideAttributeIds();
    if (columnNames.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid arguments for getting interaction columns. entityType:%s, incoming:%s",
              entityType, incoming));
    }
    return columnNames;
  }

  private boolean hasInteractionFilters(org.hypertrace.gateway.service.v1.common.Filter filter) {
    if (ExpressionReader.isSimpleAttributeSelection(filter.getLhs())) {
      String attributeId =
          ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs()).orElseThrow();

      return !StringUtils.equals(attributeId, FROM_ENTITY_TYPE_ATTRIBUTE_ID)
          && !StringUtils.equals(attributeId, TO_ENTITY_TYPE_ATTRIBUTE_ID);
    }

    boolean hasFilters = false;
    if (filter.getChildFilterCount() > 0) {
      for (org.hypertrace.gateway.service.v1.common.Filter child : filter.getChildFilterList()) {
        hasFilters = hasFilters || hasInteractionFilters(child);
      }
    }
    return hasFilters;
  }

  public Set<EntityKey> fetchInteractionsIdsIfNecessary(
      RequestContext context, EntitiesRequest entitiesRequest) {

    if (!hasInteractionFilters(entitiesRequest.getIncomingInteractions().getFilter())
        && !hasInteractionFilters(entitiesRequest.getOutgoingInteractions().getFilter())) {
      return Collections.emptySet();
    }

    List<EntityInteractionQueryRequest> allQueryRequests = new ArrayList<>();
    if (!InteractionsRequest.getDefaultInstance()
        .equals(entitiesRequest.getIncomingInteractions())) {
      allQueryRequests.addAll(
          prepareInteractionIdsQueryRequest(
              context, entitiesRequest, entitiesRequest.getIncomingInteractions(), INCOMING));
    }

    // Process the outgoing interactions, and prepare the QS queries
    if (!InteractionsRequest.getDefaultInstance()
        .equals(entitiesRequest.getOutgoingInteractions())) {
      allQueryRequests.addAll(
          prepareInteractionIdsQueryRequest(
              context, entitiesRequest, entitiesRequest.getOutgoingInteractions(), OUTGOING));
    }

    // execute all the requests in parallel, and wait for results
    List<CompletableFuture<EntityInteractionQueryResponse>> queryRequestCompletableFutures =
        allQueryRequests.stream()
            .map(
                e ->
                    CompletableFuture.supplyAsync(
                        () -> executeQueryRequest(context, e), this.queryExecutor))
            .collect(Collectors.toUnmodifiableList());

    Set<EntityKey> entityKeys = new HashSet<>();
    queryRequestCompletableFutures.forEach(
        queryRequestCompletableFuture -> {
          EntityInteractionQueryResponse qsResponse = queryRequestCompletableFuture.join();
          entityKeys.addAll(parseInteractionIdsResponse(entitiesRequest, qsResponse));
        });

    return Collections.unmodifiableSet(entityKeys);
  }

  private Set<EntityKey> parseInteractionIdsResponse(
      EntitiesRequest entitiesRequest, EntityInteractionQueryResponse qsResponse) {
    Set<EntityKey> entityKeys = new HashSet<>();
    qsResponse
        .getResultSetChunkIterator()
        .forEachRemaining(
            resultSetChunk -> {
              if (resultSetChunk.getRowCount() < 1) {
                return;
              }
              resultSetChunk
                  .getRowList()
                  .forEach(
                      row -> {
                        List<String> idColumns =
                            getEntityIdColumnsFromInteraction(
                                DomainEntityType.valueOf(entitiesRequest.getEntityType()),
                                !qsResponse.getRequest().isIncoming());
                        EntityKey key =
                            EntityKey.of(
                                IntStream.range(0, idColumns.size())
                                    .mapToObj(value -> row.getColumn(value).getString())
                                    .toArray(String[]::new));
                        entityKeys.add(key);
                      });
            });
    return Collections.unmodifiableSet(entityKeys);
  }

  public void populateEntityInteractions(
      RequestContext context,
      EntitiesRequest entitiesRequest,
      Map<EntityKey, Builder> entityBuilders) {
    List<EntityInteractionQueryRequest> allQueryRequests = new ArrayList<>();
    // Process the incoming interactions, and prepare QS queries
    if (!InteractionsRequest.getDefaultInstance()
        .equals(entitiesRequest.getIncomingInteractions())) {
      allQueryRequests.addAll(
          prepareQueryRequests(
              context,
              entitiesRequest,
              entityBuilders,
              entitiesRequest.getIncomingInteractions(),
              INCOMING,
              "fromEntityType filter is mandatory for incoming interactions."));
    }

    // Process the outgoing interactions, and prepare the QS queries
    if (!InteractionsRequest.getDefaultInstance()
        .equals(entitiesRequest.getOutgoingInteractions())) {
      allQueryRequests.addAll(
          prepareQueryRequests(
              context,
              entitiesRequest,
              entityBuilders,
              entitiesRequest.getOutgoingInteractions(),
              OUTGOING,
              "toEntityType filter is mandatory for outgoing interactions."));
    }

    // execute all the requests in parallel, and wait for results
    List<CompletableFuture<EntityInteractionQueryResponse>> queryRequestCompletableFutures =
        allQueryRequests.stream()
            .map(
                e ->
                    CompletableFuture.supplyAsync(
                        () -> executeQueryRequest(context, e), this.queryExecutor))
            .collect(Collectors.toList());

    // wait and parse result as an when complete
    queryRequestCompletableFutures.forEach(
        queryRequestCompletableFuture -> {
          EntityInteractionQueryResponse qsResponse = queryRequestCompletableFuture.join();
          InteractionsRequest interactionsRequest =
              qsResponse.getRequest().getInteractionsRequest();

          Map<String, FunctionExpression> metricToAggFunction =
              MetricAggregationFunctionUtil.getAggMetricToFunction(
                  interactionsRequest.getSelectionList());

          parseResultSet(
              entitiesRequest.getEntityType(),
              qsResponse.getRequest().getEntityType(),
              interactionsRequest.getSelectionList(),
              metricToAggFunction,
              qsResponse.getResultSetChunkIterator(),
              qsResponse.getRequest().isIncoming(),
              entityBuilders,
              context);
        });
  }

  private EntityInteractionQueryResponse executeQueryRequest(
      RequestContext context, EntityInteractionQueryRequest entityInteractionQueryRequest) {
    Iterator<ResultSetChunk> resultSet =
        queryServiceClient.executeQuery(context, entityInteractionQueryRequest.getRequest());
    return new EntityInteractionQueryResponse(entityInteractionQueryRequest, resultSet);
  }

  private List<EntityInteractionQueryRequest> prepareInteractionIdsQueryRequest(
      RequestContext requestContext,
      EntitiesRequest request,
      InteractionsRequest interactionsRequest,
      boolean incoming) {
    Set<String> otherEntityTypes = getOtherEntityTypes(interactionsRequest.getFilter());
    if (otherEntityTypes.isEmpty()) {
      return Collections.emptyList();
    }

    List<Filter> childFilters = new ArrayList<>();
    childFilters.add(
        QueryRequestUtil.createBetweenTimesFilter(
            AttributeMetadataUtil.getTimestampAttributeId(metadataProvider, requestContext, SCOPE),
            request.getStartTimeMillis(),
            request.getEndTimeMillis()));

    this.buildSpaceQueryFilterIfNeeded(requestContext, request.getSpaceId())
        .ifPresent(childFilters::add);

    List<String> idColumns =
        getEntityIdColumnsFromInteraction(
            DomainEntityType.valueOf(request.getEntityType()), !incoming);

    // Group by the entity id column first, then the other end entity type for the interaction.
    List<org.hypertrace.core.query.service.api.Expression> idExpressions =
        idColumns.stream()
            .map(QueryRequestUtil::createAttributeExpression)
            .collect(Collectors.toUnmodifiableList());

    // adding empty selections
    List<org.hypertrace.core.query.service.api.Expression> selections = new ArrayList<>();
    selections.add(
        QueryRequestUtil.createCountByColumnSelection(
            Optional.ofNullable(idColumns.get(0)).orElseThrow()));

    Map<String, QueryRequest> queryRequests = new HashMap<>();
    for (String e : otherEntityTypes) {
      DomainEntityType otherEntityType = DomainEntityType.valueOf(e.toUpperCase());

      // Get the filters from the interactions request to 'AND' them with the timestamp
      // defaultFilter.
      Filter.Builder filterBuilder = Filter.newBuilder().addAllChildFilter(childFilters);
      filterBuilder.addChildFilter(
          convertToQueryFilter(interactionsRequest.getFilter(), otherEntityType));

      QueryRequest.Builder queryBuilder = QueryRequest.newBuilder();
      queryBuilder.setFilter(filterBuilder);
      queryBuilder.addAllGroupBy(idExpressions);

      // Add all selections in the correct order. First id, then other entity id and finally
      // the remaining selections.
      queryBuilder.addAllSelection(idExpressions);
      selections.forEach(queryBuilder::addSelection);
      queryBuilder.setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
      queryRequests.put(e, queryBuilder.build());
    }

    return queryRequests.entrySet().stream()
        .map(
            e ->
                new EntityInteractionQueryRequest(
                    incoming, e.getKey(), interactionsRequest, e.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }

  private List<EntityInteractionQueryRequest> prepareQueryRequests(
      RequestContext context,
      EntitiesRequest request,
      Map<EntityKey, Builder> entityIdToBuilders,
      InteractionsRequest interactionsRequest,
      boolean incoming,
      String errorMsg) {

    if (!interactionsRequest.hasFilter()) {
      throw new IllegalArgumentException(errorMsg);
    }
    if (interactionsRequest.getSelectionCount() == 0) {
      throw new IllegalArgumentException("Interactions request should have non-empty selections.");
    }

    Map<String, QueryRequest> requests =
        buildQueryRequests(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            request.getEntityType(),
            interactionsRequest,
            entityIdToBuilders.keySet(),
            incoming,
            context);

    if (requests.isEmpty()) {
      throw new IllegalArgumentException(errorMsg);
    }

    return requests.entrySet().stream()
        .map(
            e ->
                new EntityInteractionQueryRequest(
                    incoming, e.getKey(), interactionsRequest, e.getValue()))
        .collect(Collectors.toList());
  }

  private Set<String> getOtherEntityTypes(org.hypertrace.gateway.service.v1.common.Filter filter) {
    if (filter.getChildFilterCount() > 0) {
      for (org.hypertrace.gateway.service.v1.common.Filter child : filter.getChildFilterList()) {
        Set<String> result = getOtherEntityTypes(child);
        if (!result.isEmpty()) {
          return result;
        }
      }
    } else if (ExpressionReader.isSimpleAttributeSelection(filter.getLhs())) {
      String attributeId =
          ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs()).orElseThrow();

      if (StringUtils.equals(attributeId, FROM_ENTITY_TYPE_ATTRIBUTE_ID)
          || StringUtils.equals(attributeId, TO_ENTITY_TYPE_ATTRIBUTE_ID)) {
        return getValues(filter.getRhs());
      }
    }

    return Collections.emptySet();
  }

  private Filter convertToQueryFilter(
      org.hypertrace.gateway.service.v1.common.Filter filter, DomainEntityType otherEntityType) {
    Filter.Builder builder = Filter.newBuilder();
    builder.setOperator(QueryAndGatewayDtoConverter.convertOperator(filter.getOperator()));
    if (filter.getChildFilterCount() > 0) {
      for (org.hypertrace.gateway.service.v1.common.Filter child : filter.getChildFilterList()) {
        builder.addChildFilter(convertToQueryFilter(child, otherEntityType));
      }
    } else {
      if (ExpressionReader.isSimpleAttributeSelection(filter.getLhs())) {
        String attributeId =
            ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs()).orElseThrow();

        switch (attributeId) {
          case FROM_ENTITY_TYPE_ATTRIBUTE_ID:
            return QueryRequestUtil.createCompositeFilter(
                Operator.AND,
                getEntityIdColumnsFromInteraction(otherEntityType, INCOMING).stream()
                    .map(
                        fromEntityIdColumn ->
                            createFilter(
                                fromEntityIdColumn,
                                Operator.NEQ,
                                createStringNullLiteralExpression()))
                    .collect(Collectors.toList()));
          case TO_ENTITY_TYPE_ATTRIBUTE_ID:
            return QueryRequestUtil.createCompositeFilter(
                Operator.AND,
                getEntityIdColumnsFromInteraction(otherEntityType, OUTGOING).stream()
                    .map(
                        fromEntityIdColumn ->
                            createFilter(
                                fromEntityIdColumn,
                                Operator.NEQ,
                                createStringNullLiteralExpression()))
                    .collect(Collectors.toList()));
          case FROM_ENTITY_ID_ATTRIBUTE_ID:
            return createFilterForEntityKeys(
                getEntityIdColumnsFromInteraction(otherEntityType, INCOMING),
                getEntityKeyValues(filter.getRhs()));
          case TO_ENTITY_ID_ATTRIBUTE_ID:
            return createFilterForEntityKeys(
                getEntityIdColumnsFromInteraction(otherEntityType, OUTGOING),
                getEntityKeyValues(filter.getRhs()));
          default:
            // Do nothing, fall through to default case
        }
      }

      // Default case.
      builder.setLhs(QueryAndGatewayDtoConverter.convertToQueryExpression(filter.getLhs()));
      builder.setRhs(QueryAndGatewayDtoConverter.convertToQueryExpression(filter.getRhs()));
    }

    return builder.build();
  }

  private Set<EntityKey> getEntityKeyValues(Expression expression) {
    Preconditions.checkArgument(expression.getValueCase() == ValueCase.LITERAL);

    Value value = expression.getLiteral().getValue();
    if (value.getValueType() == ValueType.STRING) {
      return Collections.singleton(EntityKey.from(value.getString()));
    }
    if (value.getValueType() == ValueType.STRING_ARRAY) {
      return value.getStringArrayList().stream().map(EntityKey::from).collect(Collectors.toSet());
    }
    throw new IllegalArgumentException(
        "Expected STRING value but received unhandled type: " + value.getValueType());
  }

  private Set<String> getValues(Expression expression) {
    Preconditions.checkArgument(expression.getValueCase() == ValueCase.LITERAL);

    Value value = expression.getLiteral().getValue();
    if (value.getValueType() == ValueType.STRING) {
      return Collections.singleton(value.getString());
    }
    if (value.getValueType() == ValueType.STRING_ARRAY) {
      return new HashSet<>(value.getStringArrayList());
    }
    throw new IllegalArgumentException(
        "Expected STRING value but received unhandled type: " + value.getValueType());
  }

  private Filter createFilterForEntityKeys(
      List<String> idColumns, Collection<EntityKey> entityKeys) {
    // if only 1 id column use an IN list
    if (idColumns.size() == 1) {
      return QueryRequestUtil.createFilter(
          idColumns.get(0),
          Operator.IN,
          createStringArrayLiteralExpression(
              entityKeys.stream().map(EntityKey::toString).collect(Collectors.toList())));
    }
    // TODO this shouldn't be reachable, remove concept of composite IDs separately
    // otherwise use an OR chain of ANDed EQ filters.
    return Filter.newBuilder()
        .setOperator(Operator.OR)
        .addAllChildFilter(
            entityKeys.stream()
                .map(
                    entityKey ->
                        org.hypertrace.core.query.service.util.QueryRequestUtil.createValueEQFilter(
                            idColumns, entityKey.getAttributes()))
                .collect(Collectors.toList()))
        .build();
  }

  @VisibleForTesting
  Map<String, QueryRequest> buildQueryRequests(
      long startTime,
      long endTime,
      String spaceId,
      String entityType,
      InteractionsRequest interactionsRequest,
      Set<EntityKey> entityIds,
      boolean incoming,
      RequestContext requestContext) {

    Set<String> entityTypes = getOtherEntityTypes(interactionsRequest.getFilter());
    if (entityTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    QueryRequest.Builder builder = QueryRequest.newBuilder();

    // Filter should include the timestamp filters from parent request first
    Filter.Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                QueryRequestUtil.createBetweenTimesFilter(
                    AttributeMetadataUtil.getTimestampAttributeId(
                        metadataProvider, requestContext, SCOPE),
                    startTime,
                    endTime));

    this.buildSpaceQueryFilterIfNeeded(requestContext, spaceId)
        .ifPresent(filterBuilder::addChildFilter);

    List<String> idColumns =
        getEntityIdColumnsFromInteraction(DomainEntityType.valueOf(entityType), !incoming);

    // Add a filter on the entityIds
    filterBuilder.addChildFilter(createFilterForEntityKeys(idColumns, entityIds));

    // Group by the entity id column first, then the other end entity type for the interaction.
    List<org.hypertrace.core.query.service.api.Expression> idExpressions =
        idColumns.stream()
            .map(QueryRequestUtil::createAttributeExpression)
            .collect(Collectors.toList());
    builder.addAllGroupBy(idExpressions);

    List<org.hypertrace.core.query.service.api.Expression> selections = new ArrayList<>();
    for (Expression expression : interactionsRequest.getSelectionList()) {
      // Ignore the predefined selections because they're handled specially.
      if (ExpressionReader.isSimpleAttributeSelection(expression)
          && SELECTIONS_TO_IGNORE.contains(
              ExpressionReader.getAttributeIdFromAttributeSelection(expression).orElseThrow())) {
        continue;
      }

      // Selection should have metrics and attributes that were requested
      selections.add(QueryAndGatewayDtoConverter.convertToQueryExpression(expression).build());
    }

    // Pinot's GroupBy queries need at least one aggregate operation in the selection
    // so we add count(*) as a dummy placeholder if there are no explicit selectors.
    if (selections.isEmpty()) {
      selections.add(
          QueryRequestUtil.createCountByColumnSelection(
              Optional.ofNullable(idColumns.get(0)).orElseThrow()));
    }

    QueryRequest protoType = builder.build();
    Filter protoTypeFilter = filterBuilder.build();

    Map<String, QueryRequest> queryRequests = new HashMap<>();

    // In future we could send these queries in parallel to QueryService so that we can reduce the
    // response time.
    for (String e : entityTypes) {
      DomainEntityType otherEntityType = DomainEntityType.valueOf(e.toUpperCase());

      // Get the filters from the interactions request to 'AND' them with the timestamp filter.
      Filter.Builder filterCopy = Filter.newBuilder(protoTypeFilter);
      filterCopy.addChildFilter(
          convertToQueryFilter(interactionsRequest.getFilter(), otherEntityType));

      QueryRequest.Builder builderCopy = QueryRequest.newBuilder(protoType);
      builderCopy.setFilter(filterCopy);

      List<String> otherEntityIdColumns =
          getEntityIdColumnsFromInteraction(otherEntityType, incoming);
      List<org.hypertrace.core.query.service.api.Expression> otherIdExpressions =
          otherEntityIdColumns.stream()
              .map(QueryRequestUtil::createAttributeExpression)
              .collect(Collectors.toList());
      builderCopy.addAllGroupBy(otherIdExpressions);

      // Add all selections in the correct order. First id, then other entity id and finally
      // the remaining selections.
      builderCopy.addAllSelection(idExpressions);
      builderCopy.addAllSelection(otherIdExpressions);

      selections.forEach(builderCopy::addSelection);
      int limit = interactionsRequest.getLimit();
      if (limit > 0) {
        builderCopy.setLimit(limit);
      } else {
        builderCopy.setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
      }

      queryRequests.put(e, builderCopy.build());
    }

    return queryRequests;
  }

  private void parseResultSet(
      String entityType,
      String otherEntityType,
      Collection<Expression> selections,
      Map<String, FunctionExpression> metricToAggFunction,
      Iterator<ResultSetChunk> resultset,
      boolean incoming,
      Map<EntityKey, Builder> entityIdToBuilders,
      RequestContext requestContext) {

    Map<String, AttributeMetadata> attributeMetadataMap =
        metadataProvider.getAttributesMetadata(requestContext, SCOPE);

    Map<String, AttributeKind> aliasToAttributeKind =
        MetricAggregationFunctionUtil.getValueTypeForFunctionType(
            metricToAggFunction, attributeMetadataMap);

    while (resultset.hasNext()) {
      ResultSetChunk chunk = resultset.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // Construct the from/to EntityKeys from the columns
        List<String> idColumns =
            getEntityIdColumnsFromInteraction(
                DomainEntityType.valueOf(entityType.toUpperCase()),
                !incoming); // Note: We add the selections it in this order
        EntityKey entityId =
            EntityKey.of(
                IntStream.range(0, idColumns.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));

        List<String> otherIdColumns =
            getEntityIdColumnsFromInteraction(
                DomainEntityType.valueOf(otherEntityType.toUpperCase()), incoming);
        EntityKey otherEntityId =
            EntityKey.of(
                IntStream.range(idColumns.size(), idColumns.size() + otherIdColumns.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));

        EntityInteraction.Builder interaction = EntityInteraction.newBuilder();

        addInteractionEdges(
            interaction,
            selections,
            incoming ? otherEntityType : entityType,
            incoming ? otherEntityId : entityId,
            incoming ? entityType : otherEntityType,
            incoming ? entityId : otherEntityId);

        for (int i = idColumns.size() + otherIdColumns.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);

          // Ignore the count column since we introduced that ourselves into the query.
          if (StringUtils.equalsIgnoreCase(COUNT_COLUMN_NAME, metadata.getColumnName())) {
            continue;
          }

          // Check if this is an attribute vs metric and set it accordingly on the interaction.
          if (metricToAggFunction.containsKey(metadata.getColumnName())) {
            Value value =
                QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
                    aliasToAttributeKind, attributeMetadataMap, metadata, row.getColumn(i));
            interaction.putMetrics(
                metadata.getColumnName(),
                AggregatedMetricValue.newBuilder()
                    .setValue(value)
                    .setFunction(metricToAggFunction.get(metadata.getColumnName()).getFunction())
                    .build());
          } else {
            interaction.putAttribute(
                metadata.getColumnName(),
                QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
                    row.getColumn(i), attributeMetadataMap.get(metadata.getColumnName())));
          }
        }

        if (incoming) {
          entityIdToBuilders.get(entityId).addIncomingInteraction(interaction);
        } else {
          entityIdToBuilders.get(entityId).addOutgoingInteraction(interaction);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(interaction.build().toString());
        }
      }
    }
  }

  private void addInteractionEdges(
      EntityInteraction.Builder interaction,
      Collection<Expression> selections,
      String fromEntityType,
      EntityKey fromEntityId,
      String toEntityType,
      EntityKey toEntityId) {

    Map<String, String> selectionResultNames =
        selections.stream()
            .filter(ExpressionReader::isSimpleAttributeSelection)
            .collect(
                Collectors.toUnmodifiableMap(
                    expression ->
                        ExpressionReader.getAttributeIdFromAttributeSelection(expression)
                            .orElseThrow(),
                    expression ->
                        ExpressionReader.getSelectionResultName(expression).orElseThrow()));

    if (selectionResultNames.containsKey(FROM_ENTITY_ID_ATTRIBUTE_ID)) {
      interaction.putAttribute(
          selectionResultNames.get(FROM_ENTITY_ID_ATTRIBUTE_ID),
          Value.newBuilder()
              .setString(fromEntityId.toString())
              .setValueType(ValueType.STRING)
              .build());
    }
    if (selectionResultNames.containsKey(FROM_ENTITY_TYPE_ATTRIBUTE_ID)) {
      interaction.putAttribute(
          selectionResultNames.get(FROM_ENTITY_TYPE_ATTRIBUTE_ID),
          Value.newBuilder().setString(fromEntityType).setValueType(ValueType.STRING).build());
    }
    if (selectionResultNames.containsKey(TO_ENTITY_ID_ATTRIBUTE_ID)) {
      interaction.putAttribute(
          selectionResultNames.get(TO_ENTITY_ID_ATTRIBUTE_ID),
          Value.newBuilder()
              .setString(toEntityId.toString())
              .setValueType(ValueType.STRING)
              .build());
    }

    if (selectionResultNames.containsKey(TO_ENTITY_TYPE_ATTRIBUTE_ID)) {
      interaction.putAttribute(
          selectionResultNames.get(TO_ENTITY_TYPE_ATTRIBUTE_ID),
          Value.newBuilder().setString(toEntityType).setValueType(ValueType.STRING).build());
    }
  }

  private Optional<Filter> buildSpaceQueryFilterIfNeeded(
      RequestContext requestContext, String spaceId) {
    if (Strings.isNullOrEmpty(spaceId)) {
      return Optional.empty();
    }

    String fromSpaceId =
        this.metadataProvider
            .getAttributeMetadata(requestContext, SCOPE, FROM_SPACE_ATTRIBUTE_KEY)
            .orElseThrow()
            .getId();
    String toSpaceId =
        this.metadataProvider
            .getAttributeMetadata(requestContext, SCOPE, TO_SPACE_ATTRIBUTE_KEY)
            .orElseThrow()
            .getId();
    // For interactions, consider it in space only if both incoming and outgoing event spaces match
    return Optional.of(
        QueryRequestUtil.createCompositeFilter(
            Operator.AND,
            List.of(
                QueryRequestUtil.createStringFilter(fromSpaceId, Operator.EQ, spaceId),
                QueryRequestUtil.createStringFilter(toSpaceId, Operator.EQ, spaceId))));
  }
}
