package org.hypertrace.gateway.service.common.transformer;

import static org.hypertrace.gateway.service.v1.common.Operator.AND;
import static org.hypertrace.gateway.service.v1.common.Operator.EQ;
import static org.hypertrace.gateway.service.v1.common.Operator.LIKE;
import static org.hypertrace.gateway.service.v1.common.Operator.NEQ;
import static org.hypertrace.gateway.service.v1.common.Operator.OR;
import static org.hypertrace.gateway.service.v1.common.Operator.UNDEFINED;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.TimeRangeFilterUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.DomainObjectFilter;
import org.hypertrace.gateway.service.entity.config.DomainObjectMapping;
import org.hypertrace.gateway.service.trace.TraceScope;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre processes the incoming {@link EntitiesRequest}
 *
 * <p>The Domain Object's Id Attribute referenced anywhere in the Select/Order/Filter, is mapped to
 * its corresponding Identifying Attributes based on the Domain Object's configuration and the
 * EntitiesRequest is transformed accordingly
 */
public class RequestPreProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestPreProcessor.class);
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ScopeFilterConfigs scopeFilterConfigs;

  public RequestPreProcessor(AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.scopeFilterConfigs = scopeFilterConfigs;
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request
   */
  public EntitiesRequest transform(
      EntitiesRequest originalRequest, EntitiesRequestContext context) {
    Map<String, List<DomainObjectMapping>> attributeIdMappings =
        AttributeMetadataUtil.getAttributeIdMappings(
            attributeMetadataProvider, context, originalRequest.getEntityType());
    EntitiesRequest.Builder entitiesRequestBuilder = EntitiesRequest.newBuilder(originalRequest);

    // Convert the time range into a filter and set it on the request so that all downstream
    // components needn't treat it specially.
    Filter filter = TimeRangeFilterUtil.addTimeRangeFilter(
        context.getTimestampAttributeId(), originalRequest.getFilter(),
        originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());

    if (attributeIdMappings.isEmpty()) {
      LOGGER.debug(
          "No attribute id mappings found for entityType:{}", originalRequest.getEntityType());
      return entitiesRequestBuilder
          .clearSelection()
          .setFilter(filter)
          // Clean out duplicate columns in selections
          .addAllSelection(getUniqueSelections(originalRequest.getSelectionList()))
          .build();
    }

    // Handle mappings in selection expressions
    entitiesRequestBuilder.clearSelection();
    List<Expression> selections =
        transformSelection(attributeIdMappings, originalRequest.getSelectionList(), context);
    entitiesRequestBuilder.addAllSelection(getUniqueSelections(selections));

    entitiesRequestBuilder.clearTimeAggregation();
    entitiesRequestBuilder.addAllTimeAggregation(
        transformTimeAggregation(
            attributeIdMappings, originalRequest.getTimeAggregationList(), context));

    // Handle mappings in order by
    entitiesRequestBuilder.clearOrderBy();
    entitiesRequestBuilder.addAllOrderBy(
        transformOrderBy(attributeIdMappings, originalRequest.getOrderByList(), context));

    // Handle mappings in filter
    entitiesRequestBuilder.clearFilter();
    Filter filterToInject =
        injectIdFilter(attributeIdMappings, originalRequest.getSelectionList(), context);

    Filter transformedFilter = transformFilter(attributeIdMappings, filter, context);
    filter = mergeFilters(filterToInject, transformedFilter);

    // Apply the scope filter at the end.
    filter = scopeFilterConfigs.createScopeFilter(
            originalRequest.getEntityType(),
            filter,
            attributeMetadataProvider,
            context);

    entitiesRequestBuilder.setFilter(filter);

    return entitiesRequestBuilder.build();
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request
   */
  public TracesRequest transform(TracesRequest originalRequest, RequestContext requestContext) {

    Map<String, List<DomainObjectMapping>> attributeIdMappings =
        AttributeMetadataUtil.getAttributeIdMappings(
            attributeMetadataProvider, requestContext, originalRequest.getScope());
    if (attributeIdMappings.isEmpty()) {
      LOGGER.debug("No attribute id mappings found for scope:{}", originalRequest.getScope());
      return originalRequest;
    }

    TracesRequest.Builder tracesRequestBuilder = TracesRequest.newBuilder(originalRequest);

    // Handle mappings in selection expressions
    tracesRequestBuilder.clearSelection();
    tracesRequestBuilder.addAllSelection(
        transformSelection(
            attributeIdMappings, originalRequest.getSelectionList(), requestContext));

    // Handle mappings in order by
    tracesRequestBuilder.clearOrderBy();
    tracesRequestBuilder.addAllOrderBy(
        transformOrderBy(attributeIdMappings, originalRequest.getOrderByList(), requestContext));

    // Handle mappings in filter
    tracesRequestBuilder.clearFilter();
    Filter filterToInject =
        injectIdFilter(attributeIdMappings, originalRequest.getSelectionList(), requestContext);
    Filter transformedFilter =
        transformFilter(attributeIdMappings, originalRequest.getFilter(), requestContext);
    Filter filter = mergeFilters(filterToInject, transformedFilter);

    TraceScope scope = TraceScope.valueOf(originalRequest.getScope());
    filter = scopeFilterConfigs.createScopeFilter(
            scope.name(),
            filter,
            attributeMetadataProvider,
            requestContext);
    tracesRequestBuilder.setFilter(filter);

    return tracesRequestBuilder.build();
  }

  private boolean doesExpressionNeedRemapping(
      Map<String, List<DomainObjectMapping>> attributeIdMappings, Expression expression) {
    return expression.getValueCase() == ValueCase.COLUMNIDENTIFIER
        && attributeIdMappings.containsKey(expression.getColumnIdentifier().getColumnName());
  }

  private List<Expression> transformSelection(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      List<Expression> expressions,
      RequestContext requestContext) {
    List<Expression> transformedExpressions = new ArrayList<>();

    for (Expression expression : expressions) {
      if (expression.getValueCase() == ValueCase.COLUMNIDENTIFIER) {
        transformedExpressions.addAll(
            transformExpression(attributeIdMappings, expression, requestContext));
      } else if (expression.getValueCase() == ValueCase.FUNCTION) {
        transformedExpressions.add(
            transformFunction(attributeIdMappings, expression, requestContext));
      } else {
        transformedExpressions.add(expression);
      }
    }
    return transformedExpressions;
  }

  public static List<Expression> getUniqueSelections(List<Expression> expressions) {
    List<Expression> selections = new ArrayList<>();
    Set<String> uniqueColumnNames = new HashSet<>();

    for (Expression expression : expressions) {
      // unique columns only
      if (expression.getValueCase() == ValueCase.COLUMNIDENTIFIER) {
        String columnName = expression.getColumnIdentifier().getColumnName();
        if (!uniqueColumnNames.contains(columnName)) {
          uniqueColumnNames.add(columnName);
          selections.add(expression);
        }
      } else {
        selections.add(expression);
      }
    }

    return selections;
  }

  private Expression transformFunction(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      Expression expression,
      RequestContext requestContext) {
    FunctionExpression functionExpression = expression.getFunction();
    FunctionExpression.Builder builder = FunctionExpression.newBuilder(functionExpression);
    List<Expression> expressions = functionExpression.getArgumentsList();
    builder.clearArguments();

    for (Expression argument : expressions) {
      List<Expression> transformedExpressions =
          transformExpression(attributeIdMappings, argument, requestContext);
      if (transformedExpressions.size() == 1) {
        builder.addArguments(transformedExpressions.get(0));
      } else {
        LOGGER.warn(
            "Function domain object {} should map to one domain object entry {}",
            argument,
            transformedExpressions);
        throw new IllegalArgumentException(
            "Function domain object column should map to one domain object entry");
      }
    }

    return Expression.newBuilder(expression).setFunction(builder).build();
  }

  private List<TimeAggregation> transformTimeAggregation(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      List<TimeAggregation> timeAggregations,
      RequestContext requestContext) {
    List<TimeAggregation> transformedAggregations = new ArrayList<>();

    for (TimeAggregation aggregation : timeAggregations) {
      TimeAggregation.Builder builder = TimeAggregation.newBuilder(aggregation);
      builder.setAggregation(
          transformFunction(attributeIdMappings, aggregation.getAggregation(), requestContext));
      transformedAggregations.add(builder.build());
    }

    return transformedAggregations;
  }

  private List<Expression> transformExpression(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      Expression expression,
      RequestContext requestContext) {
    if (!doesExpressionNeedRemapping(attributeIdMappings, expression)) {
      return ImmutableList.of(expression);
    }

    String column = expression.getColumnIdentifier().getColumnName();
    return attributeIdMappings.get(column).stream()
        .map(
            col ->
                QueryExpressionUtil.getColumnExpression(
                    AttributeMetadataUtil.getAttributeMetadata(
                        attributeMetadataProvider, requestContext, col)
                        .getId())
                    .build())
        .collect(Collectors.toList());
  }

  private List<OrderByExpression> transformOrderBy(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      List<OrderByExpression> orderByExpressions,
      RequestContext requestContext) {

    List<OrderByExpression> transformedExpressions = new ArrayList<>();
    for (OrderByExpression orderByExpression : orderByExpressions) {
      Expression expression = orderByExpression.getExpression();
      if (expression.getValueCase() == ValueCase.COLUMNIDENTIFIER) {
        transformedExpressions.addAll(
            transformExpression(attributeIdMappings, expression, requestContext).stream()
                .map(
                    col ->
                        OrderByExpression.newBuilder(orderByExpression).setExpression(col).build())
                .collect(Collectors.toList()));
      } else if (expression.getValueCase() == ValueCase.FUNCTION) {
        transformedExpressions.add(
            OrderByExpression.newBuilder(orderByExpression)
                .setExpression(transformFunction(attributeIdMappings, expression, requestContext))
                .build());
      } else {
        transformedExpressions.add(orderByExpression);
      }
    }
    return transformedExpressions;
  }

  private Filter transformFilter(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      Filter filter,
      RequestContext requestContext) {
    if (filter.getOperator().equals(AND) || filter.getOperator().equals(OR)) {
      return Filter.newBuilder()
          .setOperator(filter.getOperator())
          .addAllChildFilter(
              filter.getChildFilterList().stream()
                  .map(f -> transformFilter(attributeIdMappings, f, requestContext))
                  .collect(Collectors.toList()))
          .build();
    } else {
      return transformLeafFilter(attributeIdMappings, filter, requestContext);
    }
  }

  private Filter transformLeafFilter(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      Filter filter,
      RequestContext requestContext) {
    // If the lhs expression doesn't need remapping, return the original filter itself
    if (!doesExpressionNeedRemapping(attributeIdMappings, filter.getLhs())) {
      return filter;
    }

    // If the rhs is not a literal value, remapping is not supported
    if (!filter.getRhs().getValueCase().equals(ValueCase.LITERAL)) {
      return filter;
    }

    // If the domain object mappings contains one item that equals to the lhs expression column
    // there's no need to remap. Note equality also means there's no filter in the mapping eg:
    //    {
    //      scope = API
    //      key = id
    //      primaryKey = true
    //      mapping = [
    //      {
    //        scope = API
    //        key = id
    //      }
    //     ]
    //    }
    String lhsColumnName = filter.getLhs().getColumnIdentifier().getColumnName();
    List<DomainObjectMapping> domainObjectMappings = attributeIdMappings.get(lhsColumnName);

    if (domainObjectMappings.size() == 1) {
      DomainObjectMapping domainObjectMapping = domainObjectMappings.get(0);
      AttributeMetadata attributeMetadata = attributeMetadataProvider.getAttributeMetadata(
          requestContext,
          domainObjectMapping.getScope(),
          domainObjectMapping.getKey()
      ).orElseThrow();
      if (domainObjectMapping.getFilter() == null && attributeMetadata.getId().equals(lhsColumnName)) {
        return filter;
      }
    }

    Operator operator = filter.getOperator();
    switch (operator) {
      case EQ:
      case NEQ:
      case LIKE:
        return transformSingleValuedFilter(
            domainObjectMappings,
            operator,
            filter.getRhs().getLiteral().getValue().getString(),
            requestContext);
      case IN:
        return Filter.newBuilder()
            .setOperator(OR)
            .addAllChildFilter(
                filter.getRhs().getLiteral().getValue().getStringArrayList().stream()
                    .map(
                        val ->
                            transformSingleValuedFilter(
                                domainObjectMappings,
                                EQ,
                                val,
                                requestContext))
                    .collect(Collectors.toList()))
            .build();
      case NOT_IN:
        return Filter.newBuilder()
            .setOperator(AND)
            .addAllChildFilter(
                filter.getRhs().getLiteral().getValue().getStringArrayList().stream()
                    .map(
                        val ->
                            transformSingleValuedFilter(
                                domainObjectMappings,
                                NEQ,
                                val,
                                requestContext))
                    .collect(Collectors.toList()))
            .build();
      default:
        throw new IllegalArgumentException(
            String.format("Invalid operator for Filter on composite Id column. Filter:%s", filter));
    }
  }

  private Filter transformSingleValuedFilter(
      List<DomainObjectMapping> mappedIds,
      Operator operator,
      String rhsValue,
      RequestContext requestContext) {
    EntityKey entityKey = EntityKey.from(rhsValue);
    if (mappedIds.size() != entityKey.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Literal for composite id column doesn't have required number of values."
                  + " Invalid rhsValue:%s for operator:%s",
              rhsValue, operator));
    }
    switch (operator) {
      case EQ:
        return transformEQFilter(mappedIds, entityKey, requestContext);
      case NEQ:
        return transformNEQFilter(mappedIds, entityKey, requestContext);
      case LIKE:
        return transformLikeFilter(mappedIds, entityKey, requestContext);
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Operator:%s for single valued filter", operator));
    }
  }

  private Filter transformEQFilter(
      List<DomainObjectMapping> mappedIds, EntityKey entityKey, RequestContext requestContext) {
    List<Filter> childFilters =
        IntStream.range(0, entityKey.size())
            .mapToObj(
                i ->
                    Filter.newBuilder()
                        .setLhs(
                            QueryExpressionUtil.getColumnExpression(
                                AttributeMetadataUtil.getAttributeMetadata(
                                    attributeMetadataProvider, requestContext, mappedIds.get(i))
                                    .getId()))
                        .setOperator(EQ)
                        .setRhs(QueryExpressionUtil.getLiteralExpression(entityKey.get(i)))
                        .build())
            .collect(Collectors.toList());
    return childFilters.size() > 1 ? Filter.newBuilder().setOperator(AND).addAllChildFilter(childFilters).build() :
        childFilters.get(0);
  }

  private Filter transformNEQFilter(
      List<DomainObjectMapping> mappedIds, EntityKey entityKey, RequestContext requestContext) {
    List<Filter> childFilters =
        IntStream.range(0, entityKey.size())
            .mapToObj(
                i ->
                    Filter.newBuilder()
                        .setLhs(
                            QueryExpressionUtil.getColumnExpression(
                                AttributeMetadataUtil.getAttributeMetadata(
                                    attributeMetadataProvider, requestContext, mappedIds.get(i))
                                    .getId()))
                        .setOperator(NEQ)
                        .setRhs(QueryExpressionUtil.getLiteralExpression(entityKey.get(i)))
                        .build())
            .collect(Collectors.toList());
    return childFilters.size() > 1 ? Filter.newBuilder().setOperator(OR).addAllChildFilter(childFilters).build() :
        childFilters.get(0);
  }

  private Filter transformLikeFilter(
      List<DomainObjectMapping> mappedIds, EntityKey entityKey, RequestContext requestContext) {
    List<Filter> childFilters =
        IntStream.range(0, entityKey.size())
            .mapToObj(
                i ->
                    Filter.newBuilder()
                        .setLhs(
                            QueryExpressionUtil.getColumnExpression(
                                AttributeMetadataUtil.getAttributeMetadata(
                                    attributeMetadataProvider, requestContext, mappedIds.get(i))
                                    .getId()))
                        .setOperator(LIKE)
                        .setRhs(QueryExpressionUtil.getLiteralExpression(entityKey.get(i)))
                        .build())
            .collect(Collectors.toList());
    return childFilters.size() > 1 ? Filter.newBuilder().setOperator(AND).addAllChildFilter(childFilters).build() :
        childFilters.get(0);
  }

  /**
   * @param injectIdFilter    Filter to inject from Domain Object Mapping Filter
   * @param transformedFilter Transformed filter based on Domain Object Mapping columns
   * @return Returns AND of injectIdFilter and transformedFilter
   */
  private Filter mergeFilters(Filter injectIdFilter, Filter transformedFilter) {
    if (injectIdFilter == null) {
      return transformedFilter;
    }

    if (transformedFilter.getOperator().equals(UNDEFINED)) {
      return injectIdFilter;
    }

    return Filter.newBuilder()
        .setOperator(AND)
        .addChildFilter(injectIdFilter)
        .addChildFilter(transformedFilter)
        .build();
  }

  /**
   * Creates a filter for Domain Object, if any filter available
   * {
   *  scope = EVENT
   *  key = id
   *  mapping = [
   *    {
   *      scope = SERVICE
   *      key = id
   *    },
   *    {
   *      scope = API
   *      key = isExternal
   *      filter {
   *        value = true
   *      }
   *    }
   *  ]
   * }
   *
   * <p>will create a filter `API.is_external = true` which needs to be injected when Domain.id is
   * queried
   */
  @Nullable
  private Filter injectIdFilter(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      List<Expression> expressions,
      RequestContext requestContext) {
    for (Expression expression : expressions) {
      if (!doesExpressionNeedRemapping(attributeIdMappings, expression)) {
        continue;
      }

      String columnName = expression.getColumnIdentifier().getColumnName();
      List<DomainObjectMapping> mappedIdDomainObjects = attributeIdMappings.get(columnName);
      for (DomainObjectMapping mappedIdDomainObject : mappedIdDomainObjects) {
        DomainObjectFilter filter = mappedIdDomainObject.getFilter();
        if (filter == null) {
          continue;
        }

        String mappedDomainId =
            AttributeMetadataUtil.getAttributeMetadata(
                attributeMetadataProvider, requestContext, mappedIdDomainObject)
                .getId();
        Expression lhs = QueryExpressionUtil.getColumnExpression(mappedDomainId).build();
        Expression rhs =
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            getValueFromDomainObjectFilter(
                                requestContext,
                                mappedIdDomainObject.getScope(),
                                mappedIdDomainObject.getKey(),
                                filter.getValue()))
                        .build())
                .build();

        // currently, supports adding filter only on one of the mapped domain object
        return Filter.newBuilder().setLhs(lhs).setOperator(EQ).setRhs(rhs).build();
      }
    }

    return null;
  }

  /**
   * @param scope scope of the attribute
   * @param key   key of the attribute
   * @param value value of the attribute
   * @return Returns the Value object based on the AttributeKind of `value` defined in
   * AttributeService
   */
  @Nonnull
  private Value getValueFromDomainObjectFilter(
      RequestContext requestContext, String scope, String key, String value) {
    AttributeKind attributeKind =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext, scope, key)
            .get()
            .getValueKind();
    Value.Builder builder = Value.newBuilder();

    switch (attributeKind) {
      case UNRECOGNIZED:
      case KIND_UNDEFINED:
      case TYPE_BOOL_ARRAY:
      case TYPE_STRING_ARRAY:
      case TYPE_INT64_ARRAY:
      case TYPE_DOUBLE_ARRAY:
      case TYPE_STRING_MAP:
      case TYPE_BYTES:
        LOGGER.warn(
            "Value type {} for value {} not supported for domain object filter",
            attributeKind,
            value);
        break;
      case TYPE_BOOL:
        builder.setValueType(ValueType.BOOL);
        builder.setBoolean(Boolean.parseBoolean(value));
        break;
      case TYPE_STRING:
        builder.setValueType(ValueType.STRING);
        builder.setString(value);
        break;
      case TYPE_INT64:
        builder.setValueType(ValueType.LONG);
        builder.setLong(Long.parseLong(value));
        break;
      case TYPE_DOUBLE:
        builder.setValueType(ValueType.DOUBLE);
        builder.setDouble(Double.parseDouble(value));
        break;
      case TYPE_TIMESTAMP:
        builder.setValueType(ValueType.TIMESTAMP);
        builder.setTimestamp(Long.parseLong(value));
        break;
    }

    return builder.build();
  }
}
