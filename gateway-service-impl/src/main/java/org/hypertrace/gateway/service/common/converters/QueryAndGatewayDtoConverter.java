package org.hypertrace.gateway.service.common.converters;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to help with conversion of objects/DTOs from/to QueryService to/from EntityGateway
 * DTOs.
 */
public class QueryAndGatewayDtoConverter {

  private static final Logger LOG = LoggerFactory.getLogger(QueryAndGatewayDtoConverter.class);

  private static Value convertGatewayValueToQueryApiValue(
      org.hypertrace.gateway.service.v1.common.Value value) {
    Value.Builder builder = Value.newBuilder();

    switch (value.getValueType()) {
      case UNRECOGNIZED:
      case UNSET:
        return null;
      case BOOL:
        builder.setValueType(ValueType.BOOL);
        builder.setBoolean(value.getBoolean());
        break;
      case STRING:
        builder.setValueType(ValueType.STRING);
        builder.setString(value.getString());
        break;
      case LONG:
        builder.setValueType(ValueType.LONG);
        builder.setLong(value.getLong());
        break;
      case DOUBLE:
        builder.setValueType(ValueType.DOUBLE);
        builder.setDouble(value.getDouble());
        break;
      case TIMESTAMP:
        builder.setValueType(ValueType.TIMESTAMP);
        builder.setTimestamp(value.getTimestamp());
        break;
      case BOOLEAN_ARRAY:
        builder.setValueType(ValueType.BOOLEAN_ARRAY);
        builder.addAllBooleanArray(value.getBooleanArrayList());
        break;
      case STRING_ARRAY:
        builder.setValueType(ValueType.STRING_ARRAY);
        builder.addAllStringArray(value.getStringArrayList());
        break;
      case LONG_ARRAY:
        builder.setValueType(ValueType.LONG_ARRAY);
        builder.addAllLongArray(value.getLongArrayList());
        break;
      case DOUBLE_ARRAY:
        builder.setValueType(ValueType.DOUBLE_ARRAY);
        builder.addAllDoubleArray(value.getDoubleArrayList());
        break;
      case STRING_MAP:
        builder.setValueType(ValueType.STRING_MAP);
        builder.putAllStringMap(value.getStringMapMap());
        break;
    }

    return builder.build();
  }

  public static org.hypertrace.gateway.service.v1.common.Value convertQueryValueToGatewayValue(
      Value value) {
    org.hypertrace.gateway.service.v1.common.Value.Builder builder =
        org.hypertrace.gateway.service.v1.common.Value.newBuilder();
    switch (value.getValueType()) {
      case UNRECOGNIZED:
        break;
      case BOOL:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.BOOL);
        builder.setBoolean(value.getBoolean());
        break;
      case STRING:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING);
        builder.setString(value.getString());
        break;
      case INT:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.LONG);
        builder.setLong(value.getInt());
        break;
      case LONG:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.LONG);
        builder.setLong(value.getLong());
        break;
      case FLOAT:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.DOUBLE);
        builder.setDouble(value.getFloat());
        break;
      case DOUBLE:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.DOUBLE);
        builder.setDouble(value.getDouble());
        break;
      case TIMESTAMP:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.TIMESTAMP);
        builder.setTimestamp(value.getTimestamp());
        break;
      case BOOLEAN_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.BOOLEAN_ARRAY);
        builder.addAllBooleanArray(value.getBooleanArrayList());
        break;
      case STRING_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING_ARRAY);
        builder.addAllStringArray(value.getStringArrayList());
        break;
      case INT_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.LONG_ARRAY);
        builder.addAllLongArray(
            NumberListConverter.convertIntListToLongList(value.getIntArrayList()));
        break;
      case LONG_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.LONG_ARRAY);
        builder.addAllLongArray(value.getLongArrayList());
        break;
      case FLOAT_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.DOUBLE_ARRAY);
        builder.addAllDoubleArray(
            NumberListConverter.convertFloatListToDoubleList(value.getFloatArrayList()));
        break;
      case DOUBLE_ARRAY:
        builder.setValueType(org.hypertrace.gateway.service.v1.common.ValueType.DOUBLE_ARRAY);
        builder.addAllDoubleArray(value.getDoubleArrayList());
        break;
    }

    return builder.build();
  }

  public static org.hypertrace.gateway.service.v1.common.Value convertQueryValueToGatewayValue(
      Value value, AttributeMetadata attributeMetadata) {
    ToAttributeKindConverter converter = null;
    org.hypertrace.gateway.service.v1.common.Value retValue = null;

    if (attributeMetadata == null) {
      // no attribute metadata, fail fast
      LOG.warn("No attribute metadata specified for {}", value);
      return convertQueryValueToGatewayValue(value);
    }

    switch (value.getValueType()) {
      case STRING:
        converter = StringToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getString(), attributeMetadata.getValueKind());
        break;
      case INT:
        converter = IntegerToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getInt(), attributeMetadata.getValueKind());
        break;
      case BOOL:
        converter = BooleanToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getBoolean(), attributeMetadata.getValueKind());
        break;
      case LONG:
        converter = LongToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getLong(), attributeMetadata.getValueKind());
        break;
      case FLOAT:
        converter = FloatToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getFloat(), attributeMetadata.getValueKind());
        break;
      case DOUBLE:
        converter = DoubleToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getDouble(), attributeMetadata.getValueKind());
        break;
      case TIMESTAMP:
        converter = TimestampToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getTimestamp(), attributeMetadata.getValueKind());
        break;
      case INT_ARRAY:
        converter = IntegerArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getIntArrayList(), attributeMetadata.getValueKind());
        break;
      case LONG_ARRAY:
        converter = LongArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getLongArrayList(), attributeMetadata.getValueKind());
        break;
      case FLOAT_ARRAY:
        converter = FloatArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getFloatArrayList(), attributeMetadata.getValueKind());
        break;
      case DOUBLE_ARRAY:
        converter = DoubleArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getDoubleArrayList(), attributeMetadata.getValueKind());
        break;
      case STRING_ARRAY:
        converter = StringArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getStringArrayList(), attributeMetadata.getValueKind());
        break;
      case BOOLEAN_ARRAY:
        converter = BooleanArrayToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getBooleanArrayList(), attributeMetadata.getValueKind());
        break;
      case STRING_MAP:
        converter = StringMapToAttributeKindConverter.INSTANCE;
        retValue = converter.convert(value.getStringMapMap(), attributeMetadata.getValueKind());
        break;
      default:
        break;
    }

    if (null == converter) {
      // If we reached here it implies no converter was found
      LOG.warn("No attributeKind converter found for query valueType {}", value.getValueType());
      return convertQueryValueToGatewayValue(value);
    }

    if (null == retValue) {
      LOG.warn(
          "No attributeKind mapping found for query valueType => attributeKind [{} => {}]",
          value.getValueType(),
          attributeMetadata.getValueKind());
      return convertQueryValueToGatewayValue(value);
    }
    return retValue;
  }

  public static org.hypertrace.gateway.service.v1.common.Value convertToGatewayValue(
      String attributeName, Value value, Map<String, AttributeMetadata> attributeMetadataMap) {
    AttributeMetadata attributeMetadata = attributeMetadataMap.get(attributeName);
    if (null == attributeMetadata) {
      LOG.warn("No attribute metadata found for {}", attributeName);
      return convertQueryValueToGatewayValue(value);
    }
    return convertQueryValueToGatewayValue(value, attributeMetadata);
  }

  public static Operator convertOperator(
      org.hypertrace.gateway.service.v1.common.Operator operator) {
    switch (operator) {
      case AND:
        return Operator.AND;
      case OR:
        return Operator.OR;
      case NOT:
        return Operator.NOT;
      case EQ:
        return Operator.EQ;
      case NEQ:
        return Operator.NEQ;
      case IN:
        return Operator.IN;
      case NOT_IN:
        return Operator.NOT_IN;
      case RANGE:
        return Operator.RANGE;
      case GT:
        return Operator.GT;
      case LT:
        return Operator.LT;
      case GE:
        return Operator.GE;
      case LE:
        return Operator.LE;
      case LIKE:
        return Operator.LIKE;
      case CONTAINS_KEY:
        return Operator.CONTAINS_KEY;
      case CONTAINS_KEYVALUE:
        return Operator.CONTAINS_KEYVALUE;
    }
    throw new IllegalArgumentException("Unsupported operator " + operator.name());
  }

  private static ColumnIdentifier.Builder convertToQueryColumnIdentifier(
      org.hypertrace.gateway.service.v1.common.ColumnIdentifier columnIdentifier) {
    return ColumnIdentifier.newBuilder()
        .setColumnName(columnIdentifier.getColumnName())
        .setAlias(columnIdentifier.getAlias());
  }

  public static Expression.Builder convertToQueryExpression(
      org.hypertrace.gateway.service.v1.common.Expression expression) {
    Expression.Builder builder = Expression.newBuilder();
    switch (expression.getValueCase()) {
      case VALUE_NOT_SET:
        break;
      case COLUMNIDENTIFIER:
        builder.setColumnIdentifier(
            convertToQueryColumnIdentifier(expression.getColumnIdentifier()));
        break;
      case FUNCTION:
        builder.setFunction(convertToQueryFunction(expression.getFunction()));
        break;
      case LITERAL:
        builder.setLiteral(convertToQueryLiteral(expression.getLiteral()));
        break;
      case ORDERBY:
        builder.setOrderBy(convertToQueryOrderBy(expression.getOrderBy()));
        break;
      case HEALTH:
        throw new IllegalArgumentException("Cannot convert HEALTH expression to query expression.");
    }

    return builder;
  }

  private static OrderByExpression convertToQueryOrderBy(
      org.hypertrace.gateway.service.v1.common.OrderByExpression orderBy) {
    return OrderByExpression.newBuilder()
        .setOrder(SortOrder.valueOf(orderBy.getOrder().name()))
        .setExpression(convertToQueryExpression(orderBy.getExpression()))
        .build();
  }

  private static LiteralConstant convertToQueryLiteral(
      org.hypertrace.gateway.service.v1.common.LiteralConstant literal) {
    return LiteralConstant.newBuilder()
        .setValue(convertGatewayValueToQueryApiValue(literal.getValue()))
        .build();
  }

  private static Function convertToQueryFunction(
      org.hypertrace.gateway.service.v1.common.FunctionExpression function) {
    Function.Builder builder =
        Function.newBuilder()
            .setFunctionName(function.getFunction().name())
            .setAlias(function.getAlias());

    // AVGRATE is adding a specific implementation because Pinot does not directly support this
    // function
    switch (function.getFunction()) {
      case AVGRATE:
        {
          builder.setFunctionName(FunctionType.SUM.name()).setAlias(function.getAlias());

          // Adds only the argument that is a column identifier for now.
          List<org.hypertrace.gateway.service.v1.common.Expression> columns =
              function.getArgumentsList().stream()
                  .filter(org.hypertrace.gateway.service.v1.common.Expression::hasColumnIdentifier)
                  .collect(Collectors.toList());
          columns.forEach(e -> builder.addArguments(convertToQueryExpression(e)));
          break;
        }
      case PERCENTILE:
        {
          org.hypertrace.gateway.service.v1.common.Expression percentileExp =
              function.getArgumentsList().stream()
                  .filter(org.hypertrace.gateway.service.v1.common.Expression::hasLiteral)
                  .findFirst()
                  .orElseThrow(); // Should have validated arguments using PercentileValidator

          long percentile = percentileExp.getLiteral().getValue().getLong();
          String functionName = function.getFunction().name() + percentile;
          builder.setFunctionName(functionName).setAlias(function.getAlias());

          // Adds only the argument that is a literal identifier. This will bring the required nth
          // percentile.
          List<org.hypertrace.gateway.service.v1.common.Expression> columns =
              function.getArgumentsList().stream()
                  .filter(org.hypertrace.gateway.service.v1.common.Expression::hasColumnIdentifier)
                  .collect(Collectors.toList());
          columns.forEach(e -> builder.addArguments(convertToQueryExpression(e)));
          break;
        }
      default:
        {
          builder.setFunctionName(function.getFunction().name()).setAlias(function.getAlias());

          if (function.getArgumentsCount() > 0) {
            function
                .getArgumentsList()
                .forEach(
                    e -> {
                      // Health expressions are treated differently so ignore them.
                      if (!e.hasHealth()) {
                        builder.addArguments(convertToQueryExpression(e));
                      }
                    });
          }
        }
    }
    return builder.build();
  }

  public static Filter convertToQueryFilter(
      org.hypertrace.gateway.service.v1.common.Filter filter) {
    if (filter.equals(org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance())) {
      return Filter.getDefaultInstance();
    }
    Filter.Builder builder = Filter.newBuilder();
    builder.setOperator(convertOperator(filter.getOperator()));
    if (filter.getChildFilterCount() > 0) {
      // if there are child filters, handle them recursively.
      for (org.hypertrace.gateway.service.v1.common.Filter child : filter.getChildFilterList()) {
        builder.addChildFilter(convertToQueryFilter(child));
      }
    } else {
      // Copy the lhs and rhs from the filter.
      builder.setLhs(convertToQueryExpression(filter.getLhs()));
      builder.setRhs(convertToQueryExpression(filter.getRhs()));
    }

    return builder.build();
  }

  public static Filter addTimeAndSpaceFiltersAndConvertToQueryFilter(
      long startTimeMillis,
      long endTimeMillis,
      String spaceId,
      String timestampAttributeId,
      String spacesAttributeId,
      org.hypertrace.gateway.service.v1.common.Filter providedFilter) {

    Filter.Builder compositeFilter = Filter.newBuilder().setOperator(Operator.AND);
    Filter convertedProvidedFilter =
        isNonDefaultFilter(providedFilter)
            ? convertToQueryFilter(providedFilter)
            : Filter.getDefaultInstance();

    if (!hasTimeRangeFilter(convertedProvidedFilter, timestampAttributeId)) {
      compositeFilter.addChildFilter(
          QueryRequestUtil.createBetweenTimesFilter(
              timestampAttributeId, startTimeMillis, endTimeMillis));
    }

    if (isNonDefaultFilter(convertedProvidedFilter)) {
      compositeFilter.addChildFilter(convertedProvidedFilter);
    }

    if (!Strings.isNullOrEmpty(spaceId)) {
      compositeFilter.addChildFilter(
          QueryRequestUtil.createStringFilter(spacesAttributeId, Operator.EQ, spaceId));
    }

    // If only one filter was added, unwrap the one child filter and use that
    return compositeFilter.getChildFilterCount() == 1
        ? compositeFilter.getChildFilter(0)
        : compositeFilter.build();
  }

  private static boolean hasTimeRangeFilter(Filter filter, String timestampAttributeId) {
    // Used to prevent duplicate time ranges added from different locations
    if (filter.getOperator() == Operator.AND || filter.getOperator() == Operator.OR) {
      return filter.getChildFilterList().stream()
          .anyMatch(f -> hasTimeRangeFilter(f, timestampAttributeId));
    }
    return filter.getLhs().getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER
        && filter.getLhs().getColumnIdentifier().getColumnName().equals(timestampAttributeId);
  }

  private static boolean isNonDefaultFilter(org.hypertrace.gateway.service.v1.common.Filter filter) {
    return filter != null
        && !org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance()
                                                           .equals(filter);
  }

  private static boolean isNonDefaultFilter(Filter filter) {
    return filter != null && !Filter.getDefaultInstance().equals(filter);
  }

  public static List<OrderByExpression> convertToQueryOrderByExpressions(
      List<org.hypertrace.gateway.service.v1.common.OrderByExpression> gatewayOrderBy) {
    return gatewayOrderBy.stream()
        .map(QueryAndGatewayDtoConverter::convertToQueryOrderBy)
        .collect(Collectors.toList());
  }

  /**
   * Converts query value for metric aggregations to gateway value by ensuring that aggregations
   * that involve division like AVG, AVGRATE have values represented as DOUBLE
   *
   * @return gateway value
   */
  public static org.hypertrace.gateway.service.v1.common.Value convertToGatewayValueForMetricValue(
      Map<String, AttributeKind> aliasToOverrideKind,
      Map<String, AttributeMetadata> attributeMetadataMap,
      ColumnMetadata metadata,
      Value queryValue) {
    AttributeKind overrideKind = aliasToOverrideKind.get(metadata.getColumnName());
    return convertToGatewayValueForMetricValue(
        overrideKind, attributeMetadataMap, metadata, queryValue);
  }

  public static org.hypertrace.gateway.service.v1.common.Value convertToGatewayValueForMetricValue(
      AttributeKind attributeKind,
      Map<String, AttributeMetadata> attributeMetadataMap,
      ColumnMetadata metadata,
      Value queryValue) {
    String columnName = metadata.getColumnName();
    org.hypertrace.gateway.service.v1.common.Value gwValue;
    // if there's no override from the function, then this is regular attribute, then
    // use the attribute map to convert the value
    AttributeMetadata attributeMetadata;
    if (attributeKind == null) {
      attributeMetadata = attributeMetadataMap.get(columnName);
    } else {
      attributeMetadata =
          AttributeMetadata.newBuilder()
              .setId(metadata.getColumnName())
              .setType(AttributeType.METRIC)
              .setValueKind(attributeKind)
              .build();
    }
    LOG.debug(
        "Converting {} from type: {} to type: {}",
        columnName,
        metadata.getValueType().name(),
        attributeMetadata.getValueKind().name());
    gwValue =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(queryValue, attributeMetadata);

    return gwValue;
  }
}
