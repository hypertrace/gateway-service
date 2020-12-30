package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.SortOrder;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityServiceAndGatewayServiceConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityServiceAndGatewayServiceConverter.class);

  /**
   * Add between time filter for queries on EDS where a time notion (based on the latest entity
   * state) is supported.
   *
   * @param startTimeMillis
   * @param endTimeMillis
   * @param attributeMetadataProvider
   * @param entitiesRequest
   * @param builder
   * @return
   */
  public static void addBetweenTimeFilter(
      long startTimeMillis,
      long endTimeMillis,
      AttributeMetadataProvider attributeMetadataProvider,
      EntitiesRequest entitiesRequest,
      EntityQueryRequest.Builder builder,
      EntitiesRequestContext entitiesRequestContext) {
    // only scope has timestamp is supported
    if (TimestampConfigs.getTimestampColumn(entitiesRequest.getEntityType()) == null) {
      return;
    }

    String timestampAttributeName =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, entitiesRequestContext, entitiesRequest.getEntityType());
    Expression.Builder startTimeConstant =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(ValueType.LONG).setLong(startTimeMillis)));

    Expression.Builder endTimeConstant =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(ValueType.LONG).setLong(endTimeMillis)));

    Filter.Builder startTimeFilterBuilder = Filter.newBuilder();
    startTimeFilterBuilder.setOperator(Operator.GE);
    startTimeFilterBuilder.setLhs(createColumnExpression(timestampAttributeName));
    startTimeFilterBuilder.setRhs(startTimeConstant);

    Filter.Builder endTimeFilterBuilder = Filter.newBuilder();
    endTimeFilterBuilder.setOperator(Operator.LT);
    endTimeFilterBuilder.setLhs(createColumnExpression(timestampAttributeName));
    endTimeFilterBuilder.setRhs(endTimeConstant);

    Filter existingFilter = builder.getFilter();

    Filter.Builder newFilterBuilder = Filter.newBuilder();
    newFilterBuilder.setOperator(Operator.AND);
    newFilterBuilder.addChildFilter(startTimeFilterBuilder).addChildFilter(endTimeFilterBuilder);
    if (existingFilter != null && !existingFilter.equals(Filter.getDefaultInstance())) {
      newFilterBuilder.addChildFilter(existingFilter);
    }

    builder.setFilter(newFilterBuilder);
  }

  public static Filter convertToEntityServiceFilter(
      org.hypertrace.gateway.service.v1.common.Filter filter) {
    if (filter.equals(org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance())) {
      return Filter.getDefaultInstance();
    }
    Filter.Builder builder = Filter.newBuilder();
    builder.setOperator(convertOperator(filter.getOperator()));
    if (filter.getChildFilterCount() > 0) {
      // if there are child filters, handle them recursively.
      for (org.hypertrace.gateway.service.v1.common.Filter child : filter.getChildFilterList()) {
        builder.addChildFilter(convertToEntityServiceFilter(child));
      }
    } else {
      // Copy the lhs and rhs from the filter.
      builder.setLhs(convertToEntityServiceExpression(filter.getLhs()));
      builder.setRhs(convertToEntityServiceExpression(filter.getRhs()));
    }

    return builder.build();
  }

  public static Operator convertOperator(
      org.hypertrace.gateway.service.v1.common.Operator operator) {
    // Might not handle all cases but this is a starter.
    return Operator.valueOf(operator.name());
  }

  public static Expression.Builder convertToEntityServiceExpression(
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

  public static ColumnIdentifier.Builder convertToQueryColumnIdentifier(
      org.hypertrace.gateway.service.v1.common.ColumnIdentifier columnIdentifier) {
    return ColumnIdentifier.newBuilder()
        .setColumnName(columnIdentifier.getColumnName())
        .setAlias(columnIdentifier.getAlias());
  }

  public static LiteralConstant convertToQueryLiteral(
      org.hypertrace.gateway.service.v1.common.LiteralConstant literal) {
    return LiteralConstant.newBuilder()
        .setValue(convertGatewayValueToEntityServiceApiValue(literal.getValue()))
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
      case AVGRATE: {
        builder.setFunctionName(FunctionType.SUM.name()).setAlias(function.getAlias());

        // Adds only the argument that is a column identifier for now.
        List<org.hypertrace.gateway.service.v1.common.Expression> columns =
            function.getArgumentsList().stream()
                .filter(org.hypertrace.gateway.service.v1.common.Expression::hasColumnIdentifier)
                .collect(Collectors.toList());
        columns.forEach(e -> builder.addArguments(convertToEntityServiceExpression(e)));
        break;
      }
      case PERCENTILE: {
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
        columns.forEach(e -> builder.addArguments(convertToEntityServiceExpression(e)));
        break;
      }
      default: {
        builder.setFunctionName(function.getFunction().name()).setAlias(function.getAlias());

        if (function.getArgumentsCount() > 0) {
          function
              .getArgumentsList()
              .forEach(
                  e -> {
                    // Health expressions are treated differently so ignore them.
                    if (!e.hasHealth()) {
                      builder.addArguments(convertToEntityServiceExpression(e));
                    }
                  });
        }
      }
    }
    return builder.build();
  }

  private static OrderByExpression convertToQueryOrderBy(
      org.hypertrace.gateway.service.v1.common.OrderByExpression orderBy) {
    return OrderByExpression.newBuilder()
        .setOrder(SortOrder.valueOf(orderBy.getOrder().name()))
        .setExpression(convertToEntityServiceExpression(orderBy.getExpression()))
        .build();
  }

  public static List<OrderByExpression> convertToOrderByExpressions(
      List<org.hypertrace.gateway.service.v1.common.OrderByExpression> gatewayOrderBy) {
    return gatewayOrderBy.stream()
        .map(EntityServiceAndGatewayServiceConverter::convertToQueryOrderBy)
        .collect(Collectors.toList());
  }

  private static Value convertGatewayValueToEntityServiceApiValue(
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

  public static Expression.Builder createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
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
}
