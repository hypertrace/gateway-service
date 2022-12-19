package org.hypertrace.gateway.service.entity.converter;

import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_UNSET;
import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATOR_ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATOR_REMOVE_FROM_LIST;
import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATOR_SET;
import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATOR_UNSET;

import java.util.Map;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator;

public class UpdateOperatorConverter implements Converter<Operator, AttributeUpdateOperator> {
  private static final Map<Operator, AttributeUpdateOperator> OPERATOR_MAP =
      Map.ofEntries(
          entry(OPERATOR_SET, ATTRIBUTE_UPDATE_OPERATOR_SET),
          entry(OPERATOR_UNSET, ATTRIBUTE_UPDATE_OPERATOR_UNSET),
          entry(OPERATOR_ADD_TO_LIST_IF_ABSENT, ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT),
          entry(OPERATOR_REMOVE_FROM_LIST, ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST));

  @Override
  public AttributeUpdateOperator convert(final Operator source) {
    return requireNonNull(OPERATOR_MAP.get(source));
  }
}
