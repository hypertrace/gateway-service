package org.hypertrace.gateway.service.entity.converter;

import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATOR_UNSET;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.Builder;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateOperationConverter
    implements Converter<UpdateOperation, AttributeUpdateOperation> {
  private final Converter<ColumnIdentifier, org.hypertrace.entity.query.service.v1.ColumnIdentifier>
      columnIdentifierConverter;
  private final Converter<Operator, AttributeUpdateOperator> operatorConverter;
  private final Converter<LiteralConstant, org.hypertrace.entity.query.service.v1.LiteralConstant>
      literalConstantConverter;

  @Override
  public AttributeUpdateOperation convert(final UpdateOperation source) {
    final Builder builder =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(columnIdentifierConverter.convert(source.getAttribute()))
            .setOperator(operatorConverter.convert(source.getOperator()));

    if (source.getOperator() != OPERATOR_UNSET) {
      builder.setValue(literalConstantConverter.convert(source.getValue()));
    }

    return builder.build();
  }
}
