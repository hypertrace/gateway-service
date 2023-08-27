package org.hypertrace.gateway.service.entity.converter;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.entity.Update;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateConverter
    implements Converter<Update, org.hypertrace.entity.query.service.v1.Update> {
  private final Converter<Filter, org.hypertrace.entity.query.service.v1.Filter> filterConverter;
  private final Converter<UpdateOperation, AttributeUpdateOperation> operationConverter;

  @Override
  public org.hypertrace.entity.query.service.v1.Update convert(final Update source) {
    return org.hypertrace.entity.query.service.v1.Update.newBuilder()
        .setFilter(filterConverter.convert(source.getFilter()))
        .addAllOperations(operationConverter.convert(source.getOperationsList()))
        .build();
  }
}
