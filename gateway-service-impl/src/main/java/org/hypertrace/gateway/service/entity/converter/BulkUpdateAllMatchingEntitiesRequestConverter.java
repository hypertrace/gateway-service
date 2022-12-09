package org.hypertrace.gateway.service.entity.converter;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterRequest;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Update;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class BulkUpdateAllMatchingEntitiesRequestConverter
    implements Converter<BulkUpdateAllMatchingEntitiesRequest, BulkUpdateAllMatchingFilterRequest> {
  private final Converter<Update, org.hypertrace.entity.query.service.v1.Update> updateConverter;

  @Override
  public BulkUpdateAllMatchingFilterRequest convert(
      final BulkUpdateAllMatchingEntitiesRequest source) {
    return BulkUpdateAllMatchingFilterRequest.newBuilder()
        .setEntityType(source.getEntityType())
        .addAllUpdates(updateConverter.convert(source.getUpdatesList()))
        .build();
  }
}
