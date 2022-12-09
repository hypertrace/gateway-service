package org.hypertrace.gateway.service.entity.converter;

import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterResponse;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesResponse;

public class BulkUpdateAllMatchingFilterResponseConverter
    implements Converter<
        BulkUpdateAllMatchingFilterResponse, BulkUpdateAllMatchingEntitiesResponse> {
  @Override
  public BulkUpdateAllMatchingEntitiesResponse convert(
      final BulkUpdateAllMatchingFilterResponse source) {
    return BulkUpdateAllMatchingEntitiesResponse.newBuilder().build();
  }
}
