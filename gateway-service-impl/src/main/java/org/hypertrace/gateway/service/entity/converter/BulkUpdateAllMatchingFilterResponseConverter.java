package org.hypertrace.gateway.service.entity.converter;

import static java.util.stream.Collectors.toUnmodifiableList;

import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterResponse;
import org.hypertrace.entity.query.service.v1.UpdateSummary;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.EntityUpdateSummary;
import org.hypertrace.gateway.service.v1.entity.UpdatedEntity;

public class BulkUpdateAllMatchingFilterResponseConverter
    implements Converter<
        BulkUpdateAllMatchingFilterResponse, BulkUpdateAllMatchingEntitiesResponse> {
  @Override
  public BulkUpdateAllMatchingEntitiesResponse convert(
      final BulkUpdateAllMatchingFilterResponse source) {
    return BulkUpdateAllMatchingEntitiesResponse.newBuilder()
        .addAllSummaries(
            source.getSummariesList().stream().map(this::convert).collect(toUnmodifiableList()))
        .build();
  }

  private EntityUpdateSummary convert(final UpdateSummary summary) {
    return EntityUpdateSummary.newBuilder()
        .addAllUpdatedEntities(
            summary.getUpdatedEntitiesList().stream()
                .map(this::convert)
                .collect(toUnmodifiableList()))
        .build();
  }

  private UpdatedEntity convert(
      final org.hypertrace.entity.query.service.v1.UpdatedEntity updatedEntity) {
    return UpdatedEntity.newBuilder().setId(updatedEntity.getId()).build();
  }
}
