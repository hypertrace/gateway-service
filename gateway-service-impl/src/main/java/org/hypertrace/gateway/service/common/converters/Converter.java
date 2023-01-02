package org.hypertrace.gateway.service.common.converters;

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.List;

@FunctionalInterface
public interface Converter<SourceType, TargetType> {
  TargetType convert(final SourceType source);

  default List<TargetType> convert(final List<SourceType> sources) {
    return sources.stream().map(this::convert).collect(toUnmodifiableList());
  }
}
