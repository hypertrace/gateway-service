package org.hypertrace.gateway.service.common.transformer;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.DomainObjectMapping;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;

// TODO: Nuke class
/**
 * Post processes the {@link EntitiesResponse} before sending the response
 *
 * <p>Takes care of the following - removing any additional fields fetched - Remapping the Domain
 * Object's composite Id Attributes to a single Id Attribute
 */
public class ResponsePostProcessor {

  private final AttributeMetadataProvider attributeMetadataProvider;

  public ResponsePostProcessor(AttributeMetadataProvider attributeMetadataProvider) {
    this.attributeMetadataProvider = attributeMetadataProvider;
  }

  /**
   * This is called once the response is almost ready after applying the limits, etc. Any post
   * processing like removing additional fields, adding additional annotations, etc can be done
   * here.
   *
   * @param request The original request received.
   * @param responseBuilder Final response that is about to be sent.
   */
//  public EntitiesResponse.Builder transform(
//      EntitiesRequest request,
//      EntitiesRequestContext entitiesRequestContext,
//      EntitiesResponse.Builder responseBuilder) {
//    List<Builder> entityBuilders = responseBuilder.getEntityBuilderList();
//
//    Map<String, List<DomainObjectMapping>> attributeIdMappings =
//        AttributeMetadataUtil.getAttributeIdMappings(
//            attributeMetadataProvider, entitiesRequestContext, request.getEntityType());
//
//    // Get all selected columns that were transformed and reconstruct a single column from the
//    // mapped columns
//    Set<String> selectedColumns = getSelectedColumns(request.getSelectionList());
//
//    Set<String> transformedColumns =
//        Sets.intersection(selectedColumns, attributeIdMappings.keySet());
//    for (String column : transformedColumns) {
//      entityBuilders.forEach(
//          entity ->
//              entity.putAttribute(
//                  column,
//                  buildEntityKeyValueForTransformedColumn(
//                      attributeIdMappings,
//                      column,
//                      entity.getAttributeMap(),
//                      entitiesRequestContext)));
//    }
//
//    // Remove any other attribute columns that were not part of the selection, but are present in
//    // the
//    // response because they were added by the PreProcessor
//    Set<String> transformedColumnSelections =
//        getTransformedColumns(attributeIdMappings, transformedColumns, entitiesRequestContext);
//    Set<String> attributesToRemove = Sets.difference(transformedColumnSelections, selectedColumns);
//    entityBuilders.forEach(entity -> attributesToRemove.forEach(entity::removeAttribute));
//
//    return responseBuilder;
//  }

  /**
   * This is called once the response is almost ready after applying the limits, etc. Any post
   * processing like removing additional fields, adding additional annotations, etc can be done
   * here.
   *
   * @param request The original request received.
   * @param responseBuilder Final response that is about to be sent.
   */
//  public TracesResponse.Builder transform(
//      TracesRequest request,
//      RequestContext requestContext,
//      TracesResponse.Builder responseBuilder) {
//    List<Trace.Builder> tracesBuilders = responseBuilder.getTracesBuilderList();
//
//    Map<String, List<DomainObjectMapping>> attributeIdMappings =
//        AttributeMetadataUtil.getAttributeIdMappings(
//            attributeMetadataProvider, requestContext, request.getScope());
//
//    // Get all selected columns that were transformed and reconstruct a single column from the
//    // mapped columns
//    Set<String> selectedColumns = getSelectedColumns(request.getSelectionList());
//
//    Set<String> transformedColumns =
//        Sets.intersection(selectedColumns, attributeIdMappings.keySet());
//    for (String column : transformedColumns) {
//      tracesBuilders.forEach(
//          trace ->
//              trace.putAttributes(
//                  column,
//                  buildEntityKeyValueForTransformedColumn(
//                      attributeIdMappings, column, trace.getAttributesMap(), requestContext)));
//    }
//
//    // Remove any other attribute columns that were not part of the selection, but are present in
//    // the
//    // response because they were added by the PreProcessor
//    Set<String> transformedColumnSelections =
//        getTransformedColumns(attributeIdMappings, transformedColumns, requestContext);
//    Set<String> attributesToRemove = Sets.difference(transformedColumnSelections, selectedColumns);
//    tracesBuilders.forEach(trace -> attributesToRemove.forEach(trace::removeAttributes));
//
//    return responseBuilder;
//  }

  private Set<String> getSelectedColumns(List<Expression> selections) {
    return selections.stream()
        .filter(expression -> expression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
        .map(e -> e.getColumnIdentifier().getColumnName())
        .collect(Collectors.toSet());
  }

  private Value buildEntityKeyValueForTransformedColumn(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      String column,
      Map<String, Value> attributesMap,
      RequestContext requestContext) {
    return Value.newBuilder()
        .setValueType(ValueType.STRING)
        .setString(
            EntityKey.of(
                    attributeIdMappings.get(column).stream()
                        .map(
                            col ->
                                attributesMap.get(
                                    AttributeMetadataUtil.getAttributeMetadata(
                                            attributeMetadataProvider, requestContext, col)
                                        .getId()))
                        .map(this::getValue)
                        .toArray(String[]::new))
                .toString())
        .build();
  }

  private Set<String> getTransformedColumns(
      Map<String, List<DomainObjectMapping>> attributeIdMappings,
      Set<String> transformedColumns,
      RequestContext requestContext) {
    return transformedColumns.stream()
        .flatMap(col -> attributeIdMappings.get(col).stream())
        .map(
            col ->
                AttributeMetadataUtil.getAttributeMetadata(
                        attributeMetadataProvider, requestContext, col)
                    .getId())
        .collect(Collectors.toSet());
  }

  @Nullable
  private String getValue(Value value) {
    ValueType valueType = value.getValueType();
    switch (valueType) {
      case UNRECOGNIZED:
      case UNSET:
        return null;
      case BOOL:
        return String.valueOf(value.getBoolean());
      case STRING:
        return value.getString();
      case LONG:
        return String.valueOf(value.getLong());
      case DOUBLE:
        return String.valueOf(value.getDouble());
      case TIMESTAMP:
        return String.valueOf(value.getTimestamp());
      case BOOLEAN_ARRAY:
        return String.valueOf(value.getBooleanArrayList());
      case STRING_ARRAY:
        return String.valueOf(value.getStringArrayList());
      case LONG_ARRAY:
        return String.valueOf(value.getLongArrayList());
      case DOUBLE_ARRAY:
        return String.valueOf(value.getDoubleArrayList());
      case STRING_MAP:
        return String.valueOf(value.getStringMapMap());
    }

    return null;
  }
}
