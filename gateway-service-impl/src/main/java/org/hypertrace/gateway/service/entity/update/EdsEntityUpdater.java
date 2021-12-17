package org.hypertrace.gateway.service.entity.update;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityRequest;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Updater for entities that live in Entity Data Service. */
public class EdsEntityUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(EdsEntityUpdater.class);
  private final EntityQueryServiceClient eqsClient;

  public EdsEntityUpdater(EntityQueryServiceClient eqsClient) {
    this.eqsClient = eqsClient;
  }

  public UpdateEntityResponse.Builder update(
      UpdateEntityRequest updateRequest, UpdateExecutionContext updateExecutionContext) {
    UpdateEntityResponse.Builder responseBuilder = UpdateEntityResponse.newBuilder();

    EntityUpdateRequest eqsUpdateRequest = convertToEqsUpdateRequest(updateRequest);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending update request to EDS  ======== \n {}", eqsUpdateRequest);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        eqsClient.update(eqsUpdateRequest, updateExecutionContext.getRequestHeaders());

    if (!resultSetChunkIterator.hasNext()) {
      return responseBuilder;
    }

    ResultSetChunk chunk = resultSetChunkIterator.next();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received chunk: " + chunk.toString());
    }

    if (chunk.getRowCount() == 0) {
      return responseBuilder;
    }

    if (chunk.getRowCount() > 1) {
      LOG.warn(
          "Received more than 1 row back. Only the first out of {} rows will be returned in the response",
          chunk.getRowCount());
    }
    Map<String, AttributeMetadata> resultAttributeMetadata =
        this.getAttributeMetadataByResultName(updateRequest, updateExecutionContext);
    Row row = chunk.getRow(0);
    Entity.Builder entityBuilder = Entity.newBuilder().setEntityType(updateRequest.getEntityType());

    for (int i = 0; i < chunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
      String resultName = chunk.getResultSetMetadata().getColumnMetadata(i).getColumnName();
      entityBuilder.putAttribute(
          resultName,
          EntityServiceAndGatewayServiceConverter.convertQueryValueToGatewayValue(
              row.getColumn(i), resultAttributeMetadata.get(resultName)));
    }

    responseBuilder.setEntity(entityBuilder);

    return responseBuilder;
  }

  public BulkUpdateEntitiesResponse bulkUpdateEntities(
      BulkUpdateEntitiesRequest request, UpdateExecutionContext updateExecutionContext) {
    MultiValuedAttributeOperation multiValuedAttributeOperation =
        request.getOperation().getMultiValuedAttributeOperation();
    org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest updateRequest =
        org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType(request.getEntityType())
            .addAllEntityIds(request.getEntityIdsList())
            .setOperation(
                EntityServiceAndGatewayServiceConverter
                    .convertToBulkEntityArrayAttributeUpdateOperation(
                        multiValuedAttributeOperation.getType()))
            .setAttribute(
                EntityServiceAndGatewayServiceConverter.convertToQueryColumnIdentifier(
                    multiValuedAttributeOperation.getAttribute()))
            .addAllValues(
                multiValuedAttributeOperation.getValuesList().stream()
                    .map(
                        value ->
                            EntityServiceAndGatewayServiceConverter.convertToQueryLiteral(value))
                    .collect(Collectors.toList()))
            .build();
    eqsClient.bulkUpdateEntityArrayAttribute(
        updateRequest, updateExecutionContext.getRequestHeaders());
    return BulkUpdateEntitiesResponse.newBuilder().build();
  }

  private EntityUpdateRequest convertToEqsUpdateRequest(UpdateEntityRequest updateRequest) {
    EntityUpdateRequest.Builder eqsUpdateRequestBuilder =
        EntityUpdateRequest.newBuilder()
            .setEntityType(updateRequest.getEntityType())
            .addEntityIds(updateRequest.getEntityId());

    // convert operation
    if (updateRequest.getOperation().hasSetAttribute()) {
      eqsUpdateRequestBuilder.setOperation(
          UpdateOperation.newBuilder()
              .setSetAttribute(
                  SetAttribute.newBuilder()
                      .setAttribute(
                          EntityServiceAndGatewayServiceConverter.convertToQueryColumnIdentifier(
                              updateRequest.getOperation().getSetAttribute().getAttribute()))
                      .setValue(
                          EntityServiceAndGatewayServiceConverter.convertToQueryLiteral(
                              updateRequest.getOperation().getSetAttribute().getValue()))));
    }

    // convert selections
    updateRequest.getSelectionList().stream()
        .forEach(
            expression ->
                eqsUpdateRequestBuilder.addSelection(
                    EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression(
                        expression)));

    return eqsUpdateRequestBuilder.build();
  }

  private Map<String, AttributeMetadata> getAttributeMetadataByResultName(
      UpdateEntityRequest updateRequest, UpdateExecutionContext updateExecutionContext) {
    return updateRequest.getSelectionList().stream()
        .filter(
            expression ->
                ExpressionReader.getSelectionAttributeId(expression)
                    .map(updateExecutionContext.getAttributeMetadata()::containsKey)
                    .orElse(false))
        .map(
            expression ->
                Map.entry(
                    ExpressionReader.getSelectionResultName(expression).orElseThrow(),
                    updateExecutionContext
                        .getAttributeMetadata()
                        .get(ExpressionReader.getSelectionAttributeId(expression).orElseThrow())))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue, (x, y) -> x));
  }
}
