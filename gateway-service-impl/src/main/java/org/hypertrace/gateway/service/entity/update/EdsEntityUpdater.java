package org.hypertrace.gateway.service.entity.update;

import java.util.Iterator;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.v1.entity.Entity;
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

    Row row = chunk.getRow(0);
    Entity.Builder entityBuilder = Entity.newBuilder().setEntityType(updateRequest.getEntityType());

    for (int i = 0; i < chunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
      ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
      String attributeName = metadata.getColumnName();
      entityBuilder.putAttribute(
          attributeName,
          EntityServiceAndGatewayServiceConverter.convertToGatewayValue(
              attributeName, row.getColumn(i), updateExecutionContext.getAttributeMetadata()));
    }

    responseBuilder.setEntity(entityBuilder);

    return responseBuilder;
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
}
