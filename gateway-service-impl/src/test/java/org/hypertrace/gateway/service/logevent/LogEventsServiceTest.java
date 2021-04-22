package org.hypertrace.gateway.service.logevent;

import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.Durations;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.log.events.LogEventsRequest;
import org.hypertrace.gateway.service.v1.log.events.LogEventsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LogEventsServiceTest extends AbstractGatewayServiceTest {

  private static final String TENANT_ID = "tenant1";
  private QueryServiceClient queryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;

  @BeforeEach
  public void setup() {
    super.setup();
    queryServiceClient = Mockito.mock(QueryServiceClient.class);
    attributeMetadataProvider = Mockito.mock(AttributeMetadataProvider.class);

    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq("LOG_EVENT"), anyString()))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString("LOG_EVENT")
                    .setKey("timestamp")
                    .setFqn("LOG_EVENT.timestamp")
                    .setId("timestamp")
                    .setValueKind(AttributeKind.TYPE_TIMESTAMP)
                    .setType(AttributeType.ATTRIBUTE)
                    .build()));
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq("LOG_EVENT")))
        .thenReturn(
            Map.of(
                "LOG_EVENT.spanId",
                AttributeMetadata.newBuilder()
                    .setScopeString("LOG_EVENT")
                    .setKey("spanId")
                    .setFqn("LOG_EVENT.spanId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .build(),
                "LOG_EVENT.traceId",
                AttributeMetadata.newBuilder()
                    .setScopeString("LOG_EVENT")
                    .setKey("traceId")
                    .setFqn("LOG_EVENT.traceId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .build(),
                "LOG_EVENT.attributes",
                AttributeMetadata.newBuilder()
                    .setScopeString("LOG_EVENT")
                    .setKey("attributes")
                    .setFqn("LOG_EVENT.attributes")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .build(),
                "LOG_EVENT.timestamp",
                AttributeMetadata.newBuilder()
                    .setScopeString("LOG_EVENT")
                    .setKey("timestamp")
                    .setFqn("LOG_EVENT.timestamp")
                    .setValueKind(AttributeKind.TYPE_TIMESTAMP)
                    .setType(AttributeType.ATTRIBUTE)
                    .build()));
  }

  @Test
  void testGetLogEventsByFilter() throws Exception {
    LogEventsRequest logEventRequest =
        LogEventsRequest.newBuilder()
            .setStartTimeMillis(
                System.currentTimeMillis() - Durations.toMillis(Durations.fromHours(1)))
            .setEndTimeMillis(System.currentTimeMillis())
            .addSelection(getColumnSelectionExpression("LOG_EVENT.spanId"))
            .addSelection(getColumnSelectionExpression("LOG_EVENT.traceId"))
            .addSelection(getColumnSelectionExpression("LOG_EVENT.timestamp"))
            .addSelection(getColumnSelectionExpression("LOG_EVENT.attributes"))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(getColumnSelectionExpression("LOG_EVENT.timestamp"))
                    .setOrder(SortOrder.ASC)
                    .build())
            .setFilter(
                Filter.newBuilder()
                    .setOperatorValue(Operator.EQ_VALUE)
                    .setLhs(getColumnSelectionExpression("LOG_EVENT.traceId"))
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setString("trace-1")
                                            .setValueType(ValueType.STRING)
                                            .build()))
                            .build())
                    .build())
            .setOffset(0)
            .setLimit(5)
            .build();

    LogEventsService logEventsService =
        new LogEventsService(queryServiceClient, 60_000, attributeMetadataProvider);

    String logAttributeString =
        new ObjectMapper()
            .writeValueAsString(
                Map.of(
                    "event", "Acquired lock with 0 transaction waiting",
                    "network", "tcp",
                    "message", "Slow transaction"));
    when(queryServiceClient.executeQuery(any(), any(), Mockito.anyInt()))
        .thenReturn(
            List.of(
                    getResultSetChunk(
                        List.of(
                            "LOG_EVENT.spanId",
                            "LOG_EVENT.traceId",
                            "LOG_EVENT.timestamp",
                            "LOG_EVENT.attributes"),
                        new String[][] {
                          {"span-1", "trace-1", "1000000", logAttributeString},
                          {"span-2", "trace-1", "1000010", logAttributeString},
                          {"span-3", "trace-1", "1000020", logAttributeString},
                        }))
                .iterator());

    LogEventsResponse logEventsResponse =
        logEventsService.getLogEventsByFilter(
            new RequestContext(TENANT_ID, Map.of()), logEventRequest);

    assertEquals(3, logEventsResponse.getLogEventsCount());
    logEventsResponse
        .getLogEventsList()
        .forEach(
            v -> {
              assertEquals(4, v.getAttributesCount());
              assertEquals("trace-1", v.getAttributesMap().get("LOG_EVENT.traceId").getString());
              assertEquals(
                  logAttributeString, v.getAttributesMap().get("LOG_EVENT.attributes").getString());
            });
  }

  private Expression getColumnSelectionExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }
}
