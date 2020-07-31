package org.hypertrace.gateway.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.EntityInteraction;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for {@link EntityService}
 *
 * See README.md file on how to run this test
 */
public class GatewayServiceIntegrationTest {
  private static final Logger logger =
    LoggerFactory.getLogger(GatewayServiceIntegrationTest.class);
  private static final String ENTITY_GATEWAY_HOST = "localhost";
  private static final int ENTITY_GATEWAY_PORT = 50071;
  private static final String TENANT_ID_VALUE = "test-tenant-id";
  private static GatewayServiceClient client;
  private final Set<Expression> apiSpecAttributes = ImmutableSet.of(
    QueryExpressionUtil.getColumnExpression("API.http.url").build(),
    QueryExpressionUtil.getColumnExpression("API.http.method").build(),
    QueryExpressionUtil.getColumnExpression("API.apiType").build(),
    QueryExpressionUtil.getColumnExpression("API.api_definition").build(),
    QueryExpressionUtil.getColumnExpression("API.http.path_params_type").build(),
    QueryExpressionUtil.getColumnExpression("API.http.query_params_type").build(),
    QueryExpressionUtil.getColumnExpression("API.http.request_headers_type").build(),
    QueryExpressionUtil.getColumnExpression("API.http.response_headers_type").build(),
    QueryExpressionUtil.getColumnExpression("API.request_body_schema").build(),
    QueryExpressionUtil.getColumnExpression("API.response_body_schema").build(),
    QueryExpressionUtil.getColumnExpression("API.API_DEFINITION_FROM").build(),
    QueryExpressionUtil.getColumnExpression("API.IS_EXTERNAL_API").build(),
    QueryExpressionUtil.getColumnExpression("Service.id").build()
  );

  private final Set<Expression> apiAggregateMetrics = ImmutableSet.of(
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_received", FunctionType.SUM,
      "SUM_bytes_received", true).build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_received", FunctionType.AVG,
      "AVG_bytes_received").build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_sent", FunctionType.SUM, "SUM_bytes_sent",
      true).build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_sent", FunctionType.AVG, "AVG_bytes_sent")
      .build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.security.events_count", FunctionType.AVG,
      "AVG_security_event_count").build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.duration_millis", FunctionType.AVG, "AVG_duration")
      .build(),
    QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.duration_millis", FunctionType.PERCENTILE, "P99_duration",
        ImmutableList.of(QueryExpressionUtil.getLiteralExpression(Long.valueOf(99)).build()), false)
    .build());
  
  private final Set<TimeAggregation> apiTimeAggregatedMetrics = ImmutableSet.of(
    TimeAggregation.newBuilder()
      .setPeriod(Period.newBuilder().setValue(60).setUnit(ChronoUnit.SECONDS.name()))
      .setAggregation(QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_received", FunctionType.SUM,
        "SUM_bytes_received", true)).build(), TimeAggregation.newBuilder()
      .setPeriod(Period.newBuilder().setValue(30).setUnit(ChronoUnit.SECONDS.name()))
      .setAggregation(
        QueryExpressionUtil.getAggregateFunctionExpression("API.metrics.bytes_sent", FunctionType.SUM, "SUM_bytes_sent",
          true)).build());
  private final Set<Expression> serviceAggregateMetrics = ImmutableSet.of(
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_received", FunctionType.SUM,
      "SUM_bytes_received", true).build(),
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_received", FunctionType.AVG,
      "AVG_bytes_received").build(),
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_sent", FunctionType.SUM, "SUM_bytes_sent",
      true).build(),
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_sent", FunctionType.AVG, "AVG_bytes_sent")
      .build(),
    // Average bytes sent per second for this service
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_received", FunctionType.AVGRATE,
      "AVGRATE_bytes_sent",
      ImmutableList.of(QueryExpressionUtil.getLiteralExpression(TimeUnit.SECONDS.toSeconds(1)).build()), true).build(),
    //Average bytes sent per minute
    QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_sent", FunctionType.AVGRATE,
      "AVGRATE_bytes_sent_60",
      ImmutableList.of(QueryExpressionUtil.getLiteralExpression(TimeUnit.MINUTES.toSeconds(1)).build()), false)
      .build());
  private final Set<Expression> domainAggregateMetrics = ImmutableSet.of(
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_received", FunctionType.SUM,
          "SUM_bytes_received", true).build(),
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_received", FunctionType.AVG,
          "AVG_bytes_received").build(),
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_sent", FunctionType.SUM, "SUM_bytes_sent",
          true).build(),
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_sent", FunctionType.AVG, "AVG_bytes_sent")
          .build(),
      // Average bytes sent per second for this service
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_received", FunctionType.AVGRATE,
          "AVGRATE_bytes_sent",
          ImmutableList.of(QueryExpressionUtil.getLiteralExpression(TimeUnit.SECONDS.toSeconds(1)).build()), true).build(),
      //Average bytes sent per minute
      QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_sent", FunctionType.AVGRATE,
          "AVGRATE_bytes_sent_60",
          ImmutableList.of(QueryExpressionUtil.getLiteralExpression(TimeUnit.MINUTES.toSeconds(1)).build()), false)
          .build());
  private final Set<TimeAggregation> serviceTimeAggregatedMetrics = ImmutableSet.of(
    TimeAggregation.newBuilder()
      .setPeriod(Period.newBuilder().setValue(30).setUnit(ChronoUnit.SECONDS.name()))
      .setAggregation(
        QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_received", FunctionType.SUM,
          "SUM_bytes_received")).build(), TimeAggregation.newBuilder()
      .setPeriod(Period.newBuilder().setValue(60).setUnit(ChronoUnit.SECONDS.name()))
      .setAggregation(QueryExpressionUtil.getAggregateFunctionExpression("Service.metrics.bytes_sent", FunctionType.SUM,
        "SUM_bytes_sent")).build());
  private final Set<TimeAggregation> domainTimeAggregatedMetrics = ImmutableSet.of(
      TimeAggregation.newBuilder()
          .setPeriod(Period.newBuilder().setValue(30).setUnit(ChronoUnit.SECONDS.name()))
          .setAggregation(
              QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_received", FunctionType.SUM,
                  "SUM_bytes_received")).build(), TimeAggregation.newBuilder()
          .setPeriod(Period.newBuilder().setValue(60).setUnit(ChronoUnit.SECONDS.name()))
          .setAggregation(QueryExpressionUtil.getAggregateFunctionExpression("Domain.metrics.bytes_sent", FunctionType.SUM,
              "SUM_bytes_sent")).build());
  private final InteractionsRequest fromServiceInteractions = InteractionsRequest.newBuilder()
    .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.from_entity_type", "Service"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.from_entity_type"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.from_entity_id"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.to_entity_type"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.to_entity_id")).addSelection(
      QueryExpressionUtil.getAggregateFunctionExpression("Interaction.metrics.bytes_received", FunctionType.SUM,
        "SUM_bytes_received")).build();
  private final InteractionsRequest toServiceInteractions = InteractionsRequest.newBuilder()
    .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.to_entity_type", "Service"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.to_entity_type"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.to_entity_id")).addSelection(
      QueryExpressionUtil.getAggregateFunctionExpression("Interaction.metrics.bytes_sent", FunctionType.SUM,
        "SUM_bytes_sent")).build();
  private final InteractionsRequest toApiInteractions = InteractionsRequest.newBuilder()
    .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.to_entity_type", "Api"))
    .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.from_entity_id")).addSelection(
      QueryExpressionUtil.getAggregateFunctionExpression("Interaction.metrics.bytes_sent", FunctionType.SUM,
        "SUM_bytes_sent")).build();
  private final InteractionsRequest toApiInteractions_Avgrate = InteractionsRequest.newBuilder()
        .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.to_entity_type", "API"))
        .addSelection(QueryExpressionUtil.getAggregateFunctionExpression(
            "Interaction.metrics.bytes_sent", FunctionType.AVGRATE,
            "AVG_RATE_bytes_sent"))
        .build();
  private final InteractionsRequest toApiInteractions_p99 = InteractionsRequest.newBuilder()
        .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.to_entity_type", "API"))
        .addSelection(QueryExpressionUtil.getAggregateFunctionExpression(
            "Interaction.metrics.bytes_sent", FunctionType.PERCENTILE,
            "p99bytes_sent", ImmutableList.of(QueryExpressionUtil.getLiteralExpression(Long.valueOf(99)).build()),
            false))
        .build();

  @BeforeAll
  public static void setUp() {
 //   IntegrationTestServerUtil.startServices(new String[] {"gateway-service"});
    Channel channel = ManagedChannelBuilder.forAddress(
        ENTITY_GATEWAY_HOST, ENTITY_GATEWAY_PORT).usePlaintext().build();
    client = new GatewayServiceClient(channel);
  }

  @AfterAll
  public static void teardown() {
//    IntegrationTestServerUtil.shutdownServices();
  }

//  private Set<EntitiesRequest> getServiceRequests() {
//    return ImmutableSet.of(
//      // all services
//      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
//        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//        .setEndTimeMillis(System.currentTimeMillis())
//        .addSelection(QueryExpressionUtil.getColumnExpression("Service.name")).addAllSelection(serviceAggregateMetrics)
//        .addOrderBy(
//          QueryExpressionUtil.getOrderBy("Service.metrics.bytes_received", FunctionType.SUM, "SUM_bytes_received",
//            SortOrder.DESC)).setIncomingInteractions(fromServiceInteractions)
//        .setOutgoingInteractions(toServiceInteractions).setLimit(2).setOffset(2).build(),
//      // Attribute filter
//      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
//        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//        .setEndTimeMillis(System.currentTimeMillis())
//        .setFilter(QueryExpressionUtil.getSimpleFilter("Service.name", "loginservice"))
//        .addSelection(QueryExpressionUtil.getColumnExpression("Service.attributes.namespace"))
//        .addAllSelection(serviceAggregateMetrics)
//        .addAllTimeAggregation(serviceTimeAggregatedMetrics).addOrderBy(
//        QueryExpressionUtil.getOrderBy("Service.metrics.bytes_received", FunctionType.SUM, "SUM_bytes_received",
//          SortOrder.ASC)).setIncomingInteractions(fromServiceInteractions).build(),
//      // Id and attribute filters together.
//      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
//        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//        .setEndTimeMillis(System.currentTimeMillis())
//        .addSelection(QueryExpressionUtil.getColumnExpression("Service.id"))
//        .addSelection(QueryExpressionUtil.getColumnExpression("Service.name"))
//        .setFilter(QueryExpressionUtil.getSimpleFilter("Service.name", "loginservice"))
//        .addAllSelection(serviceAggregateMetrics)
//        .addAllTimeAggregation(serviceTimeAggregatedMetrics)
//        .setIncomingInteractions(fromServiceInteractions)
//        .setOutgoingInteractions(toServiceInteractions).build());
//  }
//
//  @Test
//  public void testGetServices() {
//    for (EntitiesRequest request : getServiceRequests()) {
//      EntitiesResponse response = client.getBlockingStub().getEntities(request);
//      printEntities(response);
//      verifyResponse(request, response);
//    }
//  }
//
//  @Test public void testGetServiceMetricAndMetricSeries() {
//    EntitiesRequest request =
//      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
//        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//        .setEndTimeMillis(System.currentTimeMillis())
//        .setFilter(QueryExpressionUtil.getSimpleFilter("Service.name", "loginservice"))
//        .addSelection(QueryExpressionUtil.getColumnExpression("Service.name")).addAllSelection(serviceAggregateMetrics)
//        .addAllTimeAggregation(serviceTimeAggregatedMetrics).addOrderBy(
//        QueryExpressionUtil.getOrderBy("Service.metrics.bytes_received", FunctionType.SUM, "SUM_bytes_received",
//          SortOrder.ASC)).setIncomingInteractions(fromServiceInteractions).build();
//
//    EntitiesResponse response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
//  }

  @Test
  public void testGetApis() {
    // get all APIs
    final EntitiesRequest request =
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
        .setEndTimeMillis(System.currentTimeMillis()).addAllSelection(apiAggregateMetrics)
        .addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
        .addSelection(QueryExpressionUtil.getColumnExpression("Service.name"))
        .setFilter(QueryExpressionUtil.getLikeFilter("Service.name", "front"))
        .addOrderBy(QueryExpressionUtil.getOrderBy("Service.name", SortOrder.DESC)).addOrderBy(
        QueryExpressionUtil.getOrderBy("API.metrics.bytes_received", FunctionType.SUM, "SUM_bytes_received",
          SortOrder.ASC)).setIncomingInteractions(fromServiceInteractions)
        .setOutgoingInteractions(toApiInteractions).build();

    logger.info("Query: " + request);
    EntitiesResponse response = GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID_VALUE,
        () -> client.getBlockingStub().getEntities(request));
    printEntities(response);


    int limit = 8;
    final EntitiesRequest topApiRequest = EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
        .setEndTimeMillis(System.currentTimeMillis())
        .setFilter(QueryExpressionUtil.getSimpleFilter("API.apiDiscoveryState", "DISCOVERED"))
        .addSelection(QueryExpressionUtil
            .getAggregateFunctionExpression("API.numCalls", FunctionType.SUM, "SUM_API.numCalls_[]"))
        .addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
        .addSelection(QueryExpressionUtil.getColumnExpression("API.name"))
        .addOrderBy(QueryExpressionUtil.getOrderBy("API.numCalls", SortOrder.DESC))
        // Use pagination too here.
        .setOffset(0).setLimit(limit).build();
    logger.info("Query: " + topApiRequest);
    response = GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID_VALUE,
        () -> client.getBlockingStub().getEntities(topApiRequest));
    printEntities(response);
//    verifyResponse(request, response);

   
//    request = EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
//      .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
//      .setEndTimeMillis(System.currentTimeMillis())
//      .setFilter(QueryExpressionUtil.getSimpleFilter("Service.name", "frontend"))
//      .addAllTimeAggregation(apiTimeAggregatedMetrics).addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
//      .addSelection(QueryExpressionUtil.getColumnExpression("Service.name"))
//      .addOrderBy(QueryExpressionUtil.getOrderBy("Service.name", SortOrder.DESC))
//      .setIncomingInteractions(fromServiceInteractions).setOutgoingInteractions(toApiInteractions_Avgrate)
//      // Use pagination too here.
//      .setOffset(0).setLimit(limit).build();
//    logger.info("Query: " + request);
//    response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
//
//
//    request = EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
//      .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
//      .setEndTimeMillis(System.currentTimeMillis())
//      .setFilter(QueryExpressionUtil.getSimpleFilter("Service.name", "frontend"))
//      .addAllTimeAggregation(apiTimeAggregatedMetrics).addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
//      .addSelection(QueryExpressionUtil.getColumnExpression("Service.name"))
//      .addOrderBy(QueryExpressionUtil.getOrderBy("Service.name", SortOrder.DESC))
//      .setIncomingInteractions(fromServiceInteractions).setOutgoingInteractions(toApiInteractions_p99)
//      // Use pagination too here.
//      .setOffset(0).setLimit(limit).build();
//    logger.info("Query: " + request);
//    response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
//
//    request = EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
//      .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
//      .setEndTimeMillis(System.currentTimeMillis())
//      .addAllSelection(apiSpecAttributes)
//      .setFilter(QueryExpressionUtil.getLikeFilter("API.http.url", "grpc"))
//      .build();
//
//    logger.info("Query: " + request);
//    response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
  }

//  @Test
//  public void testDuplicateColumns() {
//    // get some duplicate columns
//    // API.domainName -> Service.host_header
//    // API.domainId -> Service.host_header where API.is_external = true
//    // Service.host_header becomes the duplicate column after derived entity transformation
//    EntitiesRequest request =
//        EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
//            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//            .setEndTimeMillis(System.currentTimeMillis())
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("Service.id"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("Service.name"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.domainName"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.name"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("API.domainId"))
//            .setFilter(QueryExpressionUtil.getSimpleFilter("API.id", "2348c9c2-40ba-3b0f-a02e-71f591551af3"))
//            .build();
//
//    logger.info("Query: " + request);
//    EntitiesResponse response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
//  }
//
//  @Test
//  public void testGetDomains() {
//    // get all Domains
//    EntitiesRequest request =
//        EntitiesRequest.newBuilder().setEntityType(DomainEntityType.DOMAIN.name())
//            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//            .setEndTimeMillis(System.currentTimeMillis())
//            .addSelection(QueryExpressionUtil.getColumnExpression("Domain.id"))
//            .addSelection(QueryExpressionUtil.getColumnExpression("Domain.name"))
//            .addAllSelection(domainAggregateMetrics)
//            .addAllTimeAggregation(domainTimeAggregatedMetrics)
//            .addOrderBy(QueryExpressionUtil.getOrderBy("Domain.metrics.duration_millis", FunctionType.AVG, "", SortOrder.ASC))
//            .build();
//
//    logger.info("Query: " + request);
//    EntitiesResponse response = client.getBlockingStub().getEntities(request);
//    printEntities(response);
//    verifyResponse(request, response);
//  }
//
//  @Test
//  public void testGetTraces() {
//    TracesRequest request =
//        TracesRequest.newBuilder()
//            .setScope(AttributeScope.API_TRACE.name())
//            .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))
//            .setEndTimeMillis(System.currentTimeMillis())
//            .addSelection(Expression.newBuilder()
//                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Service.name")))
//            .setFilter(QueryExpressionUtil.getSimpleFilter("Api.Trace.domainId", "c56e15ab-a271-3096-b5d0-c4b4dce9a9ab:::true"))
//            .build();
//
//    logger.info("Query: " + request);
//    TracesResponse response = client.getBlockingStub().getTraces(request);
//    printTraces(response);
//    verifyResponse(request, response);
//  }
  
  @Test
  public void testGetApisWithNonExistentAttributes() {
    // get all APIs
    EntitiesRequest request =
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(QueryExpressionUtil.getColumnExpression("API.nonExistentAttribute"))
        .addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
        .setIncomingInteractions(fromServiceInteractions)
        .setOutgoingInteractions(toApiInteractions).build();

    logger.info("Query: " + request);
    Assertions.assertThrows(StatusRuntimeException.class, () -> {
      EntitiesResponse response = client.getBlockingStub().getEntities(request);
      logger.info(response.toString());
    });
  }

  private Set<EntitiesRequest> getBasicQueries() {
    Filter startTimeFilter = QueryExpressionUtil.createTimeFilter("Service.start_time_millis", Operator.GE,
      System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3));
    Filter endTimeFilter =
      QueryExpressionUtil.createTimeFilter("Service.start_time_millis", Operator.LT, System.currentTimeMillis());
    return ImmutableSet.of(EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis()).addSelection(QueryExpressionUtil.getColumnExpression("API.id"))
        .setOutgoingInteractions(InteractionsRequest.newBuilder()
          .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.to_entity_type", "API"))
          .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.to_entity_id")).build()).build(),
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(QueryExpressionUtil.getColumnExpression("Service.id")).setFilter(
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(startTimeFilter)
          .addChildFilter(endTimeFilter).build()).setIncomingInteractions(
        InteractionsRequest.newBuilder()
          .setFilter(QueryExpressionUtil.getSimpleFilter("Interaction.attributes.from_entity_type", "Service"))
          .addSelection(QueryExpressionUtil.getColumnExpression("Interaction.attributes.from_entity_id")).build())
        .build());
  }

  @Test public void testBasicQueries() {
    for (EntitiesRequest request : getBasicQueries()) {
      System.out.println(request.toString());
      EntitiesResponse response = client.getBlockingStub().getEntities(request);
      printEntities(response);
      verifyResponse(request, response);
    }
  }

  private Set<EntitiesRequest> getNegativeQueries() {
    return ImmutableSet.of(
      // Empty request
      EntitiesRequest.newBuilder().build(),
      // Request without selecting any attribute, not even id.
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.API.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis()).build(),
      // Request without timestamps
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name()).build(),
      // Request without selections in the interactions
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(QueryExpressionUtil.getColumnExpression("Service.id")).setIncomingInteractions(
        InteractionsRequest.newBuilder().setFilter(QueryExpressionUtil.getSimpleFilter("foo", "bar")).build()).build(),
      // Request without filter in the interactions
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.SERVICE.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(QueryExpressionUtil.getColumnExpression("Service.id")).setOutgoingInteractions(
        InteractionsRequest.newBuilder().addSelection(QueryExpressionUtil.getColumnExpression("test")).build())
        .build());
  }

  @Test public void testNegativeQueries() {
    for (EntitiesRequest request : getNegativeQueries()) {
      try {
        EntitiesResponse response = client.getBlockingStub().getEntities(request);
        printEntities(response);
        Assertions.fail("The request should have failed. Request: " + request);
      } catch (StatusRuntimeException ex) {
        // We expect UNKNOWN as the status code in exception.
        assertEquals(ex.getMessage(), ex.getStatus().getCode().name(), "UNKNOWN");
      }
    }
  }

  private void printEntities(EntitiesResponse response) {
    System.out.println(
      "Result:\n============\nTotal=" + response.getTotal() + ",Size=" + response.getEntityCount()
        + "\n");
    if (response.getEntityCount() > 0) {
      response.getEntityList().forEach(e -> logger.info("Entity: " + e));
    } else {
      logger.info("[]");
    }
  }

  private void printTraces(TracesResponse response) {
    System.out.println(
        "Result:\n============\nTotal=" + response.getTotal() + ",Size=" + response.getTracesCount()
            + "\n");
    if (response.getTracesCount() > 0) {
      response.getTracesList().forEach(e -> logger.info("Trace: " + e));
    } else {
      logger.info("[]");
    }
  }

  private void verifyResponse(EntitiesRequest request, EntitiesResponse response) {
    for (Entity entity : response.getEntityList()) {
      // Assert that all attributes requested are returned
      // We expect id to be returned always so handle that separately
      boolean idPresent = request.getSelectionList().stream()
          .anyMatch(e -> e.getColumnIdentifier().getColumnName().endsWith(".id"));
      assertEquals(getUniqueSelectionCount(request.getSelectionList()) + (idPresent ? 0 : 1),
        entity.getAttributeCount() + entity.getMetricCount(), "Attribute/Aggregation count mismatch");
      assertEquals(request.getTimeAggregationCount(),
        entity.getMetricSeriesCount(), "Time aggregation count mismatch");

      for (EntityInteraction interaction : entity.getIncomingInteractionList()) {
        verifyEntityInteraction(request.getIncomingInteractions(), interaction);
      }

      for (EntityInteraction interaction : entity.getOutgoingInteractionList()) {
        verifyEntityInteraction(request.getOutgoingInteractions(), interaction);
      }
    }
  }

  private int getUniqueSelectionCount(List<Expression> expressions) {
    Set<String> columnNames = new HashSet<>();
    expressions.forEach(expression -> {
      if (expression.getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER) {
        columnNames.add(expression.getColumnIdentifier().getColumnName());
      } else if (expression.getValueCase() == Expression.ValueCase.FUNCTION) {
        columnNames.add(expression.getFunction().getAlias());
      }
    });

    return columnNames.size();
  }

  private void verifyResponse(TracesRequest request, TracesResponse response) {
    for (Trace trace : response.getTracesList()) {
      // Assert that all attributes requested are returned
      // We expect id to be returned always so handle that separately
      boolean idPresent = request.getSelectionList().stream()
          .anyMatch(e -> e.getColumnIdentifier().getColumnName().endsWith(".id"));
      assertEquals(request.getSelectionCount() + (idPresent ? 0 : 1),
          trace.getAttributesCount(), "Attribute/Aggregation count mismatch");
    }
  }

  private void verifyEntityInteraction(InteractionsRequest request, EntityInteraction interaction) {
    assertNotEquals("null",
      interaction.getAttributeMap().get("Interaction.attributes.to_entity_id"));
    assertNotEquals("null",
      interaction.getAttributeMap().get("Interaction.attributes.from_entity_id"));
    assertEquals(request.getSelectionCount(),
      interaction.getAttributeCount() + interaction.getMetricsCount(), "Interaction attribute/aggregation count mismatch");
  }


  private Set<EntitiesRequest> getBackendRequests() {
    return ImmutableSet.of(
      // all services
      EntitiesRequest.newBuilder().setEntityType(DomainEntityType.BACKEND.name())
        .setStartTimeMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
        .setEndTimeMillis(System.currentTimeMillis())
        .addSelection(QueryExpressionUtil.getColumnExpression("Backend.protocol")).build());
  }

  @Test public void testGetBackends() {
    for (EntitiesRequest request : getBackendRequests()) {
      EntitiesResponse response = client.getBlockingStub().getEntities(request);
      printEntities(response);
      verifyResponse(request, response);
    }
  }

}
