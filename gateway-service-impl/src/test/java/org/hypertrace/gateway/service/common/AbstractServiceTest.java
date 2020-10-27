package org.hypertrace.gateway.service.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

public abstract class AbstractServiceTest<
    TGatewayServiceRequestType extends GeneratedMessageV3,
    TGatewayServiceResponseType extends GeneratedMessageV3> {

  protected static final String TENANT_ID = "tenant1";

  private static final String GATEWAY_SERVICE_TEST_REQUESTS_DIR = "test-requests";
  private static final String EXPECTED_QUERY_REQUEST_AND_RESPONSE_DIR =
      "query-service-requests-and-responses";
  private static final String GATEWAY_SERVICE_EXPECTED_TEST_RESPONSES_DIR =
      "expected-test-responses";
  private static final String ATTRIBUTES_FILE_PATH = "attributes/attributes.json";

  private static AttributeMetadataProvider attributeMetadataProvider;
  private static List<AttributeMetadata> attributeMetadataList;
  private static ScopeFilterConfigs scopeFilterConfigs;

  @BeforeAll
  public static void setUp() throws IOException {
    createMockAttributeMetadataProvider();
    String scopeFiltersConfig =
        "scopeFiltersConfig = [\n"
            + "  {\n"
            + "    scope = API_TRACE\n"
            + "    filters = ["
            + "      {\n"
            + "         scope = API_TRACE\n"
            + "         key = apiBoundaryType\n"
            + "         op = EQ\n"
            + "         value = ENTRY\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config config = ConfigFactory.parseString(scopeFiltersConfig);
    scopeFilterConfigs = new ScopeFilterConfigs(config);
  }

  private static Reader readResourceFile(String fileName) {
    InputStream in =
        (new RequestContext(TENANT_ID, Map.of()))
            .getClass()
            .getClassLoader()
            .getResourceAsStream(fileName);
    return new BufferedReader(new InputStreamReader(in));
  }

  private static void createMockAttributeMetadataProvider() throws IOException {
    attributeMetadataList = getAttributes();

    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);

    when(attributesServiceClient.findAttributes(
        any(new HashMap<String, String>().getClass()), any(AttributeMetadataFilter.class)))
        .thenAnswer(
            (Answer<Iterator<AttributeMetadata>>)
                invocation -> {
                  AttributeMetadataFilter filter =
                      (AttributeMetadataFilter) invocation.getArguments()[1];
                  Set<String> scopes = new HashSet<>(filter.getScopeStringList());
                  return attributeMetadataList.stream()
                      .filter(a -> scopes.contains(a.getScopeString()))
                      .collect(Collectors.toList())
                      .iterator();
                });

    attributeMetadataProvider = new AttributeMetadataProvider(attributesServiceClient);
  }

  private static List<AttributeMetadata> getAttributes() throws IOException {
    List<AttributeMetadata> attributeMetadataList = new ArrayList<>();
    Reader attributesReader = readResourceFile(ATTRIBUTES_FILE_PATH);

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode attributesJsonNode = objectMapper.readTree(attributesReader);
    attributesJsonNode
        .elements()
        .forEachRemaining(
            attributeJsonNode -> {
              AttributeMetadata.Builder builder = AttributeMetadata.newBuilder();

              try {
                JsonFormat.parser().merge(attributeJsonNode.toString(), builder);
              } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
              }

              // Set the id explicitly here.
              builder.setId(builder.getScopeString() + "." + builder.getKey());

              attributeMetadataList.add(builder.build());
            });

    return attributeMetadataList;
  }

  protected static Stream<String> getTestFileNames(String suiteName) {
    // To read the resource file in a static context, we will create a new temporary object in the
    // gateway service package
    // and use it's class loader to get the Resource.
    String folderName =
        (new RequestContext(TENANT_ID, Map.of()))
            .getClass()
            .getClassLoader()
            .getResource(String.format("%s/%s", GATEWAY_SERVICE_TEST_REQUESTS_DIR, suiteName))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(queriesFolder.listFiles()).map(file -> file.getName().split("\\.")[0]);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void runTest(String fileName) throws IOException {
    QueryServiceClient queryServiceClient = createMockQueryServiceClient(fileName);
    TGatewayServiceRequestType testRequest = readGatewayServiceRequest(fileName);
    TGatewayServiceResponseType actualResponse =
        executeApi(testRequest, queryServiceClient, attributeMetadataProvider, scopeFilterConfigs);
    TGatewayServiceResponseType expectedResponse = readGatewayServiceResponse(fileName);

    Assertions.assertEquals(expectedResponse, actualResponse);
  }

  // testName is "<test service folder>/<test name>". eg. "explore/simple-selection" or
  // "entities/simple-aggregations"
  private Map<QueryRequest, ResultSetChunk> readExpectedQueryServiceRequestAndResponse(
      String fileName) throws IOException {
    Map<QueryRequest, ResultSetChunk> queryServiceRequestResponseMap = new HashMap<>();
    String resourceFileName =
        createResourceFileName(EXPECTED_QUERY_REQUEST_AND_RESPONSE_DIR, fileName);
    Reader requestsAndResponsesReader = readResourceFile(resourceFileName);

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode requestsAndResponses = objectMapper.readTree(requestsAndResponsesReader);
    requestsAndResponses
        .elements()
        .forEachRemaining(
            (element) -> {
              JsonNode requestNode = element.findValue("request");
              JsonNode responseNode = element.findValue("response");

              QueryRequest.Builder queryRequestBuilder = QueryRequest.newBuilder();
              ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
              try {
                JsonFormat.parser().merge(requestNode.toString(), queryRequestBuilder);
                JsonFormat.parser().merge(responseNode.toString(), resultSetChunkBuilder);
              } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
              }

              queryServiceRequestResponseMap.put(
                  queryRequestBuilder.build(), resultSetChunkBuilder.build());
            });

    return queryServiceRequestResponseMap;
  }

  private TGatewayServiceRequestType readGatewayServiceRequest(String fileName) {
    String resourceFileName =
        createResourceFileName(
            GATEWAY_SERVICE_TEST_REQUESTS_DIR,
            fileName); // GATEWAY_SERVICE_TEST_REQUESTS_DIR + FILE_SEPARATOR + getTestSuiteName() +
                       // FILE_SEPARATOR + fileName + EXT_SEPARATOR + JSON_EXTENSION;
    String requestJsonStr = readResourceFileAsString(resourceFileName);

    GeneratedMessageV3.Builder requestBuilder = getGatewayServiceRequestBuilder();
    try {
      JsonFormat.parser().merge(requestJsonStr, requestBuilder);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    return (TGatewayServiceRequestType) requestBuilder.build();
  }

  private TGatewayServiceResponseType readGatewayServiceResponse(String fileName) {
    String resourceFileName =
        createResourceFileName(
            GATEWAY_SERVICE_EXPECTED_TEST_RESPONSES_DIR,
            fileName); // GATEWAY_SERVICE_EXPECTED_TEST_RESPONSES_DIR + FILE_SEPARATOR +
                       // getTestSuiteName() + FILE_SEPARATOR + fileName + EXT_SEPARATOR +
                       // JSON_EXTENSION;
    String requestJsonStr = readResourceFileAsString(resourceFileName);

    GeneratedMessageV3.Builder responseBuilder = getGatewayServiceResponseBuilder();
    try {
      JsonFormat.parser().merge(requestJsonStr, responseBuilder);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

    return (TGatewayServiceResponseType) responseBuilder.build();
  }

  private String readResourceFileAsString(String fileName) {
    return ((BufferedReader) readResourceFile(fileName)).lines().collect(Collectors.joining());
  }

  private QueryServiceClient createMockQueryServiceClient(String fileName) throws IOException {
    QueryServiceClient queryServiceClient = mock(QueryServiceClient.class);

    Map<QueryRequest, ResultSetChunk> queryRequestResultSetChunkMap =
        readExpectedQueryServiceRequestAndResponse(fileName);

    when(queryServiceClient.executeQuery(
        any(QueryRequest.class), any(HashMap.class), any(Integer.class)))
        .thenAnswer(
            (Answer<Iterator<ResultSetChunk>>)
                invocation -> {
                  QueryRequest queryRequest = (QueryRequest) invocation.getArguments()[0];
                  ResultSetChunk resultSetChunk = queryRequestResultSetChunkMap.get(queryRequest);
                  if (resultSetChunk
                      == null) { // This means this QueryRequest object is unexpected.
                    Assertions.fail(
                        "Unexpected QueryRequest object:\n"
                            + JsonFormat.printer().print(queryRequest));
                  }

                  return List.of(resultSetChunk).iterator();
                });

    return queryServiceClient;
  }

  private String createResourceFileName(String filesBaseDir, String fileName) {
    return String.format("%s/%s/%s.json", filesBaseDir, getTestSuiteName(), fileName);
  }

  protected abstract String getTestSuiteName();

  protected abstract GeneratedMessageV3.Builder getGatewayServiceRequestBuilder();

  protected abstract GeneratedMessageV3.Builder getGatewayServiceResponseBuilder();

  protected abstract TGatewayServiceResponseType executeApi(
      TGatewayServiceRequestType request,
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs);
}
