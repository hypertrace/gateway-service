package org.hypertrace.gateway.service.explore;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

/**
 * This class contains logic to fetch the results of other groups that are not in the "top n" groups
 * as one group called "__Other". It creates a request from the original request and adds filters in
 * the request to exclude the groups that were found in the first query, executes the request and
 * then merges the results into the first response and sorts the response.
 *
 * <p>It is called from the implementations of IRequestHandler when ExploreRequest.includeRestGroup
 * of the original request is set to true.
 */
public class TheRestGroupRequestHandler {
  private static final String OTHER_COLUMN_VALUE = "__Other";
  private final RequestHandlerWithSorting requestHandler;

  TheRestGroupRequestHandler(RequestHandlerWithSorting requestHandler) {
    this.requestHandler = requestHandler;
  }

  public void getRowsForTheRestGroup(
      ExploreRequestContext context,
      ExploreRequest originalRequest,
      ExploreResponse.Builder originalResponse) {
    // Return if there was no data in the original request
    if (originalResponse.getRowBuilderList().isEmpty()) {
      return;
    }

    ExploreRequest theRestRequest = createRequest(originalRequest, originalResponse);
    ExploreRequestContext theRestRequestContext =
        new ExploreRequestContext(context.getGrpcContext(), theRestRequest);

    ExploreResponse.Builder theRestGroupResponse =
        requestHandler.handleRequest(theRestRequestContext, theRestRequest);

    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(theRestRequest);
    mergeAndSort(
        originalResponse,
        theRestGroupResponse,
        orderByExpressions,
        context.getRowLimitAfterRest(), // check how many rows expected from original request
        originalRequest.getOffset(),
        requestHandler,
        originalRequest.getGroupByList());
  }

  private void mergeAndSort(
      ExploreResponse.Builder originalResponse,
      ExploreResponse.Builder theRestGroupResponse,
      List<OrderByExpression> orderBys,
      int rowLimit,
      int rowOffset,
      RequestHandlerWithSorting requestHandler,
      List<Expression> groupBys) {
    mergeTheRestResponseIntoOriginalResponse(originalResponse, theRestGroupResponse, groupBys);
    requestHandler.sortAndPaginatePostProcess(originalResponse, orderBys, rowLimit, rowOffset);
  }

  /**
   * Create a new request copied from the originalRequest but without any group by and
   * includeRestGroup set to false. This way we create a query with the same filters and ordering as
   * the original request but no grouping. We will add a filter to remove the groups that were found
   * in the original request.
   */
  private ExploreRequest createRequest(
      ExploreRequest originalRequest, ExploreResponse.Builder originalResponse) {
    // Create a new request copied from the originalRequest but without any group by, order by and
    // set
    // includeRestGroup set to
    // false. This way we create a query with the same conditions as the original request.
    ExploreRequest.Builder requestBuilder =
        ExploreRequest.newBuilder(originalRequest)
            .clearGroupBy() // Remove groupBy
            .clearOrderBy() // Remove orderBy
            .setIncludeRestGroup(false) // Set includeRestGroup to false.
            .setOffset(0); // No offset

    // Create a filter to exclude the values in the the groups found in the original request.
    Filter.Builder excludedGroupsFilter =
        createExcludeFoundGroupsFilter(originalRequest, originalResponse);
    if (requestBuilder.hasFilter()
        && !(requestBuilder.getFilter().equals(Filter.getDefaultInstance()))) {
      requestBuilder.getFilterBuilder().addChildFilter(excludedGroupsFilter);
    } else {
      requestBuilder.setFilter(excludedGroupsFilter);
    }

    return requestBuilder.build();
  }

  /**
   * Returns a filter that will exclude all the found group values in the request for the "The
   * Rest". If the request contains only one group, then we will use a "NOT_IN" group values list
   * filter. If the request contains multiple groups, then we will want to exclude all tuples of the
   * group values.
   *
   * @param originalRequest
   * @param originalResponse
   * @return
   */
  private Filter.Builder createExcludeFoundGroupsFilter(
      ExploreRequest originalRequest, ExploreResponse.Builder originalResponse) {
    if (originalRequest.getGroupByList().size() == 1) {
      return createExcludeFoundGroupsNotInListFilter(originalRequest, originalResponse);
    }
    return createExcludeFoundGroupsAndChainFilter(originalRequest, originalResponse);
  }

  private Filter.Builder createExcludeFoundGroupsNotInListFilter(
      ExploreRequest originalRequest, ExploreResponse.Builder originalResponse) {
    Filter.Builder filterBuilder = Filter.newBuilder();
    filterBuilder.setOperator(Operator.AND);
    originalRequest
        .getGroupByList()
        .forEach(
            groupBy -> {
              String groupByResultName =
                  ExpressionReader.getSelectionResultName(groupBy).orElseThrow();
              Set<String> excludedValues = getExcludedValues(groupByResultName, originalResponse);
              filterBuilder.addChildFilter(createExcludedChildFilter(groupBy, excludedValues));
            });

    return filterBuilder;
  }

  /**
   * We need to create a filter that excludes all the group tuples found in the original response.
   * Suppose we are dealing with a table where we group on the column names "c2" and "c3" and we got
   * back the group tuples below in the original response.
   *
   * <pre>
   * c2  | c3
   * ---------
   * v10 | v11
   * v20 | v21
   *
   * So for "The Rest" request we need to exclude rows with the (c2,c3) tuples (v10, v11) or
   * (v20, v21). So the filter should be ("=" is for equality):
   *     ( NOT ( ( c2 = 'v10' AND c3 = 'v11' ) OR ( c2 = 'v20' AND c3 = 'v21' ) ) )
   * However, since the query-service(Pinot) does not support NOT(unless it's NOT IN), then this does
   * not work. So we can use DeMorgan's law to create a filter that Pinot will support by pushing
   * the NOT into the filter expression:
   *     ( ( ( c2 != 'v10' OR c3 != 'v11' ) AND ( c2 != 'v20' OR c3 != 'v21' ) ) )
   * We will create the filter above.
   * </pre>
   *
   * @param originalRequest
   * @param originalResponse
   * @return
   */
  private Filter.Builder createExcludeFoundGroupsAndChainFilter(
      ExploreRequest originalRequest, ExploreResponse.Builder originalResponse) {
    Filter.Builder filterBuilder = Filter.newBuilder();
    filterBuilder.setOperator(Operator.AND);
    Map<String, Expression> groupBySelectionExpressionsByResultName =
        groupByExpressionByResultName(originalRequest);

    originalResponse
        .getRowBuilderList()
        .forEach(
            rowBuilder ->
                filterBuilder.addChildFilter(
                    createGroupValuesOrFilter(
                        groupBySelectionExpressionsByResultName, rowBuilder)));

    return filterBuilder;
  }

  private Map<String, Expression> groupByExpressionByResultName(ExploreRequest originalRequest) {
    return originalRequest.getGroupByList().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                expression -> ExpressionReader.getSelectionResultName(expression).orElseThrow(),
                Function.identity()));
  }

  private Filter.Builder createGroupValuesOrFilter(
      Map<String, Expression> groupBySelectionExpressionsByResultName, Row.Builder rowBuilder) {
    Filter.Builder filterBuilder = Filter.newBuilder();
    filterBuilder.setOperator(Operator.OR);
    rowBuilder
        .getColumnsMap()
        .forEach(
            (columnName, columnValue) -> {
              if (groupBySelectionExpressionsByResultName.containsKey(columnName)) {
                filterBuilder.addChildFilter(
                    Filter.newBuilder()
                        .setLhs(groupBySelectionExpressionsByResultName.get(columnName))
                        .setOperator(Operator.NEQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setValueType(ValueType.STRING)
                                                .setString(columnValue.getString())))));
              }
            });

    return filterBuilder;
  }

  private Set<String> getExcludedValues(
      String resultName, ExploreResponse.Builder originalResponse) {
    return originalResponse.getRowBuilderList().stream()
        .map(rowBuilder -> rowBuilder.getColumnsMap().get(resultName))
        .map(Value::getString)
        .collect(ImmutableSet.toImmutableSet());
  }

  private Filter.Builder createExcludedChildFilter(
      Expression groupBySelectionExpression, Set<String> excludedValues) {
    return Filter.newBuilder()
        .setLhs(groupBySelectionExpression)
        .setOperator(Operator.NOT_IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING_ARRAY)
                                .addAllStringArray(excludedValues))));
  }

  private void mergeTheRestResponseIntoOriginalResponse(
      ExploreResponse.Builder originalResponse,
      ExploreResponse.Builder theRestGroupResponse,
      List<Expression> groupBys) {
    theRestGroupResponse
        .getRowBuilderList()
        .forEach(
            rowBuilder -> {
              appendTheRestColumnValueToRowBuilder(rowBuilder, groupBys);
              originalResponse.addRow(rowBuilder);
            });
  }

  private void appendTheRestColumnValueToRowBuilder(
      Row.Builder rowBuilder, List<Expression> groupBys) {
    groupBys.forEach(
        groupBy ->
            rowBuilder.putColumns(
                ExpressionReader.getSelectionResultName(groupBy).orElseThrow(),
                Value.newBuilder()
                    .setValueType(ValueType.STRING)
                    .setString(OTHER_COLUMN_VALUE)
                    .build()));
  }
}
