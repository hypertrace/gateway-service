package org.hypertrace.gateway.service.common.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;

public class ExpressionReader {
  public static List<Expression> getFunctionExpressions(List<Expression> expressions) {
    return expressions.stream()
        .filter(expression -> expression.getValueCase() == Expression.ValueCase.FUNCTION)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<Expression> getAttributeExpressions(List<Expression> expressions) {
    return expressions.stream()
        .filter(ExpressionReader::isAttributeSelection)
        .collect(Collectors.toUnmodifiableList());
  }

  public static Set<String> extractAttributeIds(Expression expression) {
    Set<String> columns = new HashSet<>();
    extractAttributeIds(columns, expression);
    return Collections.unmodifiableSet(columns);
  }

  private static void extractAttributeIds(Set<String> columns, Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        String columnName = expression.getColumnIdentifier().getColumnName();
        columns.add(columnName);
        break;
      case ATTRIBUTE_EXPRESSION:
        columns.add(expression.getAttributeExpression().getAttributeId());
        break;
      case FUNCTION:
        for (Expression exp : expression.getFunction().getArgumentsList()) {
          extractAttributeIds(columns, exp);
        }
        break;
      case ORDERBY:
        extractAttributeIds(columns, expression.getOrderBy().getExpression());
        break;
      case LITERAL:
      case VALUE_NOT_SET:
        break;
    }
  }

  public static Optional<String> getAttributeIdFromAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(expression.getColumnIdentifier().getColumnName());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(expression.getAttributeExpression().getAttributeId());
      case FUNCTION:
        return getAttributeIdFromAttributeSelection(expression.getFunction());
      default:
        return Optional.empty();
    }
  }

  public static Optional<String> getAttributeIdFromAttributeSelection(
      FunctionExpression functionExpression) {
    return functionExpression.getArgumentsList().stream()
        .map(ExpressionReader::getAttributeIdFromAttributeSelection)
        .flatMap(Optional::stream)
        .findFirst();
  }

  public static boolean isSimpleAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
        return !expression.getAttributeExpression().hasSubpath();
      case COLUMNIDENTIFIER:
        return true;
      default:
        return false;
    }
  }

  public static boolean isAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
      case COLUMNIDENTIFIER:
        return true;
      default:
        return false;
    }
  }

  public static Optional<String> getSelectionResultName(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(
            expression.getColumnIdentifier().getAlias().isEmpty()
                ? expression.getColumnIdentifier().getColumnName()
                : expression.getColumnIdentifier().getAlias());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(
            expression.getAttributeExpression().getAlias().isEmpty()
                ? expression.getAttributeExpression().getAttributeId()
                : expression.getAttributeExpression().getAlias());
      case FUNCTION:
        FunctionExpression functionExpression = expression.getFunction();
        if (!functionExpression.getAlias().isEmpty()) {
          return Optional.of(functionExpression.getAlias());
        }
        String argumentString =
            functionExpression.getArgumentsList().stream()
                .map(ExpressionReader::getSelectionResultName)
                .flatMap(Optional::stream)
                .collect(Collectors.joining(","));

        return Optional.of(
            String.format("%s_%s", functionExpression.getFunction(), argumentString));
      default:
        return Optional.empty();
    }
  }

  /**
   * Given a source to attributes, builds an attribute to sources map. Basically, a reverse map of
   * the map provided as input
   *
   * <p>Example:
   *
   * <p>("QS" -> API.id, "QS" -> API.name, "EDS" -> API.id) =>
   *
   * <p>("API.id" -> ["QS", "EDS"], "API.name" -> "QS")
   */
  public static Map<String, Set<String>> buildAttributeToSourcesMap(
      Map<String, Set<String>> sourcesToAttributeMap) {
    Map<String, Set<String>> attributeToSourcesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : sourcesToAttributeMap.entrySet()) {
      String source = entry.getKey();
      for (String attribute : entry.getValue()) {
        attributeToSourcesMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(source);
      }
    }
    return Collections.unmodifiableMap(attributeToSourcesMap);
  }

  public static Map<String, List<String>> getExpectedResultNamesForEachAttributeId(
      Collection<Expression> expressionList, Collection<String> attributeIds) {
    return Map.copyOf(
        expressionList.stream()
            .filter(ExpressionReader::isSimpleAttributeSelection)
            .filter(
                attributeSelection ->
                    attributeIds.contains(
                        getAttributeIdFromAttributeSelection(attributeSelection).orElseThrow()))
            .collect(
                Collectors.groupingBy(
                    expression -> getAttributeIdFromAttributeSelection(expression).orElseThrow(),
                    Collectors.mapping(
                        expression -> getSelectionResultName(expression).orElseThrow(),
                        Collectors.toUnmodifiableList()))));
  }
}
