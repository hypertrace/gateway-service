package org.hypertrace.gateway.service.entity.query;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.EDS;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;

public class ExecutionTreeUtils {

  public static Optional<String> getValidSingleSource(EntityExecutionContext executionContext) {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();

    Optional<String> singleSourceForAllAttributes =
        ExpressionContext.getSingleSourceForAllAttributes(executionContext.getExpressionContext());

    if (singleSourceForAllAttributes.isEmpty()) {
      return Optional.empty();
    }

    /**
     * Entities queries usually have an inherent time filter, via {@link
     * EntitiesRequest#getStartTimeMillis()} and {@link EntitiesRequest#getEndTimeMillis()}.
     *
     * <p>The time filter is usually applied on QS, apart from few entity types, which have a
     * timestamp column specified through timestamp config {@link TimestampConfigs}
     *
     * <p>Single source for all attributes doesn't take care of the inherent time filter for the
     * single source
     *
     * <p>If the single source is {@link EDS} and the timestamp column doesn't exist for the
     * corresponding entity type, the query will be executed on {@link EDS} source, without the
     * inherent filter, leading to erroneous outputs
     *
     * <p>Hence, default to non single source, if
     *
     * <ul>
     *   <li>single source is {@link EDS}
     *   <li>inherent time filter is valid
     *   <li>only live entities are requested (since non live entities request ignores the inherent
     *       time filter anyways)
     *   <li>timestamp column doesn't exist for the corresponding entity type
     * </ul>
     */
    String singleSource = singleSourceForAllAttributes.get();
    if (EDS.name().equals(singleSource)
        && isValidTimeRange(
            entitiesRequest.getStartTimeMillis(), entitiesRequest.getEndTimeMillis())
        && !entitiesRequest.getIncludeNonLiveEntities()
        && TimestampConfigs.getTimestampColumn(entitiesRequest.getEntityType()) == null) {
      return Optional.empty();
    }

    return singleSourceForAllAttributes;
  }

  private static boolean isValidTimeRange(long startTimeMillis, long endTimeMillis) {
    return startTimeMillis != 0 && endTimeMillis != 0 && startTimeMillis < endTimeMillis;
  }

  /**
   * Removes duplicate selection attributes from other sources using {@param pivotSource} as the
   * pivot source
   */
  public static void removeDuplicateSelectionAttributes(
      EntityExecutionContext executionContext, String pivotSource) {
    ExpressionContext expressionContext = executionContext.getExpressionContext();
    if (!expressionContext.getSourceToSelectionAttributeMap().containsKey(pivotSource)) {
      return;
    }

    Map<String, Set<String>> sourceToSelectionAttributeMap =
        Map.copyOf(expressionContext.getSourceToSelectionAttributeMap());

    Set<String> fetchedAttributes = sourceToSelectionAttributeMap.get(pivotSource);

    for (Map.Entry<String, Set<String>> entry : sourceToSelectionAttributeMap.entrySet()) {
      String sourceKey = entry.getKey();
      if (sourceKey.equals(pivotSource)) {
        continue;
      }

      Set<String> duplicateAttributes = Sets.intersection(fetchedAttributes, entry.getValue());
      executionContext.removeSelectionAttributes(sourceKey, duplicateAttributes);
    }
  }
}
