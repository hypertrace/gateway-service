package org.hypertrace.gateway.service.baseline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math.stat.descriptive.rank.Median;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

import java.util.List;

public class BaselineCalculator {

  public static Baseline getBaseline(List<Value> metricValues) {
    if (metricValues.isEmpty()) {
      return Baseline.getDefaultInstance();
    }
    double[] values = getValuesInDouble(metricValues);
    return getBaseline(values);
  }

  private static Baseline getBaseline(double[] metricValueArray) {
    Median median = new Median();
    StandardDeviation standardDeviation = new StandardDeviation();
    double medianValue = median.evaluate(metricValueArray);
    double sd = standardDeviation.evaluate(metricValueArray);
    double lowerBound = medianValue - (2 * sd);
    if (lowerBound < 0) {
      lowerBound = 0;
    }
    double upperBound = medianValue + (2 * sd);
    Baseline baseline =
        Baseline.newBuilder()
            .setLowerBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(lowerBound).build())
            .setUpperBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(upperBound).build())
            .setValue(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(medianValue).build())
            .build();
    return baseline;
  }

  @VisibleForTesting
  private static double[] getValuesInDouble(List<Value> metricValues) {
    ValueType valueType = metricValues.get(0).getValueType();
    switch (valueType) {
      case LONG:
        return metricValues.stream().mapToDouble(value -> (double) value.getLong()).toArray();
      case DOUBLE:
        return metricValues.stream().mapToDouble(value -> value.getDouble()).toArray();
      default:
        throw new IllegalArgumentException("Unsupported valueType " + valueType);
    }
  }
}
