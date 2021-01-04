package org.hypertrace.gateway.service.baseline;

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math.stat.descriptive.rank.Median;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class BaselineCalculator {

  public static Baseline getBaseline(double[] metricValueArray) {
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
}
