package org.hypertrace.gateway.service.baseline;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class BaselineCalculator {

  public static Baseline getBaseline(double[] metricValueArray) {
    Mean mean = new Mean();
    StandardDeviation standardDeviation = new StandardDeviation();
    double meanValue = mean.evaluate(metricValueArray);
    double sd = standardDeviation.evaluate(metricValueArray);
    double lowerBound = meanValue - (2 * sd);
    if (lowerBound < 0) {
      lowerBound = 0;
    }
    double upperBound = meanValue + (2 * sd);
    Baseline baseline =
        Baseline.newBuilder()
            .setLowerBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(lowerBound).build())
            .setUpperBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(upperBound).build())
            .setValue(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(meanValue).build())
            .build();
    return baseline;
  }
}
