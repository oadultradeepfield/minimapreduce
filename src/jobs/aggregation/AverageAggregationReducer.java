package jobs.aggregation;

import core.Reducer;
import java.util.List;
import utils.Pair;

/** Class implementing reducer for data aggregation (finding average) job */
public class AverageAggregationReducer implements Reducer<String, Double> {
  /**
   * {@inheritDoc}
   *
   * @param key The key for the values
   * @param value The collection of values associated with the key to reduce
   * @return A pair of the key and the average of data associated with that key
   */
  @Override
  public Pair<String, Double> reduce(String key, List<Double> value) {
    return new Pair<>(key, value.stream().mapToDouble(Double::doubleValue).average().orElse(0));
  }
}
