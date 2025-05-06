package core;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import utils.Pair;

/**
 * A generic SequentialMapReduceEngine that uses sequential stream.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public final class SequentialMapReduceEngine<K, V> extends MapReduceEngine<K, V> {
  /**
   * {@inheritDoc}
   *
   * @param lines The input data lines to process
   * @param mapper The mapper function to apply to each input line
   * @param reducer The reducer function to aggregate mapped values by key
   */
  public SequentialMapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
    super(lines, mapper, reducer);
  }

  /**
   * {@inheritDoc}
   *
   * @return A list of key-value pairs generated from the input
   */
  @Override
  protected List<Pair<K, V>> executeMap() {
    return lines.stream().flatMap(line -> mapper.map(line).stream()).toList();
  }

  /**
   * {@inheritDoc}
   *
   * @param mapped A list of key-value pairs generated from mapper
   * @return A map containing the keys and list of their associated values
   */
  @Override
  protected Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped) {
    return mapped.stream()
        .collect(
            Collectors.groupingBy(
                Pair::first, Collectors.mapping(Pair::second, Collectors.toList())));
  }

  /**
   * {@inheritDoc}
   *
   * @param shuffled The map containing the keys and list of their associated values
   * @return A map containing the final reduced results
   */
  @Override
  protected Map<K, V> executeReduce(Map<K, List<V>> shuffled) {
    return shuffled.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> reducer.reduce(entry.getKey(), entry.getValue()).second()));
  }
}
