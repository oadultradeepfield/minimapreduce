package core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import utils.Pair;

/**
 * A generic ParallelMapReduceEngine that uses ForkJoinPool for better parallelism.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public final class ParallelMapReduceEngine<K, V> extends MapReduceEngine<K, V> {
  private final ForkJoinPool pool;

  /**
   * {@inheritDoc}
   *
   * <p>Initialize another instance field for ForkJoinPool.
   *
   * @param lines The input data lines to process
   * @param mapper The mapper function to apply to each input line
   * @param reducer The reducer function to aggregate mapped values by key
   */
  public ParallelMapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
    super(lines, mapper, reducer);
    this.pool = new ForkJoinPool();
  }

  /**
   * {@inheritDoc}
   *
   * @return A list of key-value pairs generated from the input
   */
  @Override
  protected List<Pair<K, V>> executeMap() {
    return pool.submit(
            () -> lines.parallelStream().flatMap(line -> mapper.map(line).stream()).toList())
        .join();
  }

  /**
   * {@inheritDoc}
   *
   * @param mapped A list of key-value pairs generated from mapper
   * @return A map containing the keys and list of their associated values
   */
  @Override
  protected Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped) {
    return pool.submit(
            () ->
                mapped.parallelStream()
                    .collect(
                        Collectors.groupingByConcurrent(
                            Pair::first, Collectors.mapping(Pair::second, Collectors.toList()))))
        .join();
  }

  /**
   * {@inheritDoc}
   *
   * @param shuffled The map containing the keys and list of their associated values
   * @return A map containing the final reduced results
   */
  @Override
  protected Map<K, V> executeReduce(Map<K, List<V>> shuffled) {
    return pool.submit(
            () ->
                shuffled.entrySet().parallelStream()
                    .collect(
                        Collectors.toConcurrentMap(
                            Map.Entry::getKey,
                            e -> reducer.reduce(e.getKey(), e.getValue()).second())))
        .join();
  }
}
