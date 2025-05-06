package core;

import java.util.List;
import java.util.Map;
import utils.Pair;

/**
 * An abstract MapReduceEngine that processes data using the MapReduce Paradigm.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public abstract class MapReduceEngine<K, V> {
  protected final List<String> lines;
  protected final Mapper<K, V> mapper;
  protected final Reducer<K, V> reducer;

  /**
   * Create the instance of the class using inputs, mapper, reducer, and number of threads.
   *
   * @param lines The input data lines to process
   * @param mapper The mapper function to apply to each input line
   * @param reducer The reducer function to aggregate mapped values by key
   */
  public MapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
    if (lines == null || lines.isEmpty()) {
      throw new IllegalArgumentException("Input data cannot be null or empty");
    }

    if (mapper == null) {
      throw new IllegalArgumentException("Mapper cannot be null");
    }

    if (reducer == null) {
      throw new IllegalArgumentException("Reducer cannot be null");
    }

    this.lines = lines;
    this.mapper = mapper;
    this.reducer = reducer;
  }

  /**
   * Execute the MapReduce operation on the provided data.
   *
   * @return A map containing the final reduced results
   */
  public Map<K, V> execute() {
    List<Pair<K, V>> mapped = executeMap();
    Map<K, List<V>> shuffled = executeShuffle(mapped);
    return executeReduce(shuffled);
  }

  /**
   * Execute the map phase of the MapReduce operation.
   *
   * @return A list of key-value pairs generated from the input
   */
  protected abstract List<Pair<K, V>> executeMap();

  /**
   * Execute the shuffle phase which group values of similar keys.
   *
   * @param mapped A list of key-value pairs generated from mapper
   * @return A map containing the keys and list of their associated values
   */
  protected abstract Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped);

  /**
   * Execute the reduce phase of the MapReduce operation.
   *
   * @param shuffled The map containing the keys and list of their associated values
   * @return A map containing the final reduced results
   */
  protected abstract Map<K, V> executeReduce(Map<K, List<V>> shuffled);
}
