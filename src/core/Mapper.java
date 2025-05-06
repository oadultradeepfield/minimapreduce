package core;

import java.util.List;
import utils.Pair;

/**
 * Interface for mapping the input data to key-value pairs.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface Mapper<K, V> {
  /**
   * Map a single input line to a collection of key-value pairs.
   *
   * @param line The input line to process
   * @return A list of key-value pairs generated from the input
   */
  List<Pair<K, V>> map(String line);
}
