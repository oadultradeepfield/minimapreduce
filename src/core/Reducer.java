package core;

import utils.Pair;

import java.util.List;

/**
 * Interface for reducing grouped (shuffled) values by key.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface Reducer<K, V> {
    /**
     * Reduce a collection of values associated with a key to a single key-value pair.
     *
     * @param key    The key for the values
     * @param values The collection of values associated with the key to reduce
     * @return A key-value pair containing the reduced result
     */
    Pair<K, V> reduce(K key, List<V> values);
}
