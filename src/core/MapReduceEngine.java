package core;

import utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

/**
 * A generic MapReduceEngine that processes data using the MapReduce Paradigm.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class MapReduceEngine<K, V> {
    private final List<String> lines;
    private final Mapper<K, V> mapper;
    private final Reducer<K, V> reducer;
    private final ForkJoinPool pool;

    /**
     * Create the instance of the class using inputs, mapper, reducer, and number of threads.
     *
     * @param lines   The input data lines to process
     * @param mapper  The mapper function to apply to each input line
     * @param reducer The reducer function to aggregate mapped values by key
     */
    public MapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
        validateParameters(lines, mapper, reducer);
        this.lines = lines;
        this.mapper = mapper;
        this.reducer = reducer;
        this.pool = new ForkJoinPool();
    }

    /**
     * Execute the MapReduce operation on the provided data.
     *
     * @return A map containing the final reduced results
     */
    public Map<K, V> execute() {
        List<Pair<K, V>> mapped = executeMap(pool);
        Map<K, List<V>> shuffled = executeShuffle(mapped);
        return executeReduce(pool, shuffled);
    }

    /**
     * Validate all input parameters for the constructor.
     *
     * @param lines   The input data lines to process
     * @param mapper  The mapper function to apply to each input line
     * @param reducer The reducer function to aggregate mapped values by key
     */
    private void validateParameters(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("Input data cannot be null or empty");
        }

        if (mapper == null) {
            throw new IllegalArgumentException("Mapper cannot be null");
        }

        if (reducer == null) {
            throw new IllegalArgumentException("Reducer cannot be null");
        }
    }

    /**
     * Execute the map phase of the MapReduce operation.
     *
     * @param pool The ForkJoinPool instance used for running the operation
     * @return A list of key-value pairs generated from the input
     */
    private List<Pair<K, V>> executeMap(ForkJoinPool pool) {
        return pool.submit(() -> lines.parallelStream().flatMap(line -> mapper.map(line).stream()).toList()).join();
    }

    /**
     * Execute the shuffle phase which group values of similar keys.
     *
     * @param mapped A list of key-value pairs generated from mapper
     * @return A map containing the keys and list of their associated values
     */
    private Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped) {
        Map<K, List<V>> shuffled = new HashMap<>();

        for (Pair<K, V> pair : mapped) {
            shuffled.computeIfAbsent(pair.first(), _ -> new ArrayList<>()).add(pair.second());
        }

        return shuffled;
    }

    /**
     * Execute the reduce phase of the MapReduce operation.
     *
     * @param pool     The ForkJoinPool instance used for running the operation
     * @param shuffled The map containing the keys and list of their associated values
     * @return A map containing the final reduced results
     */
    private Map<K, V> executeReduce(ForkJoinPool pool, Map<K, List<V>> shuffled) {
        ConcurrentHashMap<K, V> reduced = new ConcurrentHashMap<>();

        pool.submit(() -> shuffled.entrySet().parallelStream().forEach(entry -> {
            K key = entry.getKey();
            List<V> values = entry.getValue();
            reduced.put(key, reducer.reduce(key, values).second());
        })).join();

        return reduced;
    }
}
