package core;

import utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

/**
 * A generic ParallelMapReduceEngine that uses ForkJoinPool for better parallelism.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public final class ParallelMapReduceEngine<K, V> extends MapReduceEngine<K, V> {
    private final ForkJoinPool pool;

    /**
     * Create the instance of the class using inputs, mapper, reducer, and number of threads.
     *
     * @param lines   The input data lines to process
     * @param mapper  The mapper function to apply to each input line
     * @param reducer The reducer function to aggregate mapped values by key
     */
    public ParallelMapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer) {
        super(lines, mapper, reducer);
        this.pool = new ForkJoinPool();
    }

    @Override
    protected List<Pair<K, V>> executeMap() {
        return pool.submit(() -> lines.parallelStream().flatMap(line -> mapper.map(line).stream()).toList()).join();
    }

    @Override
    protected Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped) {
        Map<K, List<V>> shuffled = new HashMap<>();

        for (Pair<K, V> pair : mapped) {
            shuffled.computeIfAbsent(pair.first(), _ -> new ArrayList<>()).add(pair.second());
        }

        return shuffled;
    }

    @Override
    protected Map<K, V> executeReduce(Map<K, List<V>> shuffled) {
        ConcurrentHashMap<K, V> reduced = new ConcurrentHashMap<>();

        pool.submit(() -> shuffled.entrySet().parallelStream().forEach(entry -> {
            K key = entry.getKey();
            List<V> values = entry.getValue();
            reduced.put(key, reducer.reduce(key, values).second());
        })).join();

        return reduced;
    }
}
