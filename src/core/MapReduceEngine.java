package core;

import utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

public class MapReduceEngine<K, V> {
    private final List<String> lines;
    private final Mapper<K, V> mapper;
    private final Reducer<K, V> reducer;
    private final int numThreads;

    public MapReduceEngine(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer, int numThreads) {
        validateParameters(lines, mapper, reducer, numThreads);
        this.lines = lines;
        this.mapper = mapper;
        this.reducer = reducer;
        this.numThreads = numThreads;
    }

    public Map<K, V> execute() {
        try (ForkJoinPool pool = new ForkJoinPool(numThreads)) {
            List<Pair<K, V>> mapped = executeMap(pool);
            Map<K, List<V>> shuffled = executeShuffle(mapped);
            return executeReduce(pool, shuffled);
        }
    }

    private void validateParameters(List<String> lines, Mapper<K, V> mapper, Reducer<K, V> reducer, int numThreads) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("Input data cannot be null or empty");
        }

        if (mapper == null) {
            throw new IllegalArgumentException("Mapper cannot be null");
        }

        if (reducer == null) {
            throw new IllegalArgumentException("Reducer cannot be null");
        }

        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads must be positive");
        }
    }

    private List<Pair<K, V>> executeMap(ForkJoinPool pool) {
        return pool.submit(() -> lines.parallelStream().flatMap(line -> mapper.map(line).stream()).toList()).join();
    }

    private Map<K, List<V>> executeShuffle(List<Pair<K, V>> mapped) {
        Map<K, List<V>> shuffled = new HashMap<>();

        for (Pair<K, V> pair : mapped) {
            shuffled.computeIfAbsent(pair.first(), _ -> new ArrayList<>()).add(pair.second());
        }

        return shuffled;
    }

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
