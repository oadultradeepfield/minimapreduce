package core;

import utils.Pair;

import java.util.List;

public interface Reducer<K, V> {
    Pair<K, V> reduce(K key, List<V> values);
}
