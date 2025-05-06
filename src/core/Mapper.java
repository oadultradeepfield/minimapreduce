package core;

import utils.Pair;

import java.util.List;

public interface Mapper<K, V> {
    List<Pair<K, V>> map(String line);
}