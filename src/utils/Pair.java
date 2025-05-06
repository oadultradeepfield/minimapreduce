package utils;

/**
 * Record class for storing a pair of values.
 *
 * @param first The first value in the pair
 * @param second The second value in the pair
 * @param <S> The first type
 * @param <T> The second type
 */
public record Pair<S, T>(S first, T second) {}
