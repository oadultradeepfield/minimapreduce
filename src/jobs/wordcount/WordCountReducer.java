package jobs.wordcount;

import core.Reducer;
import java.util.List;
import utils.Pair;

/** Class implementing reducer for word count job. */
public class WordCountReducer implements Reducer<String, Integer> {
  /**
   * {@inheritDoc}
   *
   * <p>The count for each word is just the number of elements in the list where each element is 1.
   *
   * @param key The key for the values
   * @param value The collection of values associated with the key to reduce
   * @return A pair containing a word and its count
   */
  @Override
  public Pair<String, Integer> reduce(String key, List<Integer> value) {
    return new Pair<>(key, value.size());
  }
}
