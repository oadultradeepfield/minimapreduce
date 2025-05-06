package jobs.wordcount;

import core.Mapper;
import java.util.Arrays;
import java.util.List;
import utils.Pair;

/** Class implementing mapper for word count job. */
public class WordCountMapper implements Mapper<String, Integer> {
  /**
   * {@inheritDoc}
   *
   * @param line The input line to process
   * @return A list of pairs (word, 1) since a count of one word is always 1
   */
  @Override
  public List<Pair<String, Integer>> map(String line) {
    return Arrays.stream(line.toLowerCase().split("\\W+"))
        .filter(word -> !word.isEmpty())
        .map(word -> new Pair<>(word, 1))
        .toList();
  }
}
