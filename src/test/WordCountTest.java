package test;

import static org.junit.Assert.*;

import core.MapReduceEngine;
import core.ParallelMapReduceEngine;
import core.SequentialMapReduceEngine;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import jobs.wordcount.WordCountMapper;
import jobs.wordcount.WordCountReducer;
import org.junit.Before;
import org.junit.Test;
import utils.Pair;

/**
 * Test cases for the Word Count functionality. Tests both the individual mapper/reducer components
 * and the end-to-end integration with different engine implementations.
 */
public class WordCountTest {
  private WordCountMapper mapper;
  private WordCountReducer reducer;

  @Before
  public void setUp() {
    mapper = new WordCountMapper();
    reducer = new WordCountReducer();
  }

  @Test
  public void testMapperSimpleLine() {
    String line = "Hello, HELLO world!";
    List<Pair<String, Integer>> result = mapper.map(line);

    assertEquals("Mapper should produce correct number of word tokens", 3, result.size());
    assertTrue(
        "Mapper should correctly lowercase 'hello'", result.contains(new Pair<>("hello", 1)));
    assertEquals(
        "Mapper should count 'hello' twice",
        2,
        result.stream().filter(p -> p.first().equals("hello")).count());
    assertEquals(
        "Mapper should count 'world' once",
        1,
        result.stream().filter(p -> p.first().equals("world")).count());
  }

  @Test
  public void testMapperEmptyOrNonWord() {
    String line = "   ... --- !!!";
    List<Pair<String, Integer>> result = mapper.map(line);
    assertTrue("Mapper should filter out all non-word tokens", result.isEmpty());
  }

  @Test
  public void testReducerCounts() {
    List<Integer> ones = Arrays.asList(1, 1, 1, 1);
    Pair<String, Integer> output = reducer.reduce("test", ones);

    assertEquals("Reducer should preserve the key", "test", output.first());
    assertEquals("Reducer should sum the values", 4, output.second().intValue());
  }

  @Test
  public void testSequentialEngineEndToEnd() {
    List<String> lines = Arrays.asList("foo bar foo", "bar baz");
    MapReduceEngine<String, Integer> engine =
        new SequentialMapReduceEngine<>(lines, mapper, reducer);
    Map<String, Integer> counts = engine.execute();

    assertEquals("Should find correct number of unique words", 3, counts.size());
    assertEquals("Should count 'foo' correctly", Integer.valueOf(2), counts.get("foo"));
    assertEquals("Should count 'bar' correctly", Integer.valueOf(2), counts.get("bar"));
    assertEquals("Should count 'baz' correctly", Integer.valueOf(1), counts.get("baz"));
  }

  @Test
  public void testParallelEngineEndToEnd() {
    List<String> lines = Arrays.asList("a a b c", "b c c");
    MapReduceEngine<String, Integer> engine = new ParallelMapReduceEngine<>(lines, mapper, reducer);
    Map<String, Integer> counts = engine.execute();

    assertEquals("Should find correct number of unique words", 3, counts.size());
    assertEquals("Should count 'a' correctly", Integer.valueOf(2), counts.get("a"));
    assertEquals("Should count 'b' correctly", Integer.valueOf(2), counts.get("b"));
    assertEquals("Should count 'c' correctly", Integer.valueOf(3), counts.get("c"));
  }

  @Test
  public void testMapperWithPunctuation() {
    String line = "Hello, world! This is a test.";
    List<Pair<String, Integer>> result = mapper.map(line);

    assertEquals("Should correctly extract words with punctuation", 6, result.size());
    assertEquals(
        "Should count 'hello' once",
        1,
        result.stream().filter(p -> p.first().equals("hello")).count());
    assertEquals(
        "Should count 'test' once",
        1,
        result.stream().filter(p -> p.first().equals("test")).count());
  }

  @Test
  public void testEmptyInput() {
    List<String> emptyLines = List.of("");
    MapReduceEngine<String, Integer> engine =
        new SequentialMapReduceEngine<>(emptyLines, mapper, reducer);
    Map<String, Integer> counts = engine.execute();

    assertTrue("Empty input should produce empty results", counts.isEmpty());
  }
}
