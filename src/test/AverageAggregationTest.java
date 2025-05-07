package test;

import static org.junit.Assert.*;

import core.MapReduceEngine;
import core.ParallelMapReduceEngine;
import core.SequentialMapReduceEngine;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import jobs.aggregation.AggregationMapper;
import jobs.aggregation.AverageAggregationReducer;
import org.junit.Before;
import org.junit.Test;
import utils.Pair;

/**
 * Test cases for the Average Aggregation functionality. Tests both the individual mapper/reducer
 * components and the end-to-end integration with different engine implementations.
 */
public class AverageAggregationTest {
  private AggregationMapper mapper;
  private AverageAggregationReducer reducer;

  @Before
  public void setUp() {
    mapper = new AggregationMapper(0, 1);
    reducer = new AverageAggregationReducer();
  }

  @Test
  public void testMapperSimpleLine() {
    String line = "A,10.5";
    List<Pair<String, Double>> result = mapper.map(line);

    assertEquals("Mapper should produce one pair", 1, result.size());
    assertEquals("Mapper should extract key correctly", "A", result.get(0).first());
    assertEquals("Mapper should extract value correctly", 10.5, result.get(0).second(), 0.001);
  }

  @Test
  public void testMapperCustomSeparator() {
    AggregationMapper tabMapper = new AggregationMapper(1, 2, "\t");
    String line = "data\tB\t20.7\textra";
    List<Pair<String, Double>> result = tabMapper.map(line);

    assertEquals("Mapper should produce one pair", 1, result.size());
    assertEquals("Mapper should extract key correctly", "B", result.get(0).first());
    assertEquals("Mapper should extract value correctly", 20.7, result.get(0).second(), 0.001);
  }

  @Test
  public void testReducerCalculatesAverage() {
    List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0);
    Pair<String, Double> output = reducer.reduce("test", values);

    assertEquals("Reducer should preserve the key", "test", output.first());
    assertEquals("Reducer should calculate average correctly", 25.0, output.second(), 0.001);
  }

  @Test
  public void testReducerWithEmptyValues() {
    List<Double> values = List.of();
    Pair<String, Double> output = reducer.reduce("empty", values);

    assertEquals("Reducer should preserve the key", "empty", output.first());
    assertEquals("Reducer should return 0 for empty list", 0.0, output.second(), 0.001);
  }

  @Test
  public void testSequentialEngineEndToEnd() {
    List<String> lines = Arrays.asList("A,10.0", "B,20.0", "A,30.0", "C,40.0", "B,60.0");
    MapReduceEngine<String, Double> engine =
        new SequentialMapReduceEngine<>(lines, mapper, reducer);
    Map<String, Double> averages = engine.execute();

    assertEquals("Should find correct number of keys", 3, averages.size());
    assertEquals("Should calculate average for 'A' correctly", 20.0, averages.get("A"), 0.001);
    assertEquals("Should calculate average for 'B' correctly", 40.0, averages.get("B"), 0.001);
    assertEquals("Should calculate average for 'C' correctly", 40.0, averages.get("C"), 0.001);
  }

  @Test
  public void testParallelEngineEndToEnd() {
    List<String> lines = Arrays.asList("X,5.0", "Y,15.0", "X,15.0", "Z,25.0", "Y,25.0");
    MapReduceEngine<String, Double> engine = new ParallelMapReduceEngine<>(lines, mapper, reducer);
    Map<String, Double> averages = engine.execute();

    assertEquals("Should find correct number of keys", 3, averages.size());
    assertEquals("Should calculate average for 'X' correctly", 10.0, averages.get("X"), 0.001);
    assertEquals("Should calculate average for 'Y' correctly", 20.0, averages.get("Y"), 0.001);
    assertEquals("Should calculate average for 'Z' correctly", 25.0, averages.get("Z"), 0.001);
  }

  @Test
  public void testMapperWithDifferentColumnIndexes() {
    AggregationMapper customMapper = new AggregationMapper(2, 1, ",");
    String line = "data,42.5,region1,extra";
    List<Pair<String, Double>> result = customMapper.map(line);

    assertEquals("Mapper should produce one pair", 1, result.size());
    assertEquals(
        "Mapper should extract key correctly from index 2", "region1", result.get(0).first());
    assertEquals(
        "Mapper should extract value correctly from index 1", 42.5, result.get(0).second(), 0.001);
  }

  @Test(expected = NumberFormatException.class)
  public void testMapperWithInvalidNumber() {
    String line = "A,invalid";
    mapper.map(line);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testMapperWithInsufficientColumns() {
    String line = "A";
    mapper.map(line);
  }
}
