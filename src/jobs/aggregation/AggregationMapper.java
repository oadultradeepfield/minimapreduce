package jobs.aggregation;

import core.Mapper;
import java.util.List;
import utils.Pair;

/** Class implementing mapper for general data aggregation job. */
public class AggregationMapper implements Mapper<String, Double> {
  private final int idIdx;
  private final int dataIdx;
  private final String splitter;

  /**
   * Create mapper for aggregation using index for key and value.
   *
   * @param idIdx The column index of the identifier or key used to group data
   * @param dataIdx The column index of the data to analyze
   * @param splitter The string use to split the line of data
   */
  public AggregationMapper(int idIdx, int dataIdx, String splitter) {
    this.idIdx = idIdx;
    this.dataIdx = dataIdx;
    this.splitter = splitter;
  }

  /**
   * Create mapper for aggregation using comma as the splitter.
   *
   * @param idIdx The column index of the identifier or key used to group data
   * @param dataIdx The column index of the data to analyze
   */
  public AggregationMapper(int idIdx, int dataIdx) {
    this.idIdx = idIdx;
    this.dataIdx = dataIdx;
    this.splitter = ",";
  }

  /**
   * {@inheritDoc}
   *
   * @param line The input line to process (split by splitter)
   * @return A list of pairs of (id, data)
   */
  @Override
  public List<Pair<String, Double>> map(String line) {
    String[] partition = line.split(splitter);
    String id = partition[idIdx];
    double data = Double.parseDouble(partition[dataIdx]);
    return List.of(new Pair<>(id, data));
  }
}
